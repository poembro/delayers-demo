package delayers

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	//logFile, err := os.OpenFile("./c.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	//if err != nil {
	//    log.Panic("打开日志文件异常")
	//}
	//log.SetOutput(logFile)
}

////////////////配置 start//////////////////////

// 配置数据
type Config struct {
	Delayer *Delayer
	Redis   *Redis
}

// delayer 节点数据
type Delayer struct {
	TimerInterval int64
}

// redis 节点数据
type Redis struct {
	Host            string
	Port            string
	Database        int
	Password        string
	MaxIdle         int
	MaxActive       int
	IdleTimeout     int64
	ConnMaxLifetime int64
}

////////////////配置 end//////////////////////

// 定时器类
type Timer struct {
	Config      *Config
	Ticker      *time.Ticker
	Pool        *redis.Pool
	HandleError func(err error, funcName string, data string)
}

const (
	KEY_JOB_POOL       = "delayer:job_pool"
	PREFIX_JOB_BUCKET  = "delayer:job_bucket:"
	PREFIX_READY_QUEUE = "delayer:ready_queue:"
)

func NewTimer(r *Redis) *Timer {
	c := &Config{
		Delayer: &Delayer{
			TimerInterval: 1000, // 计算间隔时间, 单位毫秒
		},
		Redis: r,
	}

	t := &Timer{
		Config: c,
	}
	t.Init()
	//timer.Start()
	return t
}

// 初始化
func (p *Timer) Init() {
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", p.Config.Redis.Host+":"+p.Config.Redis.Port)
			if err != nil {
				return nil, err
			}
			if p.Config.Redis.Password != "" {
				if _, err := c.Do("AUTH", p.Config.Redis.Password); err != nil {
					c.Close()
					return nil, err
				}
			}
			if _, err := c.Do("SELECT", p.Config.Redis.Database); err != nil {
				c.Close()
				return nil, err
			}
			return c, nil
		},
		MaxIdle:         p.Config.Redis.MaxIdle,
		MaxActive:       p.Config.Redis.MaxActive,
		IdleTimeout:     time.Duration(p.Config.Redis.IdleTimeout) * time.Second,
		MaxConnLifetime: time.Duration(p.Config.Redis.ConnMaxLifetime) * time.Second,
	}
	p.Pool = pool
	handleError := func(err error, funcName string, data string) {
		if err != nil {
			if data != "" {
				data = ", [" + data + "]"
			}
			log.Println(fmt.Sprintf("FAILURE: func %s, %s%s.", funcName, err.Error(), data), false)
		}
	}
	p.HandleError = handleError
}

// 开始
func (p *Timer) Start() {
	ticker := time.NewTicker(time.Duration(p.Config.Delayer.TimerInterval) * time.Millisecond)
	go func() {
		for range ticker.C {
			p.run()
		}
	}()
	p.Ticker = ticker
}

// 执行任务
func (p *Timer) run() {
	// 获取到期的任务
	jobs, err := p.getExpireJobs()
	if err != nil {
		p.HandleError(err, "getExpireJobs", "")
		return
	}
	// 并行获取Topic
	topics := make(map[string][]string)
	ch := make(chan []string)
	for _, jobID := range jobs {
		go p.getJobTopic(jobID, ch)
	}
	// Topic分组
	for i := 0; i < len(jobs); i++ {
		arr := <-ch
		if arr[1] != "" { //比如 order  表示 订单类型
			if _, ok := topics[arr[1]]; !ok {
				jobIDs := []string{arr[0]} // uuid 加入对应map
				topics[arr[1]] = jobIDs
			} else {
				topics[arr[1]] = append(topics[arr[1]], arr[0])
			}
		}
	}
	// 并行移动至Topic对应的ReadyQueue
	for topic, jobIDs := range topics {
		go p.moveJobToReadyQueue(jobIDs, topic)
	}
}

// 获取到期的任务
func (p *Timer) getExpireJobs() ([]string, error) {
	conn := p.Pool.Get()
	defer conn.Close()
	return redis.Strings(conn.Do("ZRANGEBYSCORE", KEY_JOB_POOL, "0", time.Now().Unix()))
}

// 获取任务的Topic
func (p *Timer) getJobTopic(jobID string, ch chan []string) {
	conn := p.Pool.Get()
	defer conn.Close()
	topic, err := redis.Strings(conn.Do("HMGET", PREFIX_JOB_BUCKET+jobID, "topic"))
	if err != nil {
		p.HandleError(err, "getJobTopic", jobID)
		ch <- []string{jobID, ""}
		return
	}
	arr := []string{jobID, topic[0]}
	ch <- arr
}

// 移动任务至ReadyQueue
func (p *Timer) moveJobToReadyQueue(jobIDs []string, topic string) {
	// 获取连接
	conn := p.Pool.Get()
	defer conn.Close()
	jobIDsStr := strings.Join(jobIDs, ",")
	// 开启事物
	if err := p.startTrans(conn); err != nil {
		p.HandleError(err, "startTrans", jobIDsStr)
		return
	}
	// 移除JobPool
	if err := p.delJobPool(conn, jobIDs, topic); err != nil {
		p.HandleError(err, "delJobPool", jobIDsStr)
		return
	}
	// 插入ReadyQueue
	if err := p.addReadyQueue(conn, jobIDs, topic); err != nil {
		p.HandleError(err, "addReadyQueue", jobIDsStr)
		return
	}
	// 提交事物
	values, err := p.commit(conn)
	if err != nil {
		p.HandleError(err, "commit", jobIDsStr)
		return
	}
	// 事务结果处理
	v := values[0].(int64)
	v1 := values[1].(int64)
	if v == 0 || v1 == 0 {
		p.HandleError(err, "commit", jobIDsStr)
		return
	}
	// 打印日志
	log.Println(fmt.Sprintf("Job is ready, Topic: %s, IDs: [%s]", topic, jobIDsStr))
}

// 开启事务
func (p *Timer) startTrans(conn redis.Conn) error {
	return conn.Send("MULTI")
}

// 提交事务
func (p *Timer) commit(conn redis.Conn) ([]interface{}, error) {
	return redis.Values(conn.Do("EXEC"))
}

// 移除JobPool
func (p *Timer) delJobPool(conn redis.Conn, jobIDs []string, topic string) error {
	args := make([]interface{}, len(jobIDs)+1)
	args[0] = KEY_JOB_POOL
	for k, v := range jobIDs {
		args[k+1] = v
	}
	return conn.Send("ZREM", args...)
}

// 插入ReadyQueue
func (p *Timer) addReadyQueue(conn redis.Conn, jobIDs []string, topic string) error {
	args := make([]interface{}, len(jobIDs)+1)
	args[0] = PREFIX_READY_QUEUE + topic
	for k, v := range jobIDs {
		args[k+1] = v
	}
	return conn.Send("LPUSH", args...)
}

// 执行
func (p *Timer) Stop() {
	p.Ticker.Stop()
}
