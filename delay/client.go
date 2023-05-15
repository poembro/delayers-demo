package delay

import (
	"errors"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
)

// 消息结构
type Message struct {
	ID    string
	Topic string
	Body  string
}

// 效验
func (p *Message) Valid() bool {
	if p.ID == "" || p.Topic == "" || p.Body == "" {
		return false
	}
	return true
}

///////////////////////////
// 客户端结构
type Client struct {
	Config      *Config
	Pool        *redis.Pool
	HandleError func(err error, funcName string, data string)
	Timer       *Timer
}

func NewClient(r *Redis) *Client {
	c := &Client{
		Config: &Config{
			Redis: r,
		},
		Timer: NewTimer(r), // 开始定时器 到期的任务转移至队列
	}
	c.Init()
	return c
}

// 初始化
func (p *Client) Init() {
	p.Pool = NewPool(p.Config.Redis)
	handleError := func(err error, funcName string, data string) {
		if err != nil {
			if data != "" {
				data = ", [" + data + "]"
			}
			//log.Println(fmt.Sprintf("FAILURE: func %s, %s%s.", funcName, err.Error(), data), false)
		}
	}
	p.HandleError = handleError
}

// 增加任务
func (p *Client) Push(topic, body string, delayTime int, readyMaxLifetime int) (*Message, error) {
	conn := p.Pool.Get()
	defer conn.Close()
	// 参数验证
	// if !message.Valid() {
	//	return false, errors.New("Invalid message.")
	// }
	uuidStr := uuid.New().String()
	message := &Message{
		ID:    uuidStr,
		Topic: topic,
		Body:  body,
	}
	// 执行事务
	conn.Send("MULTI")
	conn.Send("HMSET", PREFIX_JOB_BUCKET+message.ID, "topic", message.Topic, "body", message.Body)
	conn.Send("EXPIRE", PREFIX_JOB_BUCKET+message.ID, delayTime+readyMaxLifetime)
	conn.Send("ZADD", KEY_JOB_POOL, time.Now().Unix()+int64(delayTime), message.ID)
	values, err := redis.Values(conn.Do("EXEC"))
	if err != nil {
		return nil, err
	}
	// 事务结果处理
	v := values[0].(string)
	v1 := values[1].(int64)
	v2 := values[2].(int64)
	if v != "OK" || v1 == 0 || v2 == 0 {
		return nil, fmt.Errorf("操作失败")
	}
	// 返回
	return message, nil
}

// 取出任务
func (p *Client) Pop(topic string) (*Message, error) {
	conn := p.Pool.Get()
	defer conn.Close()

	id, err := redis.String(conn.Do("RPOP", PREFIX_READY_QUEUE+topic))
	if err != nil {
		return nil, err
	}
	result, err := redis.StringMap(conn.Do("HGETALL", PREFIX_JOB_BUCKET+id))
	if err != nil {
		return nil, err
	}
	if result["topic"] == "" || result["body"] == "" {
		return nil, errors.New("Job bucket has expired or is incomplete")
	}
	conn.Do("DEL", PREFIX_JOB_BUCKET+id)
	msg := &Message{
		ID:    id,
		Topic: result["topic"],
		Body:  result["body"],
	}
	return msg, nil
}

// 阻塞取出任务
func (p *Client) BPop(topic string, timeout int) (*Message, error) {
	conn := p.Pool.Get()
	defer conn.Close()

	values, err := redis.Strings(conn.Do("BRPOP", PREFIX_READY_QUEUE+topic, timeout))
	if err != nil {
		return nil, err
	}
	id := values[1]
	result, err := redis.StringMap(conn.Do("HGETALL", PREFIX_JOB_BUCKET+id))
	if err != nil {
		return nil, err
	}
	if result["topic"] == "" || result["body"] == "" {
		return nil, errors.New("Job bucket has expired or is incomplete")
	}
	conn.Do("DEL", PREFIX_JOB_BUCKET+id)
	msg := &Message{
		ID:    id,
		Topic: result["topic"],
		Body:  result["body"],
	}
	return msg, nil
}

// 移除任务
func (p *Client) Remove(id string) (bool, error) {
	conn := p.Pool.Get()
	defer conn.Close()

	// 执行事务
	conn.Send("MULTI")
	conn.Send("ZREM", KEY_JOB_POOL, id)
	conn.Send("DEL", PREFIX_JOB_BUCKET+id)
	values, err := redis.Values(conn.Do("EXEC"))
	if err != nil {
		return false, err
	}
	// 事务结果处理
	v := values[0].(int64)
	v1 := values[1].(int64)
	if v == 0 || v1 == 0 {
		return false, nil
	}
	// 返回
	return true, nil
}

func (p *Client) Close() error {
	p.Timer.Stop()
	return p.Pool.Close()
}
