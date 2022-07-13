package main

import (
	"context"
	"delayers-demo/delayers"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	//logFile, err := os.OpenFile("./c.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	//if err != nil {
	//    log.Panic("打开日志文件异常")
	//}
	//log.SetOutput(logFile)
}

type OutData struct {
	Success bool        `json:"success"`
	Code    int         `json:"resultCode"`    //接口响应状态码
	Msg     string      `json:"resultMessage"` //接口响应信息
	Data    interface{} `json:"data"`
}

func OutJson(w http.ResponseWriter, code int, msg string, data interface{}) error {
	w.WriteHeader(200)
	dst := &OutData{
		Success: true,
		Code:    code,
		Msg:     msg,
		Data:    data,
	}
	err := json.NewEncoder(w).Encode(dst)
	if err != nil {
		log.Println(err)
	}

	return err
}

func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println("Middleware", r.Method, r.URL.String(), r.UserAgent())

		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, PATCH, DELETE")
		w.Header().Set("Access-Control-Max-Age", "3600")
		w.Header().Set("Access-Control-Expose-Headers", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")

		w.Header().Set("Content-Type", "application/json; charset=utf-8")

		if r.Method == "OPTIONS" {
			w.WriteHeader(200)
			return
		}

		// url 白名单 (不需要授权token的api)
		urlWhiteList := []string{"/push", "/ping", "/remove"}
		for _, v := range urlWhiteList {
			if r.URL.Path == v {
				next.ServeHTTP(w, r)
				return
			}
		}

		// 解析token值给后续handler访问
		var token string
		token = r.URL.Query().Get("token")
		if token == "" {
			token = r.Header.Get("token")
		}
		if token == "" {
			log.Println("认证失败,没有token参数")
			return
		}
		//tokenInfo, err := util.DecryptToken(token)
		//if err != nil {
		//    OutJson(w, -1, "error", "认证失败")
		//    return
		//}

		ctx := context.WithValue(r.Context(), "active_id", 1)
		ctx2 := context.WithValue(ctx, "user_id", 2)

		next.ServeHTTP(w, r.WithContext(ctx2))
	})
}

type Route struct {
	RdsConf *delayers.Redis
}

func NewRoute() *Route {
	return &Route{
		RdsConf: &delayers.Redis{
			Host:            "10.0.41.145",
			Port:            "6379",
			Database:        12,
			Password:        "",
			MaxIdle:         2,    //最大空闲连接数
			MaxActive:       20,   //最大激活连接数
			IdleTimeout:     3600, //空闲连接超时时间, 单位秒
			ConnMaxLifetime: 3600, //连接最大生存时间, 单位秒
		},
	}
}

func (s *Route) Ping(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(`pong`))
}

func (s *Route) Push(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic            string `json:"topic"`               // topic
		Body             string `json:"body"`                // body
		DelayTime        int    `json:"delay_time"`          // 延迟多久后触发
		ReadyMaxLifetime int    `json:"ready_max_life_time"` // 最大生存时间(多久后失效)
	}
	body, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(body, &req)

	if req.Topic == "" || req.Body == "" || req.DelayTime == 0 || req.ReadyMaxLifetime == 0 {
		OutJson(w, -1, "error topic or body", nil)
		return
	}

	cli := delayers.NewClient(s.RdsConf)
	msg := delayers.Message{
		ID:    uuid.New().String(),
		Topic: req.Topic, //"order",
		Body:  req.Body,  //"12942829372519756200",
	}
	_, err := cli.Push(msg, req.DelayTime, req.ReadyMaxLifetime) // cli.Push(msg,10, 600)
	if err != nil {
		OutJson(w, -1, err.Error(), nil)
		return
	}
	OutJson(w, 200, "success", msg)
	return
}

func (s *Route) Remove(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID string `json:"id"` // body
	}
	body, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(body, &req)

	if req.ID == "" {
		OutJson(w, -1, "error id", nil)
		return
	}

	cli := delayers.NewClient(s.RdsConf)
	_, err := cli.Remove(req.ID) // 28131156741517
	if err != nil {
		OutJson(w, -1, err.Error(), nil)
		return
	}

	OutJson(w, 200, "success", req)
	return
}

func consumer(r *Route) {
	cli := delayers.NewClient(r.RdsConf)
	for {
		msg, err := cli.BPop("order", 10)
		if err != nil {
			log.Println("--error---->", err.Error())
			//break
		}
		if msg != nil {
			// 更多自己的逻辑  TODO
			log.Println("有订单到期了: id:", msg.ID, "  topic:", msg.Topic, "   body:", msg.Body)
		}
	}
}

func main() {
	r := NewRoute()

	t := delayers.NewTimer(r.RdsConf)
	t.Start()

	// 消费 order 队列
	go consumer(r)

	// api路由
	http.Handle("/ping", Middleware(http.HandlerFunc(r.Ping)))
	http.Handle("/push", Middleware(http.HandlerFunc(r.Push)))
	http.Handle("/remove", Middleware(http.HandlerFunc(r.Remove)))

	listen := "0.0.0.0:8080"

	//下方固定写法(不用管) 用来优雅的关闭服务
	srv := http.Server{
		Addr:    listen,
		Handler: http.DefaultServeMux,
	}
	//使用WaitGroup同步Goroutine
	var wg sync.WaitGroup
	exit := make(chan os.Signal)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-exit
		wg.Add(1)
		//使用context控制 srv.Shutdown 的超时时间
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		err := srv.Shutdown(ctx)
		if err != nil {
			log.Println(err)
		}
		//关闭mysql连接 TODO
		t.Stop()

		wg.Done()
	}()

	err := srv.ListenAndServe() //设置监听的端口

	wg.Wait()
	if err != nil && err != http.ErrServerClosed {
		panic(err)
	}
	log.Println("StartHttpServer", "msg", "优雅的关闭服务")
}
