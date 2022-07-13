# 运行在golang上的延迟消息通知程序, 用来处理30分钟后还未支付的订单

## 描述
  针对一段时间后去做某事的场景，典型案例 未支付订单 处理


## 构建运行

- git clone git@github.com:poembro/delayers-demo.git
- cd delayers-demo 
- go run main.go   


## 接口

#### 添加延迟任务 
```
> POST /push HTTP/1.1
> Host: 127.0.0.1:8080 
> Content-Type: application/json
> Accept: */*
> Content-Length: 95

| {
| 	"topic":"order",   // 订单类型 
| 	"body":"555555555555555", // 订单号 
| 	"delay_time":50,  // 50秒后触发
| 	"ready_max_life_time":150 // 150秒后 任务失效
| }

< HTTP/1.1 200 OK
< Content-Type: application/json; charset=utf-8
< Date: Tue, 17 May 2022 06:55:49 GMT
< Content-Length: 41

{
  "success": true,
  "resultCode": 200,
  "resultMessage": "success",
  "data": {
    "ID": "bccb94da-5ab8-4e3b-9699-dd3c8ba800ba",
    "Topic": "order",
    "Body": "555555555555555"
  }
}
```

#### 删除1个未结束的任务
```
> POST /remove HTTP/1.1
> Host: 127.0.0.1:8080
> User-Agent: insomnia/2021.6.0-alpha.7
> Content-Type: application/json
> Accept: */*
> Content-Length: 48

| {
| 	"id":"bccb94da-5ab8-4e3b-9699-dd3c8ba800ba" // 添加延迟任务时返回的 唯一id
| }

* upload completely sent off: 12 out of 12 bytes
* Mark bundle as not supporting multiuse

< HTTP/1.1 200 OK
< Content-Type: application/json; charset=utf-8
< Date: Tue, 17 May 2022 06:55:49 GMT
< Content-Length: 41

{
  "success": true,
  "resultCode": 200,
  "resultMessage": "success",
  "data": {
    "id": "bccb94da-5ab8-4e3b-9699-dd3c8ba800ba"
  }
}
```


## 参考 
  - https://github.com/mix-basic/delayer

