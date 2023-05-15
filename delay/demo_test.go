package delay

import (
	"fmt"
	"testing"
)

func TestNewTimer(t *testing.T) {
	srv := NewTimer(&Redis{
		Addr:            "10.0.41.145:6379",
		Database:        12,
		Password:        "",
		MaxIdle:         2,    //最大空闲连接数
		MaxActive:       20,   //最大激活连接数
		IdleTimeout:     3600, //空闲连接超时时间, 单位秒
		ConnMaxLifetime: 3600, //连接最大生存时间, 单位秒
	})

	srv.Start()

	//////////////////////////

	cli := NewClient(&Redis{
		Addr:            "10.0.41.145:6379",
		Database:        12,
		Password:        "",
		MaxIdle:         2,    //最大空闲连接数
		MaxActive:       20,   //最大激活连接数
		IdleTimeout:     3600, //空闲连接超时时间, 单位秒
		ConnMaxLifetime: 3600, //连接最大生存时间, 单位秒
	})

	reply, err := cli.Push("order", "12942829372519756200", 10, 600)
	fmt.Println(reply)
	fmt.Println(err)
	fmt.Println(1111)
}

// Push 例子
func TestClient_Push(t *testing.T) {
	cli := NewClient(&Redis{
		Addr:            "10.0.41.145:6379",
		Database:        12,
		Password:        "",
		MaxIdle:         2,    //最大空闲连接数
		MaxActive:       20,   //最大激活连接数
		IdleTimeout:     3600, //空闲连接超时时间, 单位秒
		ConnMaxLifetime: 3600, //连接最大生存时间, 单位秒
	})

	reply, err := cli.Push("order", "12942829372519756200", 1, 600)

	fmt.Println(reply)
	fmt.Println(err)
}

// BPop 例子
func TestClient_BPop(t *testing.T) {
	cli := NewClient(&Redis{
		Addr:            "10.0.41.145:6379",
		Database:        12,
		Password:        "",
		MaxIdle:         2,    //最大空闲连接数
		MaxActive:       20,   //最大激活连接数
		IdleTimeout:     3600, //空闲连接超时时间, 单位秒
		ConnMaxLifetime: 3600, //连接最大生存时间, 单位秒
	})
	msg, err := cli.BPop("order", 10)
	fmt.Println(msg)
	fmt.Println(err)
}

// Pop 例子
func TestClient_Pop(t *testing.T) {
	cli := NewClient(&Redis{
		Addr:            "10.0.41.145:6379",
		Database:        12,
		Password:        "",
		MaxIdle:         2,    //最大空闲连接数
		MaxActive:       20,   //最大激活连接数
		IdleTimeout:     3600, //空闲连接超时时间, 单位秒
		ConnMaxLifetime: 3600, //连接最大生存时间, 单位秒
	})

	msg, err := cli.Pop("order")
	fmt.Println(msg)
	fmt.Println(err)
}

// Remove 例子
func TestClient_Remove(t *testing.T) {
	cli := NewClient(&Redis{
		Addr:            "10.0.41.145:6379",
		Database:        12,
		Password:        "",
		MaxIdle:         2,    //最大空闲连接数
		MaxActive:       20,   //最大激活连接数
		IdleTimeout:     3600, //空闲连接超时时间, 单位秒
		ConnMaxLifetime: 3600, //连接最大生存时间, 单位秒
	})
	ok, err := cli.Remove("12942829372519756200")
	fmt.Println(ok)
	fmt.Println(err)
}
