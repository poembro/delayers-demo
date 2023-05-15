package delay

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

func NewPool(p *Redis) *redis.Pool {
	cli := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", p.Addr)
			if err != nil {
				return nil, err
			}
			if p.Password != "" {
				if _, err := c.Do("AUTH", p.Password); err != nil {
					c.Close()
					return nil, err
				}
			}
			if _, err := c.Do("SELECT", p.Database); err != nil {
				c.Close()
				return nil, err
			}
			return c, nil
		},
		MaxIdle:         p.MaxIdle,
		MaxActive:       p.MaxActive,
		IdleTimeout:     time.Duration(p.IdleTimeout) * time.Second,
		MaxConnLifetime: time.Duration(p.ConnMaxLifetime) * time.Second,
	}
	// 测试区域 start
	conn := cli.Get() // 获取1个连接
	if err := conn.Send("SET", "test", "test"); err != nil {
	}
	if err := conn.Send("EXPIRE", "test", 5); err != nil {
	}
	if err := conn.Send("DEL", "test"); err != nil {
	}
	// 关闭这个连接
	defer conn.Close()
	// 测试区域 end

	return cli
}
