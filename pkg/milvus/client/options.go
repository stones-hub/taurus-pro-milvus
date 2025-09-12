package client

import (
	"crypto/tls"
	"time"
)

// Options 定义客户端选项
type Options struct {
	// Address 服务地址，格式：host:port
	Address string

	// Username 用户名（如果启用了身份验证）
	Username string

	// Password 密码（如果启用了身份验证）
	Password string

	// ConnectTimeout 连接超时时间
	ConnectTimeout time.Duration

	// OperationTimeout 操作超时时间
	OperationTimeout time.Duration

	// TLSConfig TLS 配置
	TLSConfig *tls.Config
}

// Option 定义选项设置函数
type Option func(*Options)

// DefaultOptions 返回默认选项
func DefaultOptions() *Options {
	return &Options{
		Address:          "localhost:19530",
		ConnectTimeout:   time.Second * 5,
		OperationTimeout: time.Second * 30,
	}
}

// WithAddress 设置服务地址
func WithAddress(address string) Option {
	return func(o *Options) {
		o.Address = address
	}
}

// WithAuth 设置认证信息
func WithAuth(username, password string) Option {
	return func(o *Options) {
		o.Username = username
		o.Password = password
	}
}

// WithTimeout 设置超时时间
func WithTimeout(connect, operation time.Duration) Option {
	return func(o *Options) {
		o.ConnectTimeout = connect
		o.OperationTimeout = operation
	}
}

// WithTLS 设置 TLS 配置
func WithTLS(config *tls.Config) Option {
	return func(o *Options) {
		o.TLSConfig = config
	}
}
