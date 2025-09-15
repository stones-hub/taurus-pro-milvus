package client

import (
	"time"
)

// Options 定义Milvus客户端的配置选项
type Options struct {
	// 基础连接配置
	Address       string // Milvus服务地址，格式：host:port，例如 "localhost:19530"
	Username      string // 用户名，用于身份验证
	Password      string // 密码，用于身份验证
	APIKey        string // API密钥认证，与用户名/密码认证互斥，优先使用APIKey
	DBName        string // 数据库名称，指定要连接的数据库，默认为default
	Identifier    string // 连接标识符，用于区分不同的客户端连接，例如："client-1"或"search-service"
	EnableTLSAuth bool   // 是否启用TLS安全传输，如果地址使用https://则自动启用

	// GRPC连接配置
	ConnectTimeout time.Duration // 仅控制建立连接的超时时间，不影响后续的操作超时。具体操作超时应该通过context.WithTimeout控制

	// 重试配置
	MaxRetry        uint          // 最大重试次数
	MaxRetryBackoff time.Duration // 最大重试退避时间

	// 保活配置
	KeepAliveTime    time.Duration // 保活检测间隔时间
	KeepAliveTimeout time.Duration // 保活检测超时时间
}

// DefaultOptions 返回默认配置
func DefaultOptions() *Options {
	return &Options{
		Address:          "localhost:19530",
		ConnectTimeout:   30 * time.Second, // 默认连接超时30秒
		MaxRetry:         75,               // 默认最大重试75次
		MaxRetryBackoff:  3 * time.Second,  // 默认最大退避3秒
		KeepAliveTime:    5 * time.Second,  // 默认每5秒发送一次ping
		KeepAliveTimeout: 10 * time.Second, // 默认ping超时10秒
	}
}

// Option 定义配置选项函数类型
type Option func(*Options)

// WithAddress 设置服务地址
func WithAddress(address string) Option {
	return func(o *Options) {
		o.Address = address
	}
}

// WithAuth 设置用户名密码认证
func WithAuth(username, password string) Option {
	return func(o *Options) {
		o.Username = username
		o.Password = password
		o.APIKey = "" // 清除APIKey，因为它与用户名/密码认证互斥
	}
}

// WithAPIKey 设置API密钥认证
func WithAPIKey(apiKey string) Option {
	return func(o *Options) {
		o.APIKey = apiKey
		o.Username = "" // 清除用户名/密码认证
		o.Password = ""
	}
}

// WithDatabase 设置数据库
func WithDatabase(dbName string) Option {
	return func(o *Options) {
		o.DBName = dbName
	}
}

// WithTLS 启用TLS安全传输
func WithTLS() Option {
	return func(o *Options) {
		o.EnableTLSAuth = true
	}
}

// WithIdentifier 设置连接标识符
func WithIdentifier(identifier string) Option {
	return func(o *Options) {
		o.Identifier = identifier
	}
}

// WithConnectTimeout 设置连接超时
func WithConnectTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.ConnectTimeout = timeout
	}
}

// WithRetry 设置重试配置
func WithRetry(maxRetry uint, maxBackoff time.Duration) Option {
	return func(o *Options) {
		o.MaxRetry = maxRetry
		o.MaxRetryBackoff = maxBackoff
	}
}

// WithKeepAlive 设置保活配置
func WithKeepAlive(keepAliveTime, keepAliveTimeout time.Duration) Option {
	return func(o *Options) {
		o.KeepAliveTime = keepAliveTime
		o.KeepAliveTimeout = keepAliveTimeout
	}
}
