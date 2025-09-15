package client

import (
	"math"
	"time"
)

// Options 定义Milvus客户端的配置选项
type Options struct {
	// 基础连接配置
	Address  string // Milvus服务地址，格式：host:port，例如 "localhost:19530"
	Username string // 用户名，用于身份验证
	Password string // 密码，用于身份验证
	DBName   string // 数据库名称，指定要连接的数据库，默认为default

	EnableTLSAuth bool   // 是否启用TLS安全传输，如果地址使用https://则自动启用
	APIKey        string // API密钥认证，与用户名/密码认证互斥，优先使用APIKey

	// 重试配置
	MaxRetry        uint          // 最大重试次数
	MaxRetryBackoff time.Duration // 最大重试退避时间

	// GRPC连接配置
	// 注意：这些配置项主要用于高级用户，大多数情况下使用默认值即可
	WithBlock           bool          // 是否阻塞等待连接建立，默认为true（推荐保持默认）, 一旦设置了GRPC配置，则该配置一定是true
	KeepaliveTime       time.Duration // Keepalive 时间间隔，用于保持连接活跃
	KeepaliveTimeout    time.Duration // Keepalive 超时时间，超过此时间未响应则断开连接
	PermitWithoutStream bool          // 是否允许无流连接，用于保持空闲连接
	BaseDelay           time.Duration // 连接退避基础延迟时间，重连时的初始延迟
	Multiplier          float64       // 连接退避倍数，每次重连延迟的倍数
	Jitter              float64       // 连接退避抖动系数，避免同时重连
	MaxDelay            time.Duration // 连接退避最大延迟时间，重连延迟的上限
	MinConnectTimeout   time.Duration // 最小连接超时时间，连接建立的最短超时
	MaxRecvMsgSize      int           // 最大接收消息大小，0表示使用默认值(2GB-1)

	// 其他配置
	DisableConn bool // 是否禁用连接握手，true时跳过向Milvus服务器发送ConnectRequest，通常用于测试或特殊场景
}

// DefaultOptions 返回默认配置
func DefaultOptions() *Options {
	return &Options{
		Address: "localhost:19530",
		DBName:  "default", // 默认数据库名称

		MaxRetry:        75,              // 默认最大重试75次
		MaxRetryBackoff: 3 * time.Second, // 默认最大退避3秒

		// GRPC 默认配置 - 与 milvusclient.DefaultGrpcOpts 保持一致
		WithBlock:           true,
		KeepaliveTime:       5 * time.Second,
		KeepaliveTimeout:    10 * time.Second,
		PermitWithoutStream: true,
		BaseDelay:           100 * time.Millisecond,
		Multiplier:          1.6,
		Jitter:              0.2,
		MaxDelay:            3 * time.Second,
		MinConnectTimeout:   3 * time.Second,
		MaxRecvMsgSize:      math.MaxInt32, // 2GB - 1

		DisableConn: false,
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

// WithRetry 设置重试配置
func WithRetry(maxRetry uint, maxBackoff time.Duration) Option {
	return func(o *Options) {
		o.MaxRetry = maxRetry
		o.MaxRetryBackoff = maxBackoff
	}
}

// WithGrpcOpts 集中设置所有GRPC连接配置
// 注意：这些配置项主要用于高级用户，大多数情况下使用默认值即可
// keepaliveTime: Keepalive时间间隔，用于保持连接活跃
// keepaliveTimeout: Keepalive超时时间，超过此时间未响应则断开连接
// permitWithoutStream: 是否允许无流连接，用于保持空闲连接
// baseDelay: 连接退避基础延迟时间，重连时的初始延迟
// multiplier: 连接退避倍数，每次重连延迟的倍数
// jitter: 连接退避抖动系数，避免同时重连
// maxDelay: 连接退避最大延迟时间，重连延迟的上限
// minConnectTimeout: 最小连接超时时间，连接建立的最短超时
// maxRecvMsgSize: 最大接收消息大小，0表示使用默认值(2GB-1)
func WithGrpcOpts(
	keepaliveTime, keepaliveTimeout time.Duration,
	permitWithoutStream bool,
	baseDelay time.Duration,
	multiplier, jitter float64,
	maxDelay, minConnectTimeout time.Duration,
	maxRecvMsgSize int,
) Option {
	return func(o *Options) {
		o.WithBlock = true
		o.KeepaliveTime = keepaliveTime
		o.KeepaliveTimeout = keepaliveTimeout
		o.PermitWithoutStream = permitWithoutStream
		o.BaseDelay = baseDelay
		o.Multiplier = multiplier
		o.Jitter = jitter
		o.MaxDelay = maxDelay
		o.MinConnectTimeout = minConnectTimeout
		o.MaxRecvMsgSize = maxRecvMsgSize
	}
}

// WithDisableConn 设置是否禁用连接握手
// disable: true时跳过向Milvus服务器发送ConnectRequest，通常用于测试或特殊场景
// 注意：大多数情况下应保持默认值false，确保客户端创建时连接完全建立
func WithDisableConn(disable bool) Option {
	return func(o *Options) {
		o.DisableConn = disable
	}
}
