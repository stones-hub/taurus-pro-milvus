package collection

import (
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

// Options 定义集合选项
type Options struct {
	// Description 集合描述
	Description string

	// ShardNum 分片数
	ShardNum int32

	// ConsistencyLevel 一致性级别
	ConsistencyLevel entity.ConsistencyLevel
}

// Option 定义选项设置函数
type Option func(*Options)

// DefaultOptions 返回默认选项
func DefaultOptions() *Options {
	return &Options{
		ShardNum:         2,
		ConsistencyLevel: entity.ClStrong,
	}
}

// WithDescription 设置描述
func WithDescription(desc string) Option {
	return func(o *Options) {
		o.Description = desc
	}
}

// WithShardNum 设置分片数
func WithShardNum(num int32) Option {
	return func(o *Options) {
		o.ShardNum = num
	}
}

// WithConsistencyLevel 设置一致性级别
func WithConsistencyLevel(level entity.ConsistencyLevel) Option {
	return func(o *Options) {
		o.ConsistencyLevel = level
	}
}
