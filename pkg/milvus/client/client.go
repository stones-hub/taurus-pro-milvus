package client

import (
	"context"
	"sync"

	milvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/pkg/errors"
)

// Client 定义 Milvus 客户端接口
type Client interface {
	// Collection 相关操作
	CreateCollection(ctx context.Context, schema *entity.Schema, shardNum int32) error
	DropCollection(ctx context.Context, collectionName string) error
	HasCollection(ctx context.Context, collectionName string) (bool, error)
	LoadCollection(ctx context.Context, collectionName string, async bool) error
	ReleaseCollection(ctx context.Context, collectionName string) error
	GetCollectionStatistics(ctx context.Context, collectionName string) (map[string]string, error)

	// 分区相关操作
	CreatePartition(ctx context.Context, collectionName string, partitionName string) error
	DropPartition(ctx context.Context, collectionName string, partitionName string) error
	HasPartition(ctx context.Context, collectionName string, partitionName string) (bool, error)
	LoadPartitions(ctx context.Context, collectionName string, partitionNames []string, async bool) error
	ReleasePartitions(ctx context.Context, collectionName string, partitionNames []string) error

	// 索引相关操作
	CreateIndex(ctx context.Context, collectionName string, fieldName string, indexParams entity.Index, async bool) error
	DropIndex(ctx context.Context, collectionName string, fieldName string) error
	GetIndexState(ctx context.Context, collectionName string, fieldName string) (entity.IndexState, error)

	// 数据操作
	Insert(ctx context.Context, collectionName string, partitionName string, columns ...entity.Column) (entity.Column, error)
	Delete(ctx context.Context, collectionName string, partitionName string, expr string) error
	Search(ctx context.Context, collectionName string, partitionNames []string, expr string, outputFields []string, vectors []entity.Vector, vectorField string, metricType entity.MetricType, topK int, params entity.SearchParam) ([]milvus.SearchResult, error)
	Query(ctx context.Context, collectionName string, partitionNames []string, expr string, outputFields []string) ([]entity.Column, error)

	// 关闭连接
	Close() error
}

// client 实现 Client 接口
type client struct {
	opts   *Options
	cli    milvus.Client
	mu     sync.RWMutex
	closed bool
}

// New 创建新的客户端实例
func New(opts ...Option) (Client, error) {
	options := DefaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	config := milvus.Config{
		Address: options.Address,
	}

	if options.Username != "" {
		config.Username = options.Username
		config.Password = options.Password
	}

	cli, err := milvus.NewClient(context.Background(), config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create milvus client")
	}

	return &client{
		opts:   options,
		cli:    cli,
		closed: false,
	}, nil
}

// CreateCollection 创建集合
func (c *client) CreateCollection(ctx context.Context, schema *entity.Schema, shardNum int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.CreateCollection(ctx, schema, shardNum)
}

// DropCollection 删除集合
func (c *client) DropCollection(ctx context.Context, collectionName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.DropCollection(ctx, collectionName)
}

// HasCollection 检查集合是否存在
func (c *client) HasCollection(ctx context.Context, collectionName string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return false, errors.New("client is closed")
	}

	return c.cli.HasCollection(ctx, collectionName)
}

// LoadCollection 加载集合到内存
func (c *client) LoadCollection(ctx context.Context, collectionName string, async bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.LoadCollection(ctx, collectionName, async)
}

// ReleaseCollection 从内存中释放集合
func (c *client) ReleaseCollection(ctx context.Context, collectionName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.ReleaseCollection(ctx, collectionName)
}

// GetCollectionStatistics 获取集合统计信息
func (c *client) GetCollectionStatistics(ctx context.Context, collectionName string) (map[string]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("client is closed")
	}

	return c.cli.GetCollectionStatistics(ctx, collectionName)
}

// CreatePartition 创建分区
func (c *client) CreatePartition(ctx context.Context, collectionName string, partitionName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.CreatePartition(ctx, collectionName, partitionName)
}

// DropPartition 删除分区
func (c *client) DropPartition(ctx context.Context, collectionName string, partitionName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.DropPartition(ctx, collectionName, partitionName)
}

// HasPartition 检查分区是否存在
func (c *client) HasPartition(ctx context.Context, collectionName string, partitionName string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return false, errors.New("client is closed")
	}

	return c.cli.HasPartition(ctx, collectionName, partitionName)
}

// LoadPartitions 加载分区到内存
func (c *client) LoadPartitions(ctx context.Context, collectionName string, partitionNames []string, async bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.LoadPartitions(ctx, collectionName, partitionNames, async)
}

// ReleasePartitions 从内存中释放分区
func (c *client) ReleasePartitions(ctx context.Context, collectionName string, partitionNames []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.ReleasePartitions(ctx, collectionName, partitionNames)
}

// CreateIndex 创建索引
func (c *client) CreateIndex(ctx context.Context, collectionName string, fieldName string, indexParams entity.Index, async bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.CreateIndex(ctx, collectionName, fieldName, indexParams, async)
}

// DropIndex 删除索引
func (c *client) DropIndex(ctx context.Context, collectionName string, fieldName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.DropIndex(ctx, collectionName, fieldName)
}

// GetIndexState 获取索引状态
func (c *client) GetIndexState(ctx context.Context, collectionName string, fieldName string) (entity.IndexState, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return 0, errors.New("client is closed")
	}

	return c.cli.GetIndexState(ctx, collectionName, fieldName)
}

// Insert 插入数据
func (c *client) Insert(ctx context.Context, collectionName string, partitionName string, columns ...entity.Column) (entity.Column, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.New("client is closed")
	}

	return c.cli.Insert(ctx, collectionName, partitionName, columns...)
}

// Delete 删除数据
func (c *client) Delete(ctx context.Context, collectionName string, partitionName string, expr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.Delete(ctx, collectionName, partitionName, expr)
}

// Search 搜索数据
func (c *client) Search(ctx context.Context, collectionName string, partitionNames []string, expr string, outputFields []string, vectors []entity.Vector, vectorField string, metricType entity.MetricType, topK int, params entity.SearchParam) ([]milvus.SearchResult, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("client is closed")
	}

	return c.cli.Search(
		ctx,
		collectionName,
		partitionNames,
		expr,
		outputFields,
		vectors,
		vectorField,
		metricType,
		topK,
		params,
	)
}

// Query 查询数据
func (c *client) Query(ctx context.Context, collectionName string, partitionNames []string, expr string, outputFields []string) ([]entity.Column, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("client is closed")
	}

	return c.cli.Query(ctx, collectionName, partitionNames, expr, outputFields)
}

// Close 关闭客户端
func (c *client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	return c.cli.Close()
}
