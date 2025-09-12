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
// 参数:
//   - opts: 可选的配置选项，如 WithAddress("localhost:19530")
//
// 示例:
//
//	cli, err := New(
//	    WithAddress("localhost:19530"),
//	    WithTimeout(5*time.Second, 30*time.Second),
//	)
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
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - schema: 集合的结构定义，必须包含至少一个主键字段和一个向量字段
//   - shardNum: 分片数量，通常设置为 2 或更多
//
// 示例:
//
//	schema := &entity.Schema{
//	    CollectionName: "test_collection",
//	    Fields: []*entity.Field{
//	        {Name: "id", DataType: entity.FieldTypeInt64, PrimaryKey: true, AutoID: true},
//	        {Name: "vector", DataType: entity.FieldTypeFloatVector, TypeParams: map[string]string{"dim": "128"}},
//	    },
//	}
//	err := cli.CreateCollection(ctx, schema, 2)
func (c *client) CreateCollection(ctx context.Context, schema *entity.Schema, shardNum int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.CreateCollection(ctx, schema, shardNum)
}

// DropCollection 删除集合
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 要删除的集合名称
//
// 示例:
//
//	err := cli.DropCollection(ctx, "test_collection")
func (c *client) DropCollection(ctx context.Context, collectionName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.DropCollection(ctx, collectionName)
}

// HasCollection 检查集合是否存在
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 要检查的集合名称
//
// 返回:
//   - bool: true 表示集合存在，false 表示不存在
//
// 示例:
//
//	exists, err := cli.HasCollection(ctx, "test_collection")
func (c *client) HasCollection(ctx context.Context, collectionName string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return false, errors.New("client is closed")
	}

	return c.cli.HasCollection(ctx, collectionName)
}

// LoadCollection 加载集合到内存
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 要加载的集合名称
//   - async: true 表示异步加载，false 表示同步加载
//
// 注意: 在加载集合之前必须先创建索引
//
// 示例:
//
//	err := cli.LoadCollection(ctx, "test_collection", false)
func (c *client) LoadCollection(ctx context.Context, collectionName string, async bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.LoadCollection(ctx, collectionName, async)
}

// ReleaseCollection 从内存中释放集合
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 要释放的集合名称
//
// 示例:
//
//	err := cli.ReleaseCollection(ctx, "test_collection")
func (c *client) ReleaseCollection(ctx context.Context, collectionName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.ReleaseCollection(ctx, collectionName)
}

// GetCollectionStatistics 获取集合统计信息
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 要获取统计信息的集合名称
//
// 返回:
//   - map[string]string: 包含集合统计信息的键值对
//
// 示例:
//
//	stats, err := cli.GetCollectionStatistics(ctx, "test_collection")
func (c *client) GetCollectionStatistics(ctx context.Context, collectionName string) (map[string]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("client is closed")
	}

	return c.cli.GetCollectionStatistics(ctx, collectionName)
}

// CreatePartition 创建分区
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 集合名称
//   - partitionName: 要创建的分区名称
//
// 示例:
//
//	err := cli.CreatePartition(ctx, "test_collection", "test_partition")
func (c *client) CreatePartition(ctx context.Context, collectionName string, partitionName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.CreatePartition(ctx, collectionName, partitionName)
}

// DropPartition 删除分区
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 集合名称
//   - partitionName: 要删除的分区名称
//
// 示例:
//
//	err := cli.DropPartition(ctx, "test_collection", "test_partition")
func (c *client) DropPartition(ctx context.Context, collectionName string, partitionName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.DropPartition(ctx, collectionName, partitionName)
}

// HasPartition 检查分区是否存在
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 集合名称
//   - partitionName: 要检查的分区名称
//
// 返回:
//   - bool: true 表示分区存在，false 表示不存在
//
// 示例:
//
//	exists, err := cli.HasPartition(ctx, "test_collection", "test_partition")
func (c *client) HasPartition(ctx context.Context, collectionName string, partitionName string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return false, errors.New("client is closed")
	}

	return c.cli.HasPartition(ctx, collectionName, partitionName)
}

// LoadPartitions 加载分区到内存
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 集合名称
//   - partitionNames: 要加载的分区名称列表
//   - async: true 表示异步加载，false 表示同步加载
//
// 示例:
//
//	err := cli.LoadPartitions(ctx, "test_collection", []string{"test_partition"}, false)
func (c *client) LoadPartitions(ctx context.Context, collectionName string, partitionNames []string, async bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.LoadPartitions(ctx, collectionName, partitionNames, async)
}

// ReleasePartitions 从内存中释放分区
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 集合名称
//   - partitionNames: 要释放的分区名称列表
//
// 示例:
//
//	err := cli.ReleasePartitions(ctx, "test_collection", []string{"test_partition"})
func (c *client) ReleasePartitions(ctx context.Context, collectionName string, partitionNames []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.ReleasePartitions(ctx, collectionName, partitionNames)
}

// CreateIndex 创建索引
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 集合名称
//   - fieldName: 要创建索引的字段名称（通常是向量字段）
//   - indexParams: 索引参数，如 IVF_FLAT、IVF_SQ8 等
//   - async: true 表示异步创建，false 表示同步创建
//
// 示例:
//
//	indexParams, _ := entity.NewIndexIvfFlat(entity.L2, 1024)
//	err := cli.CreateIndex(ctx, "test_collection", "vector", indexParams, false)
func (c *client) CreateIndex(ctx context.Context, collectionName string, fieldName string, indexParams entity.Index, async bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.CreateIndex(ctx, collectionName, fieldName, indexParams, async)
}

// DropIndex 删除索引
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 集合名称
//   - fieldName: 要删除索引的字段名称
//
// 示例:
//
//	err := cli.DropIndex(ctx, "test_collection", "vector")
func (c *client) DropIndex(ctx context.Context, collectionName string, fieldName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.DropIndex(ctx, collectionName, fieldName)
}

// GetIndexState 获取索引状态
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 集合名称
//   - fieldName: 要获取索引状态的字段名称
//
// 返回:
//   - entity.IndexState: 索引状态
//
// 示例:
//
//	state, err := cli.GetIndexState(ctx, "test_collection", "vector")
func (c *client) GetIndexState(ctx context.Context, collectionName string, fieldName string) (entity.IndexState, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return 0, errors.New("client is closed")
	}

	return c.cli.GetIndexState(ctx, collectionName, fieldName)
}

// Insert 插入数据
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 集合名称
//   - partitionName: 分区名称，空字符串表示默认分区
//   - columns: 要插入的数据列，必须与集合的 Schema 匹配
//
// 示例:
//
//	columns := []entity.Column{
//	    entity.NewColumnVarChar("name", []string{"张三", "李四"}),
//	    entity.NewColumnInt64("age", []int64{25, 30}),
//	    entity.NewColumnFloatVector("vector", 128, vectors),
//	}
//	_, err := cli.Insert(ctx, "test_collection", "", columns...)
func (c *client) Insert(ctx context.Context, collectionName string, partitionName string, columns ...entity.Column) (entity.Column, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.New("client is closed")
	}

	return c.cli.Insert(ctx, collectionName, partitionName, columns...)
}

// Delete 删除数据
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 集合名称
//   - partitionName: 分区名称，空字符串表示默认分区
//   - expr: 删除条件表达式，如 "age >= 25"
//
// 示例:
//
//	err := cli.Delete(ctx, "test_collection", "", "age == 25")
func (c *client) Delete(ctx context.Context, collectionName string, partitionName string, expr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	return c.cli.Delete(ctx, collectionName, partitionName, expr)
}

// Search 搜索数据
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 集合名称
//   - partitionNames: 要搜索的分区名称列表，nil 表示所有分区
//   - expr: 过滤条件表达式，空字符串表示无过滤
//   - outputFields: 要返回的字段列表
//   - vectors: 要搜索的向量列表
//   - vectorField: 向量字段名称
//   - metricType: 距离计算方式，如 L2、IP 等
//   - topK: 返回最相似的前 K 个结果
//   - params: 搜索参数，如 nprobe 等
//
// 示例:
//
//	searchVectors := []entity.Vector{entity.FloatVector(vectors[0])}
//	searchParams, _ := entity.NewIndexIvfFlatSearchParam(10)
//	results, err := cli.Search(
//	    ctx,
//	    "test_collection",
//	    nil,
//	    "",
//	    []string{"name", "age"},
//	    searchVectors,
//	    "vector",
//	    entity.L2,
//	    3,
//	    searchParams,
//	)
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
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - collectionName: 集合名称
//   - partitionNames: 要查询的分区名称列表，nil 表示所有分区
//   - expr: 查询条件表达式，如 "age >= 25"
//   - outputFields: 要返回的字段列表
//
// 示例:
//
//	results, err := cli.Query(
//	    ctx,
//	    "test_collection",
//	    nil,
//	    "age >= 25",
//	    []string{"name", "age", "vector"},
//	)
func (c *client) Query(ctx context.Context, collectionName string, partitionNames []string, expr string, outputFields []string) ([]entity.Column, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("client is closed")
	}

	return c.cli.Query(ctx, collectionName, partitionNames, expr, outputFields)
}

// Close 关闭客户端
// 示例:
//
//	defer cli.Close()
func (c *client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	return c.cli.Close()
}
