package client

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

// Client 定义 Milvus 客户端接口
type Client interface {
	// GetClient 获取原始 Milvus 客户端
	GetClient() *milvusclient.Client

	// 数据库管理操作
	CreateDatabase(ctx context.Context, dbName string) error
	DropDatabase(ctx context.Context, dbName string) error
	UseDatabase(ctx context.Context, dbName string) error

	// Collection 相关操作
	CreateCollection(ctx context.Context, schema *entity.Schema, shardNum int32) error
	DropCollection(ctx context.Context, collectionName string) error
	HasCollection(ctx context.Context, collectionName string) (bool, error)
	LoadCollection(ctx context.Context, collectionName string) error
	ReleaseCollection(ctx context.Context, collectionName string) error
	GetCollectionStatistics(ctx context.Context, collectionName string) (map[string]string, error)
	DescribeCollection(ctx context.Context, collectionName string) (*entity.Collection, error)

	// 集合别名操作
	CreateAlias(ctx context.Context, collectionName string, alias string) error
	DropAlias(ctx context.Context, alias string) error
	AlterAlias(ctx context.Context, collectionName string, alias string) error

	// 分区相关操作
	CreatePartition(ctx context.Context, collectionName string, partitionName string) error
	DropPartition(ctx context.Context, collectionName string, partitionName string) error
	HasPartition(ctx context.Context, collectionName string, partitionName string) (bool, error)
	LoadPartitions(ctx context.Context, collectionName string, partitionNames []string) error
	ReleasePartitions(ctx context.Context, collectionName string, partitionNames []string) error

	// 索引相关操作
	CreateIndex(ctx context.Context, collectionName string, fieldName string, idx index.Index) error
	DropIndex(ctx context.Context, collectionName string, fieldName string) error

	// 数据操作
	Insert(ctx context.Context, collectionName string, partitionName string, columns ...column.Column) (column.Column, error)
	Delete(ctx context.Context, collectionName string, partitionName string, expr string) error
	Search(ctx context.Context, collectionName string, partitionNames []string, outputFields []string, vectors []entity.Vector, vectorField string, metricType entity.MetricType, topK int, expr string, params map[string]string) ([]milvusclient.ResultSet, error)
	Query(ctx context.Context, collectionName string, partitionNames []string, expr string, outputFields []string) ([]column.Column, error)

	// 批量操作
	Compact(ctx context.Context, collectionName string) (int64, error)

	// 关闭连接
	Close() error
}

// client 实现 Client 接口
type client struct {
	cli    *milvusclient.Client
	mu     sync.RWMutex
	closed bool
}

// New 创建新的客户端实例
// ctx: 上下文，用于控制请求生命周期
// addr: Milvus服务地址，格式为"host:port"，例如"localhost:19530"或"192.168.1.100:19530"
// username: 用户名，用于身份验证，例如"root"
// password: 密码，用于身份验证，可以为空字符串
func New(ctx context.Context, addr string, username string, password string) (Client, error) {
	return NewWithOptions(ctx, WithAddress(addr), WithAuth(username, password))
}

// NewWithOptions 使用自定义选项创建新的客户端实例
// ctx: 上下文，用于控制请求生命周期
// opts: 配置选项列表，用于自定义客户端行为
func NewWithOptions(ctx context.Context, opts ...Option) (Client, error) {
	// 创建默认配置并应用选项
	options := DefaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	// 构建 milvusclient.ClientConfig
	config := &milvusclient.ClientConfig{
		Address:       options.Address,
		Username:      options.Username,
		Password:      options.Password,
		DBName:        options.DBName,
		EnableTLSAuth: options.EnableTLSAuth,
		APIKey:        options.APIKey,
		DisableConn:   options.DisableConn,
	}

	// 设置重试配置
	if options.MaxRetry > 0 || options.MaxRetryBackoff > 0 {
		config.RetryRateLimit = &milvusclient.RetryRateLimitOption{
			MaxRetry:   options.MaxRetry,
			MaxBackoff: options.MaxRetryBackoff,
		}
	}

	// 构建GRPC DialOptions
	var dialOptions []grpc.DialOption
	dialOptions = append(dialOptions, grpc.WithBlock())

	// 添加Keepalive配置
	keepaliveParams := keepalive.ClientParameters{
		Time:                options.KeepaliveTime,
		Timeout:             options.KeepaliveTimeout,
		PermitWithoutStream: options.PermitWithoutStream,
	}
	dialOptions = append(dialOptions, grpc.WithKeepaliveParams(keepaliveParams))

	// 添加连接退避配置
	backoffConfig := backoff.Config{
		BaseDelay:  options.BaseDelay,
		Multiplier: options.Multiplier,
		Jitter:     options.Jitter,
		MaxDelay:   options.MaxDelay,
	}
	connectParams := grpc.ConnectParams{
		Backoff:           backoffConfig,
		MinConnectTimeout: options.MinConnectTimeout,
	}
	dialOptions = append(dialOptions, grpc.WithConnectParams(connectParams))

	// 添加消息大小配置
	dialOptions = append(dialOptions, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(options.MaxRecvMsgSize),
	))

	config.DialOptions = dialOptions

	cli, err := milvusclient.New(ctx, config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create milvus client")
	}

	return &client{
		cli:    cli,
		closed: false,
	}, nil
}

// GetClient 获取原始 Milvus 客户端
// 返回值: 原始Milvus客户端实例，如果客户端已关闭则返回nil
func (c *client) GetClient() *milvusclient.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil
	}

	return c.cli
}

// CreateCollection 创建集合
// ctx: 上下文，用于控制请求生命周期
// schema: 集合模式定义，包含字段、索引等信息，例如包含id、vector、text字段的Schema
// shardNum: 分片数量，用于数据分片存储，建议值为1-8
func (c *client) CreateCollection(ctx context.Context, schema *entity.Schema, shardNum int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewCreateCollectionOption(schema.CollectionName, schema).
		WithShardNum(shardNum)

	return c.cli.CreateCollection(ctx, option)
}

// DropCollection 删除集合
// ctx: 上下文，用于控制请求生命周期
// collectionName: 要删除的集合名称，例如"my_collection"
func (c *client) DropCollection(ctx context.Context, collectionName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewDropCollectionOption(collectionName)
	return c.cli.DropCollection(ctx, option)
}

// HasCollection 检查集合是否存在
// ctx: 上下文，用于控制请求生命周期
// collectionName: 要检查的集合名称，例如"my_collection"
// 返回值: (是否存在, 错误信息)
func (c *client) HasCollection(ctx context.Context, collectionName string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return false, errors.New("client is closed")
	}

	option := milvusclient.NewHasCollectionOption(collectionName)
	return c.cli.HasCollection(ctx, option)
}

// LoadCollection 加载集合到内存
// ctx: 上下文，用于控制请求生命周期
// collectionName: 要加载的集合名称，例如"my_collection"
func (c *client) LoadCollection(ctx context.Context, collectionName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewLoadCollectionOption(collectionName)
	task, err := c.cli.LoadCollection(ctx, option)
	if err != nil {
		return err
	}
	return task.Await(ctx)
}

// ReleaseCollection 从内存中释放集合
// ctx: 上下文，用于控制请求生命周期
// collectionName: 要释放的集合名称
func (c *client) ReleaseCollection(ctx context.Context, collectionName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewReleaseCollectionOption(collectionName)
	return c.cli.ReleaseCollection(ctx, option)
}

// GetCollectionStatistics 获取集合统计信息
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称
// 返回值: (统计信息映射, 错误信息)
func (c *client) GetCollectionStatistics(ctx context.Context, collectionName string) (map[string]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("client is closed")
	}

	option := milvusclient.NewGetCollectionStatsOption(collectionName)
	return c.cli.GetCollectionStats(ctx, option)
}

// CreatePartition 创建分区
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// partitionName: 分区名称，例如"partition_1"
func (c *client) CreatePartition(ctx context.Context, collectionName string, partitionName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewCreatePartitionOption(collectionName, partitionName)
	return c.cli.CreatePartition(ctx, option)
}

// DropPartition 删除分区
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称
// partitionName: 要删除的分区名称
func (c *client) DropPartition(ctx context.Context, collectionName string, partitionName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewDropPartitionOption(collectionName, partitionName)
	return c.cli.DropPartition(ctx, option)
}

// HasPartition 检查分区是否存在
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称
// partitionName: 要检查的分区名称
// 返回值: (是否存在, 错误信息)
func (c *client) HasPartition(ctx context.Context, collectionName string, partitionName string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return false, errors.New("client is closed")
	}

	option := milvusclient.NewHasPartitionOption(collectionName, partitionName)
	return c.cli.HasPartition(ctx, option)
}

// LoadPartitions 加载分区到内存
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// partitionNames: 要加载的分区名称列表，例如[]string{"partition_1", "partition_2"}
func (c *client) LoadPartitions(ctx context.Context, collectionName string, partitionNames []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewLoadPartitionsOption(collectionName, partitionNames...)
	task, err := c.cli.LoadPartitions(ctx, option)
	if err != nil {
		return err
	}
	return task.Await(ctx)
}

// ReleasePartitions 从内存中释放分区
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称
// partitionNames: 要释放的分区名称列表
func (c *client) ReleasePartitions(ctx context.Context, collectionName string, partitionNames []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewReleasePartitionsOptions(collectionName, partitionNames...)
	return c.cli.ReleasePartitions(ctx, option)
}

// CreateIndex 创建索引
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// fieldName: 字段名称，例如"vector"
// idx: 索引配置对象，例如index.NewIvfFlatIndex(entity.L2, 1024)
func (c *client) CreateIndex(ctx context.Context, collectionName string, fieldName string, idx index.Index) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewCreateIndexOption(collectionName, fieldName, idx)
	task, err := c.cli.CreateIndex(ctx, option)
	if err != nil {
		return err
	}
	return task.Await(ctx)
}

// DropIndex 删除索引
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称
// fieldName: 字段名称
func (c *client) DropIndex(ctx context.Context, collectionName string, fieldName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewDropIndexOption(collectionName, fieldName)
	return c.cli.DropIndex(ctx, option)
}

// Insert 插入数据
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// partitionName: 分区名称，空字符串表示默认分区，例如"partition_1"或""
// columns: 列数据，支持多个列，例如column.NewColumnFloatVector("vector", 128, vectors), column.NewColumnVarChar("text", texts)
// 返回值: (插入数据的ID列, 错误信息)
func (c *client) Insert(ctx context.Context, collectionName string, partitionName string, columns ...column.Column) (column.Column, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.New("client is closed")
	}

	option := milvusclient.NewColumnBasedInsertOption(collectionName, columns...)
	if partitionName != "" {
		option = option.WithPartition(partitionName)
	}
	result, err := c.cli.Insert(ctx, option)
	if err != nil {
		return nil, err
	}
	return result.IDs, nil
}

// Delete 删除数据
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// partitionName: 分区名称，空字符串表示默认分区，例如"partition_1"或""
// expr: 删除条件表达式，例如"id > 0"、"id in [1,2,3]"、"text like 'test%'"
func (c *client) Delete(ctx context.Context, collectionName string, partitionName string, expr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewDeleteOption(collectionName).WithExpr(expr)
	if partitionName != "" {
		option = option.WithPartition(partitionName)
	}
	_, err := c.cli.Delete(ctx, option)
	return err
}

// Search 搜索数据
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// partitionNames: 分区名称列表，nil表示搜索所有分区，例如[]string{"partition_1"}
// outputFields: 输出字段列表，例如[]string{"text", "id"}
// vectors: 搜索向量列表，例如[]entity.Vector{entity.FloatVector([]float32{0.1, 0.2, ...})}
// vectorField: 向量字段名称，例如"vector"
// metricType: 相似度度量类型，例如entity.L2、entity.IP、entity.COSINE
// topK: 返回最相似的前K个结果，例如5
// expr: 过滤条件表达式，空字符串表示无过滤条件，例如"id > 0"
// params: 搜索参数，例如map[string]string{"nprobe": "10"}
// 返回值: (搜索结果列表, 错误信息)
func (c *client) Search(ctx context.Context, collectionName string, partitionNames []string, outputFields []string, vectors []entity.Vector, vectorField string, metricType entity.MetricType, topK int, expr string, params map[string]string) ([]milvusclient.ResultSet, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("client is closed")
	}

	option := milvusclient.NewSearchOption(collectionName, topK, vectors).
		WithPartitions(partitionNames...).
		WithOutputFields(outputFields...).
		WithFilter(expr)

	return c.cli.Search(ctx, option)
}

// Query 查询数据
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// partitionNames: 分区名称列表，nil表示查询所有分区，例如[]string{"partition_1"}
// expr: 查询条件表达式，例如"id > 0"、"text like 'test%'"、"id in [1,2,3]"
// outputFields: 输出字段列表，例如[]string{"text", "id"}
// 返回值: (查询结果列数据, 错误信息)
func (c *client) Query(ctx context.Context, collectionName string, partitionNames []string, expr string, outputFields []string) ([]column.Column, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("client is closed")
	}

	option := milvusclient.NewQueryOption(collectionName).
		WithPartitions(partitionNames...).
		WithFilter(expr).
		WithOutputFields(outputFields...)

	resultSet, err := c.cli.Query(ctx, option)
	if err != nil {
		return nil, err
	}

	// 将ResultSet转换为Column数组
	var columns []column.Column
	for _, field := range resultSet.Fields {
		columns = append(columns, field)
	}
	return columns, nil
}

// CreateDatabase 创建数据库
// ctx: 上下文，用于控制请求生命周期
// dbName: 数据库名称，例如"my_database"
func (c *client) CreateDatabase(ctx context.Context, dbName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewCreateDatabaseOption(dbName)
	return c.cli.CreateDatabase(ctx, option)
}

// DropDatabase 删除数据库
// ctx: 上下文，用于控制请求生命周期
// dbName: 要删除的数据库名称，例如"my_database"
func (c *client) DropDatabase(ctx context.Context, dbName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewDropDatabaseOption(dbName)
	return c.cli.DropDatabase(ctx, option)
}

// UseDatabase 切换当前数据库
// ctx: 上下文，用于控制请求生命周期
// dbName: 要切换到的数据库名称，例如"my_database"
func (c *client) UseDatabase(ctx context.Context, dbName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewUseDatabaseOption(dbName)
	return c.cli.UseDatabase(ctx, option)
}

// DescribeCollection 描述集合
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// 返回值: (集合详细信息, 错误信息)
func (c *client) DescribeCollection(ctx context.Context, collectionName string) (*entity.Collection, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("client is closed")
	}

	option := milvusclient.NewDescribeCollectionOption(collectionName)
	return c.cli.DescribeCollection(ctx, option)
}

// CreateAlias 创建集合别名
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// alias: 别名，例如"my_alias"
func (c *client) CreateAlias(ctx context.Context, collectionName string, alias string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewCreateAliasOption(collectionName, alias)
	return c.cli.CreateAlias(ctx, option)
}

// DropAlias 删除集合别名
// ctx: 上下文，用于控制请求生命周期
// alias: 要删除的别名，例如"my_alias"
func (c *client) DropAlias(ctx context.Context, alias string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewDropAliasOption(alias)
	return c.cli.DropAlias(ctx, option)
}

// AlterAlias 修改集合别名
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// alias: 新的别名，例如"new_alias"
func (c *client) AlterAlias(ctx context.Context, collectionName string, alias string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("client is closed")
	}

	option := milvusclient.NewAlterAliasOption(collectionName, alias)
	return c.cli.AlterAlias(ctx, option)
}

// Compact 压缩集合
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// 返回值: (压缩任务ID, 错误信息)
func (c *client) Compact(ctx context.Context, collectionName string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, errors.New("client is closed")
	}

	option := milvusclient.NewCompactOption(collectionName)
	return c.cli.Compact(ctx, option)
}

// Close 关闭客户端
// 返回值: 错误信息
func (c *client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	return c.cli.Close(context.Background())
}
