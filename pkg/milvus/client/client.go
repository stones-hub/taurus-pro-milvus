package client

import (
	"context"
	"sync"

	milvussdk "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Client 定义 Milvus 客户端接口
type Client interface {
	// GetClient 获取 Milvus 客户端
	GetClient() milvussdk.Client

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
	Search(ctx context.Context, collectionName string, partitionNames []string, expr string, outputFields []string, vectors []entity.Vector, vectorField string, metricType entity.MetricType, topK int, params entity.SearchParam) ([]milvussdk.SearchResult, error)
	Query(ctx context.Context, collectionName string, partitionNames []string, expr string, outputFields []string) ([]entity.Column, error)

	// 关闭连接
	Close() error
}

// client 实现 Client 接口
type client struct {
	opts   *Options
	cli    milvussdk.Client
	mu     sync.RWMutex
	closed bool
}

// New 创建新的客户端实例
// 参数:
//   - opts: 可选的配置选项，支持以下选项:
//   - WithAddress: 设置服务器地址
//   - WithAuth: 设置用户名密码认证
//   - WithAPIKey: 设置API Key认证
//   - WithDatabase: 设置数据库名称
//   - WithIdentifier: 设置客户端标识符
//   - WithConnectTimeout: 设置连接超时时间
//   - WithRetry: 设置重试次数和最大重试间隔
//   - WithKeepAlive: 设置保活时间和超时时间
//
// 示例:
//
//	// 基本配置
//	cli, err := New(
//	    WithAddress("localhost:19530"),
//	    WithConnectTimeout(5*time.Second),
//	)
//
//	// 带认证的配置
//	cli, err := New(
//	    WithAddress("localhost:19530"),
//	    WithAuth("user", "pass"),  // 使用用户名密码认证
//	    WithDatabase("test_db"),
//	    WithIdentifier("test_client"),
//	)
//
//	// 完整配置
//	cli, err := New(
//	    WithAddress("localhost:19530"),
//	    WithAPIKey("your-api-key"),  // 使用API Key认证
//	    WithConnectTimeout(5*time.Second),
//	    WithRetry(100, 5*time.Second),
//	    WithKeepAlive(15*time.Second, 30*time.Second),
//	)
func New(opts ...Option) (Client, error) {
	options := DefaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	// 构建GRPC选项
	dialOptions := []grpc.DialOption{
		// 使用阻塞式连接并设置超时
		grpc.WithBlock(),
		grpc.WithTimeout(options.ConnectTimeout),

		// 保活配置
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                options.KeepAliveTime,
			Timeout:             options.KeepAliveTimeout,
			PermitWithoutStream: true,
		}),
	}

	// 重试配置
	retryLimit := &milvussdk.RetryRateLimitOption{
		MaxRetry:   options.MaxRetry,
		MaxBackoff: options.MaxRetryBackoff,
	}

	// 转换为Milvus配置
	config := milvussdk.Config{
		Address:        options.Address,
		Username:       options.Username,
		Password:       options.Password,
		APIKey:         options.APIKey,
		DBName:         options.DBName,
		Identifier:     options.Identifier,
		EnableTLSAuth:  options.EnableTLSAuth,
		DialOptions:    dialOptions,
		RetryRateLimit: retryLimit,
	}

	// 创建Milvus客户端
	cli, err := milvussdk.NewClient(context.Background(), config)
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
//   - shardNum: 分片数量，通常设置为 2 或更多，用于数据分布式存储
//
// 注意:
//   - 主键字段必须是整数类型(Int64)或字符串类型(VarChar)
//   - 向量字段必须指定维度(dim)参数
//   - 字符串字段必须指定最大长度(max_length)参数
//
// 示例:
//
//	// 创建一个包含ID、向量、字符串和标量字段的集合
//	schema := &entity.Schema{
//	    CollectionName: "test_collection",
//	    Description:    "Test collection for storing user profiles",
//	    Fields: []*entity.Field{
//	        {Name: "id", DataType: entity.FieldTypeInt64, PrimaryKey: true, AutoID: true},
//	        {Name: "vector", DataType: entity.FieldTypeFloatVector, TypeParams: map[string]string{"dim": "128"}},
//	        {Name: "name", DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "100"}},
//	        {Name: "age", DataType: entity.FieldTypeInt64},
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
// 注意:
//   - 在加载集合之前必须先创建索引
//   - 加载是查询和搜索的必要步骤
//   - 异步加载时需要通过GetLoadState检查加载状态
//   - 加载会占用内存，请合理使用ReleaseCollection释放
//
// 使用流程:
//  1. 创建集合
//  2. 创建索引 (CreateIndex)
//  3. 加载集合 (LoadCollection)
//  4. 执行查询/搜索
//  5. 使用完后释放 (ReleaseCollection)
//
// 示例:
//
//	// 创建索引
//	indexParams, _ := entity.NewIndexIvfFlat(entity.L2, 1024)
//	err = cli.CreateIndex(ctx, "test_collection", "vector", indexParams, false)
//	if err != nil {
//	    return err
//	}
//
//	// 加载集合
//	err = cli.LoadCollection(ctx, "test_collection", false)
//	if err != nil {
//	    return err
//	}
//	defer cli.ReleaseCollection(ctx, "test_collection")
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
//   - map[string]string: 包含集合统计信息的键值对，包括:
//   - "row_count": 集合中的总行数
//   - "segment_count": 数据段数量
//   - "storage_size": 存储大小(字节)
//
// 注意:
//   - 统计信息可能有延迟，不保证实时准确性
//   - 建议在非高频写入场景下使用此接口
//
// 示例:
//
//	stats, err := cli.GetCollectionStatistics(ctx, "test_collection")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	// 获取总行数
//	rowCount := stats["row_count"]
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
//   - indexParams: 索引参数，支持以下索引类型:
//   - IVF_FLAT: 最基础的索引，无损但空间占用大
//   - IVF_SQ8: 标量量化索引，有损但空间占用小
//   - IVF_PQ: 乘积量化索引，有损但空间占用最小
//   - async: true 表示异步创建，false 表示同步创建
//
// 注意:
//   - 索引创建是计算密集型操作，大数据量时建议使用异步模式
//   - 异步模式需要通过GetIndexState检查创建状态
//   - 不同索引类型在搜索性能和准确性上有权衡
//   - 创建新索引会自动删除已有索引
//
// 示例:
//
//	// 创建IVF_FLAT索引
//	ivfFlatParams, err := entity.NewIndexIvfFlat(
//	    entity.L2,    // 距离度量类型
//	    1024,         // nlist聚类中心数
//	)
//	if err != nil {
//	    return err
//	}
//	err = cli.CreateIndex(ctx, "test_collection", "vector", ivfFlatParams, false)
//
//	// 创建IVF_SQ8索引
//	ivfSQ8Params, err := entity.NewIndexIvfSQ8(
//	    entity.L2,    // 距离度量类型
//	    1024,         // nlist聚类中心数
//	)
//	if err != nil {
//	    return err
//	}
//	err = cli.CreateIndex(ctx, "test_collection", "vector", ivfSQ8Params, false)
//
//	// 创建IVF_PQ索引
//	ivfPQParams, err := entity.NewIndexIvfPQ(
//	    entity.L2,    // 距离度量类型
//	    1024,         // nlist聚类中心数
//	    8,            // m 分段数
//	    8,            // nbits 每段位数
//	)
//	if err != nil {
//	    return err
//	}
//	err = cli.CreateIndex(ctx, "test_collection", "vector", ivfPQParams, true) // 异步创建
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
//   - entity.IndexState: 索引状态，可能的值:
//   - IndexStateNone: 未创建索引
//   - IndexStateCreated: 索引创建完成
//   - IndexStateCreating: 索引正在创建中
//   - IndexStateDropping: 索引正在删除中
//
// 注意:
//   - 主要用于异步创建索引时检查创建进度
//   - 建议配合重试机制使用，直到状态变为Created
//
// 示例:
//
//	// 异步创建索引并等待完成
//	indexParams, _ := entity.NewIndexIvfFlat(entity.L2, 1024)
//	err = cli.CreateIndex(ctx, "test_collection", "vector", indexParams, true)
//	if err != nil {
//	    return err
//	}
//
//	// 等待索引创建完成
//	for {
//	    state, err := cli.GetIndexState(ctx, "test_collection", "vector")
//	    if err != nil {
//	        return err
//	    }
//	    if state == entity.IndexStateCreated {
//	        break
//	    }
//	    if state == entity.IndexStateNone {
//	        return errors.New("index creation failed")
//	    }
//	    time.Sleep(time.Second)
//	}
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
// 返回:
//   - entity.Column: 如果主键字段设置了 AutoID，返回自动生成的ID列
//   - error: 错误信息
//
// 注意:
//   - 所有列的长度必须相同
//   - 向量维度必须与集合定义一致
//   - 字符串长度不能超过定义的最大长度
//   - 主键字段如果不是 AutoID，必须提供值且保证唯一性
//
// 示例:
//
//	// 准备向量数据
//	vectors := make([][]float32, 2)
//	for i := range vectors {
//	    vectors[i] = make([]float32, 128) // 128维向量
//	    for j := range vectors[i] {
//	        vectors[i][j] = float32(i*128 + j)
//	    }
//	}
//
//	// 准备插入的列数据
//	columns := []entity.Column{
//	    // 字符串列
//	    entity.NewColumnVarChar("name", []string{
//	        "张三",
//	        "李四",
//	    }),
//	    // 整数列
//	    entity.NewColumnInt64("age", []int64{
//	        25,
//	        30,
//	    }),
//	    // 向量列
//	    entity.NewColumnFloatVector("vector", 128, vectors),
//	}
//
//	// 执行插入
//	idColumn, err := cli.Insert(ctx, "test_collection", "", columns...)
//	if err != nil {
//	    return err
//	}
//
//	// 如果是自动生成ID，可以获取生成的ID
//	if idColumn != nil {
//	    ids := idColumn.(*entity.ColumnInt64).Data()
//	    fmt.Printf("Inserted IDs: %v\n", ids)
//	}
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
//   - expr: 删除条件表达式，支持以下操作:
//   - 比较: ==, !=, >, >=, <, <=
//   - 逻辑: and, or, not
//   - 示例: "age >= 18 and age < 60"
//
// 注意:
//   - 删除操作是异步的，可能需要等待一段时间才能在查询中反映出来
//   - 表达式中的字段名必须存在于集合中
//   - 不支持对向量字段进行条件删除
//   - 删除的数据无法恢复，请谨慎操作
//
// 示例:
//
//	// 删除单个条件
//	err := cli.Delete(ctx, "test_collection", "", "age == 25")
//
//	// 删除复合条件
//	err = cli.Delete(ctx, "test_collection", "", "age >= 18 and age < 60")
//
//	// 在指定分区中删除
//	err = cli.Delete(ctx, "test_collection", "partition1", "name like '张%'")
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
//   - expr: 过滤条件表达式，如 "age >= 18"，空字符串表示无过滤
//   - outputFields: 要返回的字段列表，如 []string{"name", "age"}
//   - vectors: 要搜索的向量列表，每个向量维度必须与集合中的向量字段维度一致
//   - vectorField: 向量字段名称，如 "vector"
//   - metricType: 距离计算方式:
//   - entity.L2: 欧氏距离
//   - entity.IP: 内积距离
//   - entity.COSINE: 余弦距离
//   - entity.HAMMING: 汉明距离
//   - topK: 返回最相似的前 K 个结果，建议不超过 2048
//   - params: 搜索参数，不同索引类型参数不同:
//   - IVF_FLAT: entity.NewIndexIvfFlatSearchParam(nprobe)
//   - IVF_SQ8: entity.NewIndexIvfSQ8SearchParam(nprobe)
//   - IVF_PQ: entity.NewIndexIvfPQSearchParam(nprobe)
//
// 注意:
//   - 搜索前必须先加载集合或相关分区
//   - 搜索参数会影响查询性能和准确性
//   - nprobe 值越大，结果越准确但性能越低
//
// 示例:
//
//	// 准备搜索向量
//	vectors := make([][]float32, 2)
//	for i := range vectors {
//	    vectors[i] = make([]float32, 128) // 128维向量
//	    for j := range vectors[i] {
//	        vectors[i][j] = float32(i*128 + j)
//	    }
//	}
//	searchVectors := []entity.Vector{
//	    entity.FloatVector(vectors[0]),
//	}
//
//	// 设置搜索参数
//	searchParams, _ := entity.NewIndexIvfFlatSearchParam(10) // nprobe=10
//
//	// 执行向量搜索
//	results, err := cli.Search(
//	    ctx,
//	    "test_collection",
//	    nil,                      // 搜索所有分区
//	    "age >= 18",             // 过滤条件
//	    []string{"name", "age"}, // 返回字段
//	    searchVectors,
//	    "vector",
//	    entity.L2,               // 使用L2距离
//	    3,                       // 返回top 3结果
//	    searchParams,
//	)
//	if err != nil {
//	    return err
//	}
//
//	// 处理搜索结果
//	for _, result := range results {
//	    scores := result.Scores         // 相似度分数
//	    ids := result.IDs              // 匹配的ID
//	    fields := result.Fields        // 返回的字段值
//	}
func (c *client) Search(ctx context.Context, collectionName string, partitionNames []string, expr string, outputFields []string, vectors []entity.Vector, vectorField string, metricType entity.MetricType, topK int, params entity.SearchParam) ([]milvussdk.SearchResult, error) {
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
//   - expr: 查询条件表达式，支持以下操作:
//   - 比较: ==, !=, >, >=, <, <=
//   - 逻辑: and, or, not
//   - 示例: "age >= 18 and age < 60"
//   - outputFields: 要返回的字段列表
//
// 返回:
//   - []entity.Column: 查询结果列数组，每个Column对应一个字段
//   - error: 错误信息
//
// 注意:
//   - 查询前必须先加载集合或相关分区
//   - 表达式中的字段名必须存在于集合中
//   - 不支持对向量字段进行条件查询
//   - 返回的Column类型与字段类型对应:
//   - Int64: *entity.ColumnInt64
//   - VarChar: *entity.ColumnVarChar
//   - FloatVector: *entity.ColumnFloatVector
//
// 示例:
//
//	// 执行查询
//	results, err := cli.Query(
//	    ctx,
//	    "test_collection",
//	    nil,                                  // 查询所有分区
//	    "age >= 25 and name like '张%'",      // 查询条件
//	    []string{"name", "age", "vector"},    // 返回字段
//	)
//	if err != nil {
//	    return err
//	}
//
//	// 处理查询结果
//	for _, col := range results {
//	    switch c := col.(type) {
//	    case *entity.ColumnInt64:
//	        fmt.Printf("Int64 column %s: %v\n", c.Name(), c.Data())
//	    case *entity.ColumnVarChar:
//	        fmt.Printf("VarChar column %s: %v\n", c.Name(), c.Data())
//	    case *entity.ColumnFloatVector:
//	        fmt.Printf("Vector column %s: %v\n", c.Name(), c.Data())
//	    }
//	}
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

func (c *client) GetClient() milvussdk.Client {
	return c.cli
}
