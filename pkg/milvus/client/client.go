package client

import (
	"context"
	"math"
	"strconv"
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
//
// 同步协议：所有 RPC 方法先通过 gate 获取读（只读操作）或写（变更操作、UseDatabase）门禁，
// 等待阶段受 ctx 控制，取消/超时立即返回 ctx 错误且不发 RPC；RPC（含 Await）期间持有门禁，
// 以保证 UseDatabase 不与其他操作交错，且 Close 不与仍在运行的 SDK 调用竞争。
// gate 内部只有一把仅保护内存状态的互斥锁，不跨 RPC，无锁顺序问题。
//
// 启用只读恢复时 cli 可被内部重建替换：替换只在持有写门禁时进行（此时没有经本库的在途请求），
// 另持 cliMu 以便不经门禁的 GetClient 安全读取。cliMu 仅保护指针读写，不跨 RPC。
type client struct {
	cli   *milvusclient.Client
	cliMu sync.RWMutex
	gate  gate
	db    string    // 当前数据库名，仅在持有写门禁时写入，用于 UseDatabase 失败时回滚 SDK 状态及恢复重建
	rec   *recovery // 只读恢复状态，未启用时为 nil
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

	cli, dbName, err := dialSDK(ctx, options)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create milvus client")
	}

	c := &client{
		cli: cli,
		db:  dbName, // SDK 解析后的库名：显式库优先，否则取自地址 URL 路径
	}
	if options.ReadRecovery {
		c.rec = newRecovery(*options)
	}
	return c, nil
}

// dialSDK 按 options 建立 SDK 客户端并完成握手（受 ctx 限时），返回 SDK 解析后的库名。
// 首次创建与只读恢复重建共用此函数，保证认证、数据库、TLS、拨号等配置完全一致。
func dialSDK(ctx context.Context, options *Options) (*milvusclient.Client, string, error) {
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

	// SDK 在 grpc 拨号成功但握手（Connect）失败时不会关闭连接，且不返回句柄。
	// 这里在握手期间捕获首个 RPC 所用的连接，失败时由本库关闭。
	capture := &connCapture{active: true}
	dialOptions = append(dialOptions, grpc.WithChainUnaryInterceptor(capture.interceptor))
	config.DialOptions = dialOptions

	cli, err := milvusclient.New(ctx, config)
	if err != nil {
		capture.release(true)
		return nil, "", err
	}
	capture.release(false)

	return cli, config.DBName, nil
}

// connCapture 仅在 milvusclient.New 执行期间记录 grpc 连接，结束后不保留连接引用
type connCapture struct {
	mu     sync.Mutex
	active bool
	cc     *grpc.ClientConn
}

func (c *connCapture) interceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	c.mu.Lock()
	if c.active && c.cc == nil {
		c.cc = cc
	}
	c.mu.Unlock()
	return invoker(ctx, method, req, reply, cc, opts...)
}

// release 结束捕获并清除引用；closeConn 为 true 时关闭已捕获的连接
func (c *connCapture) release(closeConn bool) {
	c.mu.Lock()
	cc := c.cc
	c.active = false
	c.cc = nil
	c.mu.Unlock()
	if closeConn && cc != nil {
		_ = cc.Close()
	}
}

// GetClient 获取原始 Milvus 客户端
// 返回值: 原始Milvus客户端实例，如果客户端已关闭（或正在关闭）则返回nil
// 该方法不等待任何在途操作。返回的裸 SDK 句柄不受本库同步保护：
// 调用方不得让它与 Close 或 UseDatabase 并发使用。
// 启用只读恢复时（默认启用），任何并发的 Search/Query 都可能触发内部重建，在裸句柄调用途中将其关闭
// （SDK 的 Close 无锁清空内部连接字段，与并发调用构成数据竞争，不只是返回错误）；
// 重新获取指针只能避开已完成的替换，不能保护随后在途的调用。因此启用恢复时本库不为裸句柄提供并发安全保证，也不承诺裸句柄透明恢复：
// 需要裸 SDK 能力时请使用以 WithReadRecovery(false) 创建的独立客户端，或由调用方确保使用期间没有并发的 Search/Query/Close/UseDatabase。
func (c *client) GetClient() *milvusclient.Client {
	if c.gate.isClosing() {
		return nil
	}

	c.cliMu.RLock()
	defer c.cliMu.RUnlock()
	return c.cli
}

// CreateCollection 创建集合
// ctx: 上下文，用于控制请求生命周期
// schema: 集合模式定义，包含字段、索引等信息，例如包含id、vector、text字段的Schema
// shardNum: 分片数量，用于数据分片存储，建议值为1-8
func (c *client) CreateCollection(ctx context.Context, schema *entity.Schema, shardNum int32) error {
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

	option := milvusclient.NewCreateCollectionOption(schema.CollectionName, schema).
		WithShardNum(shardNum)

	return c.cli.CreateCollection(ctx, option)
}

// DropCollection 删除集合
// ctx: 上下文，用于控制请求生命周期
// collectionName: 要删除的集合名称，例如"my_collection"
func (c *client) DropCollection(ctx context.Context, collectionName string) error {
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

	option := milvusclient.NewDropCollectionOption(collectionName)
	return c.cli.DropCollection(ctx, option)
}

// HasCollection 检查集合是否存在
// ctx: 上下文，用于控制请求生命周期
// collectionName: 要检查的集合名称，例如"my_collection"
// 返回值: (是否存在, 错误信息)
func (c *client) HasCollection(ctx context.Context, collectionName string) (bool, error) {
	if err := c.gate.acquire(ctx, false); err != nil {
		return false, err
	}
	defer c.gate.release(false)

	option := milvusclient.NewHasCollectionOption(collectionName)
	return c.cli.HasCollection(ctx, option)
}

// LoadCollection 加载集合到内存
// ctx: 上下文，用于控制请求生命周期
// collectionName: 要加载的集合名称，例如"my_collection"
func (c *client) LoadCollection(ctx context.Context, collectionName string) error {
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

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
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

	option := milvusclient.NewReleaseCollectionOption(collectionName)
	return c.cli.ReleaseCollection(ctx, option)
}

// GetCollectionStatistics 获取集合统计信息
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称
// 返回值: (统计信息映射, 错误信息)
func (c *client) GetCollectionStatistics(ctx context.Context, collectionName string) (map[string]string, error) {
	if err := c.gate.acquire(ctx, false); err != nil {
		return nil, err
	}
	defer c.gate.release(false)

	option := milvusclient.NewGetCollectionStatsOption(collectionName)
	return c.cli.GetCollectionStats(ctx, option)
}

// CreatePartition 创建分区
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// partitionName: 分区名称，例如"partition_1"
func (c *client) CreatePartition(ctx context.Context, collectionName string, partitionName string) error {
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

	option := milvusclient.NewCreatePartitionOption(collectionName, partitionName)
	return c.cli.CreatePartition(ctx, option)
}

// DropPartition 删除分区
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称
// partitionName: 要删除的分区名称
func (c *client) DropPartition(ctx context.Context, collectionName string, partitionName string) error {
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

	option := milvusclient.NewDropPartitionOption(collectionName, partitionName)
	return c.cli.DropPartition(ctx, option)
}

// HasPartition 检查分区是否存在
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称
// partitionName: 要检查的分区名称
// 返回值: (是否存在, 错误信息)
func (c *client) HasPartition(ctx context.Context, collectionName string, partitionName string) (bool, error) {
	if err := c.gate.acquire(ctx, false); err != nil {
		return false, err
	}
	defer c.gate.release(false)

	option := milvusclient.NewHasPartitionOption(collectionName, partitionName)
	return c.cli.HasPartition(ctx, option)
}

// LoadPartitions 加载分区到内存
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// partitionNames: 要加载的分区名称列表，例如[]string{"partition_1", "partition_2"}
func (c *client) LoadPartitions(ctx context.Context, collectionName string, partitionNames []string) error {
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

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
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

	option := milvusclient.NewReleasePartitionsOptions(collectionName, partitionNames...)
	return c.cli.ReleasePartitions(ctx, option)
}

// CreateIndex 创建索引
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// fieldName: 字段名称，例如"vector"
// idx: 索引配置对象，例如index.NewIvfFlatIndex(entity.L2, 1024)
func (c *client) CreateIndex(ctx context.Context, collectionName string, fieldName string, idx index.Index) error {
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

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
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

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
	if err := c.gate.acquire(ctx, true); err != nil {
		return nil, err
	}
	defer c.gate.release(true)

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
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

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
// 启用只读恢复时，暂时性故障按 README "只读恢复" 处理；恢复后的结果须通过数量与解析校验
func (c *client) Search(ctx context.Context, collectionName string, partitionNames []string, outputFields []string, vectors []entity.Vector, vectorField string, metricType entity.MetricType, topK int, expr string, params map[string]string) ([]milvusclient.ResultSet, error) {
	var results []milvusclient.ResultSet
	err := c.read(ctx, "Search", collectionName, partitionNames, func(ctx context.Context, cli *milvusclient.Client) error {
		option, err := buildSearchOption(collectionName, partitionNames, outputFields, vectors, vectorField, metricType, topK, expr, params)
		if err != nil {
			return err
		}
		results, err = cli.Search(ctx, option)
		return err
	}, func() error {
		return validateSearchResults(results, len(vectors))
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// buildSearchOption 将 Search 的公开参数转换为 SDK 搜索选项
// vectorField 作为 anns_field；metricType 非空时写入 metric_type（与索引不匹配时由服务端报错）；
// params 写入 ANN 搜索参数 params（JSON），数字和布尔字符串按对应类型编码，例如 nprobe="10" 编码为 10
func buildSearchOption(collectionName string, partitionNames []string, outputFields []string, vectors []entity.Vector, vectorField string, metricType entity.MetricType, topK int, expr string, params map[string]string) (milvusclient.SearchOption, error) {
	option := milvusclient.NewSearchOption(collectionName, topK, vectors).
		WithPartitions(partitionNames...).
		WithOutputFields(outputFields...).
		WithFilter(expr)

	if vectorField != "" {
		option = option.WithANNSField(vectorField)
	}
	if metricType != "" {
		option = option.WithSearchParam("metric_type", string(metricType))
	}
	if len(params) > 0 {
		annParam := index.NewCustomAnnParam()
		for k, v := range params {
			val, err := parseParamValue(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid search param %q", k)
			}
			annParam.WithExtraParam(k, val)
		}
		option = option.WithAnnParam(annParam)
	}
	return option, nil
}

// parseParamValue 将字符串参数值转换为 JSON 中更合适的类型
// NaN/Inf 无法编码为 JSON（SDK 会吞掉编码错误并发送空参数），因此本地直接拒绝
func parseParamValue(v string) (any, error) {
	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, errors.Errorf("non-finite value %q is not allowed", v)
		}
		return f, nil
	}
	if v == "true" || v == "false" {
		return v == "true", nil
	}
	return v, nil
}

// Query 查询数据
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// partitionNames: 分区名称列表，nil表示查询所有分区，例如[]string{"partition_1"}
// expr: 查询条件表达式，例如"id > 0"、"text like 'test%'"、"id in [1,2,3]"
// outputFields: 输出字段列表，例如[]string{"text", "id"}
// 返回值: (查询结果列数据, 错误信息)
// 启用只读恢复时，暂时性故障按 README "只读恢复" 处理；恢复后的结果须通过列长度一致性校验
func (c *client) Query(ctx context.Context, collectionName string, partitionNames []string, expr string, outputFields []string) ([]column.Column, error) {
	var columns []column.Column
	err := c.read(ctx, "Query", collectionName, partitionNames, func(ctx context.Context, cli *milvusclient.Client) error {
		option := milvusclient.NewQueryOption(collectionName).
			WithPartitions(partitionNames...).
			WithFilter(expr).
			WithOutputFields(outputFields...)

		resultSet, err := cli.Query(ctx, option)
		if err != nil {
			return err
		}

		// 将ResultSet转换为Column数组
		var cols []column.Column
		for _, field := range resultSet.Fields {
			cols = append(cols, field)
		}
		columns = cols
		return nil
	}, func() error {
		return validateQueryColumns(columns)
	})
	if err != nil {
		return nil, err
	}
	return columns, nil
}

// CreateDatabase 创建数据库
// ctx: 上下文，用于控制请求生命周期
// dbName: 数据库名称，例如"my_database"
func (c *client) CreateDatabase(ctx context.Context, dbName string) error {
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

	option := milvusclient.NewCreateDatabaseOption(dbName)
	return c.cli.CreateDatabase(ctx, option)
}

// DropDatabase 删除数据库
// ctx: 上下文，用于控制请求生命周期
// dbName: 要删除的数据库名称，例如"my_database"
func (c *client) DropDatabase(ctx context.Context, dbName string) error {
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

	option := milvusclient.NewDropDatabaseOption(dbName)
	return c.cli.DropDatabase(ctx, option)
}

// UseDatabase 切换当前数据库
// ctx: 上下文，用于控制请求生命周期
// dbName: 要切换到的数据库名称，例如"my_database"
func (c *client) UseDatabase(ctx context.Context, dbName string) error {
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

	// SDK 的 UseDatabase 会先改写内部 currentDB 再重连，重连失败（含 ctx 超时）不会回滚。
	// 失败时用已取消的 ctx 再切回原库：只恢复 SDK 内部状态，不发 RPC。
	option := milvusclient.NewUseDatabaseOption(dbName)
	if err := c.cli.UseDatabase(ctx, option); err != nil {
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		_ = c.cli.UseDatabase(cancelled, milvusclient.NewUseDatabaseOption(c.db))
		return err
	}
	c.db = dbName
	return nil
}

// DescribeCollection 描述集合
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// 返回值: (集合详细信息, 错误信息)
func (c *client) DescribeCollection(ctx context.Context, collectionName string) (*entity.Collection, error) {
	if err := c.gate.acquire(ctx, false); err != nil {
		return nil, err
	}
	defer c.gate.release(false)

	option := milvusclient.NewDescribeCollectionOption(collectionName)
	return c.cli.DescribeCollection(ctx, option)
}

// CreateAlias 创建集合别名
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// alias: 别名，例如"my_alias"
func (c *client) CreateAlias(ctx context.Context, collectionName string, alias string) error {
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

	option := milvusclient.NewCreateAliasOption(collectionName, alias)
	return c.cli.CreateAlias(ctx, option)
}

// DropAlias 删除集合别名
// ctx: 上下文，用于控制请求生命周期
// alias: 要删除的别名，例如"my_alias"
func (c *client) DropAlias(ctx context.Context, alias string) error {
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

	option := milvusclient.NewDropAliasOption(alias)
	return c.cli.DropAlias(ctx, option)
}

// AlterAlias 修改集合别名
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// alias: 新的别名，例如"new_alias"
func (c *client) AlterAlias(ctx context.Context, collectionName string, alias string) error {
	if err := c.gate.acquire(ctx, true); err != nil {
		return err
	}
	defer c.gate.release(true)

	option := newAlterAliasOption(collectionName, alias)
	return c.cli.AlterAlias(ctx, option)
}

// newAlterAliasOption 构建修改别名选项。SDK 的参数顺序是 (alias, collectionName)，与本库方法相反
func newAlterAliasOption(collectionName, alias string) milvusclient.AlterAliasOption {
	return milvusclient.NewAlterAliasOption(alias, collectionName)
}

// Compact 压缩集合
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// 返回值: (压缩任务ID, 错误信息)
func (c *client) Compact(ctx context.Context, collectionName string) (int64, error) {
	if err := c.gate.acquire(ctx, true); err != nil {
		return 0, err
	}
	defer c.gate.release(true)

	option := milvusclient.NewCompactOption(collectionName)
	return c.cli.Compact(ctx, option)
}

// Close 关闭客户端
// 返回值: 错误信息
func (c *client) Close() error {
	// 先终止进行中的只读恢复（含启用恢复时首次进行中的 Search/Query），使其不再拖住在途计数；
	// 再等待已开始的重建结束（其 ctx 已取消，握手/门禁等待及时返回；它不持有门禁，不会与下面的等待互锁），
	// 使未安装的候选 SDK 客户端在 Close 返回前已关闭、已安装的由下面统一关闭。
	// 先标记 closing：新请求与等待者立即返回 closed；在途操作结束后才关闭 SDK（仅一次）。
	// 并发重复调用等待首个 Close 完成后返回 nil。内部重建从不调用本方法。
	if c.rec != nil {
		c.rec.stop()
		c.rec.waitRebuild()
	}
	return c.gate.close(func() error {
		if c.cli == nil {
			return nil
		}
		return c.cli.Close(context.Background())
	})
}
