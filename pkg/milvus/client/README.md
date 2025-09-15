# Milvus Client API 文档

这是 `taurus-pro-milvus` 项目的客户端包，提供了完整的 Milvus 向量数据库操作接口。

## 包结构

```
pkg/milvus/client/
├── client.go      # 客户端实现和接口定义
├── options.go     # 配置选项和选项函数
└── client_test.go # 单元测试
```

## 核心接口

### Client 接口

```go
type Client interface {
    // 获取原始 Milvus 客户端
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
```

## 创建客户端

### 基本创建方式

```go
import (
    "context"
    "github.com/stones-hub/taurus-pro-milvus/pkg/milvus/client"
)

// 使用基本参数创建
cli, err := client.New(ctx, "localhost:19530", "root", "")
```

### 使用选项创建

```go
// 使用选项创建客户端
cli, err := client.NewWithOptions(ctx,
    client.WithAddress("localhost:19530"),
    client.WithAuth("root", ""),
    client.WithDatabase("my_db"),
    client.WithRetry(5, 3*time.Second),
)
```

## 配置选项

### 基本配置选项

| 选项函数 | 参数 | 说明 | 示例 |
|---------|------|------|------|
| `WithAddress` | `address string` | 服务器地址 | `"localhost:19530"` |
| `WithAuth` | `username, password string` | 用户名密码认证 | `"root", ""` |
| `WithAPIKey` | `apiKey string` | API密钥认证 | `"your_api_key"` |
| `WithDatabase` | `dbName string` | 数据库名称 | `"my_database"` |
| `WithTLS` | 无 | 启用TLS | `client.WithTLS()` |
| `WithRetry` | `maxRetry uint, maxBackoff time.Duration` | 重试配置 | `5, 3*time.Second` |

### 高级GRPC配置

```go
client.WithGrpcOpts(
    5*time.Second,    // keepaliveTime: Keepalive时间间隔
    10*time.Second,   // keepaliveTimeout: Keepalive超时时间
    true,             // permitWithoutStream: 是否允许无流连接
    100*time.Millisecond, // baseDelay: 连接退避基础延迟
    1.6,              // multiplier: 连接退避倍数
    0.2,              // jitter: 连接退避抖动系数
    3*time.Second,    // maxDelay: 连接退避最大延迟
    3*time.Second,    // minConnectTimeout: 最小连接超时
    4*1024*1024,      // maxRecvMsgSize: 最大接收消息大小
)
```

## API 方法详解

### 数据库操作

#### CreateDatabase
```go
// ctx: 上下文，用于控制请求生命周期
// dbName: 数据库名称，例如"my_database"
func (c *client) CreateDatabase(ctx context.Context, dbName string) error
```

#### DropDatabase
```go
// ctx: 上下文，用于控制请求生命周期
// dbName: 要删除的数据库名称，例如"my_database"
func (c *client) DropDatabase(ctx context.Context, dbName string) error
```

#### UseDatabase
```go
// ctx: 上下文，用于控制请求生命周期
// dbName: 要切换到的数据库名称，例如"my_database"
func (c *client) UseDatabase(ctx context.Context, dbName string) error
```

### 集合操作

#### CreateCollection
```go
// ctx: 上下文，用于控制请求生命周期
// schema: 集合模式定义，包含字段、索引等信息，例如包含id、vector、text字段的Schema
// shardNum: 分片数量，用于数据分片存储，建议值为1-8
func (c *client) CreateCollection(ctx context.Context, schema *entity.Schema, shardNum int32) error
```

#### HasCollection
```go
// ctx: 上下文，用于控制请求生命周期
// collectionName: 要检查的集合名称，例如"my_collection"
// 返回值: (是否存在, 错误信息)
func (c *client) HasCollection(ctx context.Context, collectionName string) (bool, error)
```

#### LoadCollection
```go
// ctx: 上下文，用于控制请求生命周期
// collectionName: 要加载的集合名称，例如"my_collection"
func (c *client) LoadCollection(ctx context.Context, collectionName string) error
```

#### ReleaseCollection
```go
// ctx: 上下文，用于控制请求生命周期
// collectionName: 要释放的集合名称，例如"my_collection"
func (c *client) ReleaseCollection(ctx context.Context, collectionName string) error
```

### 分区操作

#### CreatePartition
```go
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// partitionName: 分区名称，例如"partition_1"
func (c *client) CreatePartition(ctx context.Context, collectionName string, partitionName string) error
```

#### HasPartition
```go
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// partitionName: 要检查的分区名称，例如"partition_1"
// 返回值: (是否存在, 错误信息)
func (c *client) HasPartition(ctx context.Context, collectionName string, partitionName string) (bool, error)
```

### 索引操作

#### CreateIndex
```go
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// fieldName: 字段名称，例如"vector"
// idx: 索引配置对象，例如index.NewIvfFlatIndex(entity.L2, 1024)
func (c *client) CreateIndex(ctx context.Context, collectionName string, fieldName string, idx index.Index) error
```

### 数据操作

#### Insert
```go
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// partitionName: 分区名称，空字符串表示默认分区，例如"partition_1"或""
// columns: 列数据，支持多个列，例如column.NewColumnFloatVector("vector", 128, vectors), column.NewColumnVarChar("text", texts)
// 返回值: (插入数据的ID列, 错误信息)
func (c *client) Insert(ctx context.Context, collectionName string, partitionName string, columns ...column.Column) (column.Column, error)
```

#### Query
```go
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// partitionNames: 分区名称列表，nil表示查询所有分区，例如[]string{"partition_1"}
// expr: 查询条件表达式，例如"id > 0"、"text like 'test%'"、"id in [1,2,3]"
// outputFields: 输出字段列表，例如[]string{"text", "id"}
// 返回值: (查询结果列数据, 错误信息)
func (c *client) Query(ctx context.Context, collectionName string, partitionNames []string, expr string, outputFields []string) ([]column.Column, error)
```

#### Search
```go
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
func (c *client) Search(ctx context.Context, collectionName string, partitionNames []string, outputFields []string, vectors []entity.Vector, vectorField string, metricType entity.MetricType, topK int, expr string, params map[string]string) ([]milvusclient.ResultSet, error)
```

#### Delete
```go
// ctx: 上下文，用于控制请求生命周期
// collectionName: 集合名称，例如"my_collection"
// partitionName: 分区名称，空字符串表示默认分区，例如"partition_1"或""
// expr: 删除条件表达式，例如"id > 0"、"id in [1,2,3]"、"text like 'test%'"
func (c *client) Delete(ctx context.Context, collectionName string, partitionName string, expr string) error
```

## 表达式语法

Milvus 使用特定的表达式语法进行查询和过滤：

### 比较操作符

```go
"id > 0"           // 大于
"id >= 100"        // 大于等于
"id < 1000"        // 小于
"id <= 500"        // 小于等于
"id == 123"        // 等于（注意使用 == 而不是 =）
"id != 456"        // 不等于
```

### 逻辑操作符

```go
"id > 0 && category == 1"     // 逻辑与
"id < 100 || id > 1000"       // 逻辑或
"!(id == 0)"                   // 逻辑非
```

### 范围查询

```go
"id in [1, 2, 3, 4, 5]"       // 包含
"id not in [100, 200, 300]"   // 不包含
```

### 字符串操作

```go
"text like 'prefix%'"         // 前缀匹配
"text like '%suffix'"         // 后缀匹配
"text like '%contains%'"      // 包含匹配
```

## 错误处理

所有方法都返回详细的错误信息，建议进行适当的错误处理：

```go
if err != nil {
    log.Printf("操作失败: %v", err)
    // 处理错误
}
```

## 并发安全

客户端实例是线程安全的，可以在多个 goroutine 中并发使用。但建议为每个 goroutine 使用独立的客户端实例以获得最佳性能。

## 资源管理

使用完客户端后，应该调用 `Close()` 方法关闭连接：

```go
defer cli.Close()
```

## 示例代码

### 基本使用示例

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/milvus-io/milvus/client/v2/column"
    "github.com/milvus-io/milvus/client/v2/entity"
    "github.com/milvus-io/milvus/client/v2/index"
    "github.com/stones-hub/taurus-pro-milvus/pkg/milvus/client"
)

func main() {
    // 创建客户端
    cli, err := client.NewWithOptions(context.Background(),
        client.WithAddress("localhost:19530"),
        client.WithAuth("root", ""),
        client.WithDatabase("my_db"),
        client.WithRetry(3, 2*time.Second),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer cli.Close()

    ctx := context.Background()

    // 创建集合
    schema := &entity.Schema{
        CollectionName: "my_collection",
        Description:    "示例集合",
        AutoID:         true,
        Fields: []*entity.Field{
            {
                ID:         0,
                Name:       "id",
                DataType:   entity.FieldTypeInt64,
                PrimaryKey: true,
                AutoID:     true,
            },
            {
                ID:       1,
                Name:     "vector",
                DataType: entity.FieldTypeFloatVector,
                TypeParams: map[string]string{
                    "dim": "128",
                },
            },
            {
                ID:       2,
                Name:     "text",
                DataType: entity.FieldTypeVarChar,
                TypeParams: map[string]string{
                    "max_length": "200",
                },
            },
        },
    }

    err = cli.CreateCollection(ctx, schema, 1)
    if err != nil {
        log.Fatal(err)
    }

    // 创建索引
    idx := index.NewIvfFlatIndex(entity.L2, 1024)
    err = cli.CreateIndex(ctx, "my_collection", "vector", idx)
    if err != nil {
        log.Fatal(err)
    }

    // 加载集合
    err = cli.LoadCollection(ctx, "my_collection")
    if err != nil {
        log.Fatal(err)
    }

    // 插入数据
    vectors := [][]float32{
        {0.1, 0.2, 0.3, /* ... 128维向量 */},
        {0.4, 0.5, 0.6, /* ... 128维向量 */},
    }
    texts := []string{"文本1", "文本2"}

    vectorColumn := column.NewColumnFloatVector("vector", 128, vectors)
    textColumn := column.NewColumnVarChar("text", texts)

    ids, err := cli.Insert(ctx, "my_collection", "", vectorColumn, textColumn)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("插入成功，ID: %v", ids)

    // 查询数据
    columns, err := cli.Query(ctx, "my_collection", nil, "id > 0", []string{"text"})
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("查询到 %d 个字段", len(columns))

    // 向量搜索
    searchVector := entity.FloatVector([]float32{0.1, 0.2, 0.3, /* ... 128维向量 */})
    results, err := cli.Search(ctx, "my_collection", nil, []string{"text"}, 
        []entity.Vector{searchVector}, "vector", entity.L2, 5, "", nil)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("搜索到 %d 个结果", len(results))

    // 清理
    cli.ReleaseCollection(ctx, "my_collection")
    cli.DropCollection(ctx, "my_collection")
}
```

## 相关文档

- [主项目README](../../README.md)
- [连接池文档](../README.md)
- [示例程序](../../bin/README.md)
- [Milvus官方文档](https://milvus.io/docs)
