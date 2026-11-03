# Taurus Pro Milvus

一个高性能的 Go 语言 Milvus 向量数据库客户端库，提供连接池管理和完整的 CRUD 操作支持。

## 特性

- 🚀 **高性能连接池**：按名称注册并复用多个客户端（不做请求级负载均衡），建连有超时且不阻塞其他名称
- 🔧 **完整的 CRUD 操作**：支持集合、分区、索引、数据的增删改查
- 🎯 **向量搜索**：支持多种相似度度量（L2、IP、COSINE）的向量搜索
- 🛡️ **并发安全**：所有操作都是线程安全的
- 📊 **灵活配置**：支持丰富的客户端配置选项
- 🔄 **SDK 重试**：沿用底层 SDK 的固定重试策略；当前 `WithRetry` 配置不控制实际重试次数，详见下文
- 🧹 **资源管理**：自动资源清理，防止内存泄漏

## 测试

普通 `go test ./...` 不连接任何 Milvus 服务器；需要真实服务的集成测试默认跳过。显式开启：

```bash
MILVUS_INTEGRATION=1 MILVUS_TEST_ADDRESS=127.0.0.1:19530 MILVUS_TEST_USERNAME=root MILVUS_TEST_PASSWORD=Milvus go test -race -count=1 ./...
```

开启后缺少 `MILVUS_TEST_ADDRESS` 或连接失败会直接失败，不会跳过；用户名/密码可选。测试自行创建唯一命名的数据库、集合和别名并清理。

## 安装

```bash
go get github.com/stones-hub/taurus-pro-milvus
```

## 快速开始

### 基本使用

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/stones-hub/taurus-pro-milvus/pkg/milvus"
    "github.com/stones-hub/taurus-pro-milvus/pkg/milvus/client"
)

func main() {
    // 创建连接池
    pool := milvus.NewPool()
    defer pool.Close()

    // 添加客户端
    err := pool.Add("main_client",
        client.WithAddress("localhost:19530"),
        client.WithAuth("root", ""),
        client.WithDatabase("default"),
        client.WithRetry(3, 2*time.Second),
    )
    if err != nil {
        panic(err)
    }

    // 获取客户端
    cli, err := pool.Get("main_client")
    if err != nil {
        panic(err)
    }

    // 使用客户端进行操作
    ctx := context.Background()
    // ... 执行数据库操作
}
```

### 使用 MustGet 自动创建客户端

```go
// 如果客户端不存在，会自动创建
cli, err := pool.MustGet("auto_client",
    client.WithAddress("localhost:19530"),
    client.WithAuth("root", ""),
)
```

## 连接池管理

### 创建连接池

```go
pool := milvus.NewPool()
defer pool.Close()
```

### 添加客户端

```go
err := pool.Add("client1",
    client.WithAddress("192.168.1.100:19530"),
    client.WithAuth("root", "password"),
    client.WithDatabase("my_db"),
    client.WithRetry(5, 3*time.Second),
)
```

### 获取客户端

```go
// 获取已存在的客户端
cli, err := pool.Get("client1")

// 获取或创建客户端
cli, err := pool.MustGet("client2", opts...)
```

### 管理客户端

```go
// 检查客户端是否存在
exists := pool.Has("client1")

// 列出所有客户端
clients := pool.List()

// 移除客户端
err := pool.Remove("client1")
```

## 客户端配置

### 基本配置

```go
client.WithAddress("localhost:19530")        // 服务器地址
client.WithAuth("username", "password")      // 认证信息
client.WithDatabase("database_name")         // 数据库名称
client.WithAPIKey("your_api_key")            // API密钥认证
client.WithTLS()                             // 启用TLS
```

### 重试配置

**版本限制：**当前 SDK v2.6.5 未启用消费 `RetryRateLimit` 的拦截器，因此本库 `WithRetry` 虽会传入配置，但不会改变实际重试次数或退避。SDK 自身另有针对 `Unavailable` / `ResourceExhausted` 的固定重试策略。生产调用必须设置请求 context 的 deadline，不要依赖此选项限制耗时；本次未实现自定义重试策略。

```go
client.WithRetry(5, 3*time.Second)           // 兼容性配置入口；当前 SDK v2.6.5 不应用此配置
```

### 高级GRPC配置

```go
client.WithGrpcOpts(
    5*time.Second,    // keepaliveTime
    10*time.Second,   // keepaliveTimeout
    true,             // permitWithoutStream
    100*time.Millisecond, // baseDelay
    1.6,              // multiplier
    0.2,              // jitter
    3*time.Second,    // maxDelay
    3*time.Second,    // minConnectTimeout
    4*1024*1024,      // maxRecvMsgSize
)
```

## 数据库操作

### 创建和管理数据库

```go
// 创建数据库
err := cli.CreateDatabase(ctx, "my_database")

// 切换数据库
err := cli.UseDatabase(ctx, "my_database")

// 删除数据库
err := cli.DropDatabase(ctx, "my_database")
```

## 集合操作

### 创建集合

```go
import (
    "github.com/milvus-io/milvus/client/v2/entity"
)

schema := &entity.Schema{
    CollectionName: "my_collection",
    Description:    "我的集合",
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

err := cli.CreateCollection(ctx, schema, 1)
```

### 集合管理

```go
// 检查集合是否存在
exists, err := cli.HasCollection(ctx, "my_collection")

// 加载集合到内存
err := cli.LoadCollection(ctx, "my_collection")

// 释放集合
err := cli.ReleaseCollection(ctx, "my_collection")

// 获取集合统计信息
stats, err := cli.GetCollectionStatistics(ctx, "my_collection")

// 描述集合
collection, err := cli.DescribeCollection(ctx, "my_collection")

// 删除集合
err := cli.DropCollection(ctx, "my_collection")
```

## 分区操作

```go
// 创建分区
err := cli.CreatePartition(ctx, "my_collection", "partition_1")

// 检查分区是否存在
exists, err := cli.HasPartition(ctx, "my_collection", "partition_1")

// 加载分区
err := cli.LoadPartitions(ctx, "my_collection", []string{"partition_1"})

// 释放分区
err := cli.ReleasePartitions(ctx, "my_collection", []string{"partition_1"})

// 删除分区
err := cli.DropPartition(ctx, "my_collection", "partition_1")
```

## 索引操作

```go
import (
    "github.com/milvus-io/milvus/client/v2/index"
)

// 创建IVF_FLAT索引
idx := index.NewIvfFlatIndex(entity.L2, 1024)
err := cli.CreateIndex(ctx, "my_collection", "vector", idx)

// 删除索引
err := cli.DropIndex(ctx, "my_collection", "vector")
```

## 数据操作

### 插入数据

```go
import (
    "github.com/milvus-io/milvus/client/v2/column"
)

// 准备数据
vectors := [][]float32{
    {0.1, 0.2, 0.3, ...}, // 128维向量
    {0.4, 0.5, 0.6, ...},
}
texts := []string{"文本1", "文本2"}

// 创建列数据
vectorColumn := column.NewColumnFloatVector("vector", 128, vectors)
textColumn := column.NewColumnVarChar("text", texts)

// 插入数据
ids, err := cli.Insert(ctx, "my_collection", "partition_1", vectorColumn, textColumn)
```

### 查询数据

```go
// 查询所有数据
columns, err := cli.Query(ctx, "my_collection", nil, "id > 0", []string{"text", "id"})

// 查询特定条件的数据
columns, err := cli.Query(ctx, "my_collection", []string{"partition_1"}, 
    "category == 1", []string{"text", "score"})
```

### 向量搜索

```go
import (
    "github.com/milvus-io/milvus/client/v2/entity"
)

// 准备搜索向量
searchVectors := [][]float32{
    {0.1, 0.2, 0.3, ...}, // 128维搜索向量
}
searchVectorEntities := make([]entity.Vector, len(searchVectors))
for i, v := range searchVectors {
    searchVectorEntities[i] = entity.FloatVector(v)
}

// 执行搜索
results, err := cli.Search(ctx, "my_collection", nil, 
    []string{"text", "score"}, searchVectorEntities, "vector", 
    entity.L2, 10, "", nil)

// 带过滤条件的搜索
results, err := cli.Search(ctx, "my_collection", []string{"partition_1"}, 
    []string{"text"}, searchVectorEntities, "vector", 
    entity.COSINE, 5, "category == 1", nil)
```

### 删除数据

```go
// 删除特定条件的数据
err := cli.Delete(ctx, "my_collection", "partition_1", "id > 100")

// 删除所有数据
err := cli.Delete(ctx, "my_collection", "", "id > 0")
```

## 别名操作

```go
// 创建别名
err := cli.CreateAlias(ctx, "my_collection", "my_alias")

// 修改别名
err := cli.AlterAlias(ctx, "my_collection", "new_alias")

// 删除别名
err := cli.DropAlias(ctx, "my_alias")
```

## 压缩操作

```go
// 压缩集合
compactionID, err := cli.Compact(ctx, "my_collection")
```

## 完整示例

查看 `bin/main.go` 文件获取完整的CRUD操作示例，包括：

- 连接池管理
- 数据库和集合创建
- 向量数据插入
- 相似度搜索
- 条件查询
- 数据更新和删除
- 资源清理

运行示例：

```bash
cd bin
go run main.go
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

所有操作都返回详细的错误信息，建议进行适当的错误处理：

```go
if err != nil {
    log.Printf("操作失败: %v", err)
    // 处理错误
}
```

## 性能优化建议

1. **使用连接池**：避免频繁创建和销毁客户端连接
2. **批量操作**：尽量使用批量插入和查询
3. **合理设置索引**：根据查询模式选择合适的索引类型
4. **分区策略**：合理使用分区提高查询性能
5. **内存管理**：及时释放不需要的集合和分区

## 并发安全

- 连接池是线程安全的，可以在多个 goroutine 中并发使用
- 客户端实例也是线程安全的
- 建议为每个 goroutine 使用独立的客户端实例

## 依赖项

- Go 1.27.3+
- Milvus 2.x
- 相关依赖包会自动安装

## 许可证

本项目采用 MIT 许可证。详见 [LICENSE](LICENSE) 文件。

## 贡献

欢迎提交 Issue 和 Pull Request！

## 更新日志

### v1.0.0
- 初始版本发布
- 支持基本的 CRUD 操作
- 支持连接池管理
- 支持向量搜索
- 完整的测试覆盖

## 联系方式

如有问题或建议，请通过以下方式联系：

- 提交 GitHub Issue
- 发送邮件至项目维护者

---

**注意**：使用前请确保 Milvus 服务器正在运行且可访问。
