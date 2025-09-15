# Taurus Pro Milvus

Taurus Pro Milvus 是一个基于 [Milvus](https://milvus.io/) 向量数据库的 Go 语言客户端封装库。它提供了简洁易用的 API 接口，帮助您快速集成 Milvus 向量搜索功能到您的应用中。

## 特性

- 完整的集合和分区管理
- 支持多种索引类型（IVF_FLAT、IVF_SQ8、IVF_PQ）
- 向量搜索和属性过滤
- 异步操作支持
- 连接池和重试机制
- 完善的错误处理
- 丰富的使用示例

## 安装

```bash
go get github.com/your-username/taurus-pro-milvus
```

## 快速开始

### 1. 创建客户端

```go
import "github.com/your-username/taurus-pro-milvus/pkg/milvus/client"

// 创建客户端
cli, err := client.New(
    client.WithAddress("localhost:19530"),
    client.WithAuth("root", "password"),
    client.WithDatabase("default"),
)
if err != nil {
    log.Fatal(err)
}
defer cli.Close()
```

### 2. 创建集合

```go
// 定义集合结构
schema := &entity.Schema{
    CollectionName: "user_profiles",
    Description:    "User profile collection with feature vectors",
    Fields: []*entity.Field{
        {Name: "id", DataType: entity.FieldTypeInt64, PrimaryKey: true, AutoID: true},
        {Name: "name", DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "100"}},
        {Name: "age", DataType: entity.FieldTypeInt64},
        {Name: "feature", DataType: entity.FieldTypeFloatVector, TypeParams: map[string]string{"dim": "128"}},
    },
}

// 创建集合
err = cli.CreateCollection(ctx, schema, 2)
if err != nil {
    log.Fatal(err)
}
```

### 3. 创建索引

```go
// 创建 IVF_FLAT 索引
indexParams, err := entity.NewIndexIvfFlat(entity.L2, 1024)
if err != nil {
    log.Fatal(err)
}

err = cli.CreateIndex(ctx, "user_profiles", "feature", indexParams, false)
if err != nil {
    log.Fatal(err)
}
```

### 4. 插入数据

```go
// 准备向量数据
vectors := make([][]float32, 2)
for i := range vectors {
    vectors[i] = make([]float32, 128)
    for j := range vectors[i] {
        vectors[i][j] = float32(i*128 + j)
    }
}

// 准备插入的列数据
columns := []entity.Column{
    entity.NewColumnVarChar("name", []string{"张三", "李四"}),
    entity.NewColumnInt64("age", []int64{25, 30}),
    entity.NewColumnFloatVector("feature", 128, vectors),
}

// 执行插入
idColumn, err := cli.Insert(ctx, "user_profiles", "", columns...)
if err != nil {
    log.Fatal(err)
}
```

### 5. 向量搜索

```go
// 加载集合
err = cli.LoadCollection(ctx, "user_profiles", false)
if err != nil {
    log.Fatal(err)
}
defer cli.ReleaseCollection(ctx, "user_profiles")

// 设置搜索参数
searchParams, err := entity.NewIndexIvfFlatSearchParam(10)
if err != nil {
    log.Fatal(err)
}

// 执行搜索
results, err := cli.Search(
    ctx,
    "user_profiles",
    nil,                      // 搜索所有分区
    "age >= 18",             // 过滤条件
    []string{"name", "age"}, // 返回字段
    []entity.Vector{entity.FloatVector(vectors[0])},
    "feature",
    entity.L2,               // 使用L2距离
    3,                       // 返回top 3结果
    searchParams,
)
if err != nil {
    log.Fatal(err)
}

// 处理搜索结果
for _, result := range results {
    scores := result.Scores  // 相似度分数
    ids := result.IDs       // 匹配的ID
    fields := result.Fields // 返回的字段值
}
```

## 高级功能

### 分区管理

```go
// 创建分区
err = cli.CreatePartition(ctx, "user_profiles", "vip_users")

// 插入数据到指定分区
_, err = cli.Insert(ctx, "user_profiles", "vip_users", columns...)

// 仅加载指定分区
err = cli.LoadPartitions(ctx, "user_profiles", []string{"vip_users"}, false)
```

### 异步操作

```go
// 异步创建索引
err = cli.CreateIndex(ctx, "user_profiles", "feature", indexParams, true)

// 等待索引创建完成
for {
    state, err := cli.GetIndexState(ctx, "user_profiles", "feature")
    if err != nil {
        log.Fatal(err)
    }
    if state == entity.IndexStateCreated {
        break
    }
    time.Sleep(time.Second)
}
```

### 条件查询

```go
// 执行属性查询
results, err := cli.Query(
    ctx,
    "user_profiles",
    nil,
    "age >= 25 and name like '张%'",
    []string{"name", "age", "feature"},
)
```

## 配置选项

客户端支持多种配置选项：

```go
cli, err := client.New(
    // 基础配置
    client.WithAddress("localhost:19530"),
    client.WithDatabase("default"),
    
    // 认证配置
    client.WithAuth("user", "pass"),     // 用户名密码认证
    client.WithAPIKey("your-api-key"),   // API Key认证
    
    // 连接配置
    client.WithConnectTimeout(5*time.Second),
    client.WithRetry(100, 5*time.Second),
    client.WithKeepAlive(15*time.Second, 30*time.Second),
)
```

## 最佳实践

1. **资源管理**
   - 使用完毕后及时释放集合/分区
   - 使用 defer 确保资源正确释放
   - 合理使用异步操作提高性能

2. **索引选择**
   - IVF_FLAT: 最准确，但空间占用大
   - IVF_SQ8: 准确度和空间的平衡选择
   - IVF_PQ: 空间占用最小，但准确度较低

3. **性能优化**
   - 批量插入数据而不是单条插入
   - 合理设置分区数量
   - 根据数据量调整索引参数

## 示例代码

完整的示例代码可以在 [example](./example) 目录下找到：

- [基础示例](./example/example.go)
- [向量搜索示例](./example/vector_search/main.go)

## 许可证

本项目采用 MIT 许可证，详见 [LICENSE](./LICENSE) 文件。
