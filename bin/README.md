# Milvus 向量数据库 CRUD 示例

这个示例程序演示了如何使用 `taurus-pro-milvus` 连接池对 Milvus 向量数据库进行完整的增删改查操作。

## 功能特性

- ✅ **连接池管理**: 使用连接池管理 Milvus 客户端连接
- ✅ **数据库操作**: 创建、切换、删除数据库
- ✅ **集合管理**: 创建、加载、释放、删除集合
- ✅ **分区操作**: 创建和管理分区
- ✅ **索引管理**: 为向量字段创建索引
- ✅ **数据插入**: 批量插入向量和标量数据
- ✅ **数据查询**: 条件查询和字段选择
- ✅ **向量搜索**: 相似度搜索和过滤搜索
- ✅ **数据更新**: 通过删除+插入实现更新
- ✅ **数据删除**: 条件删除数据
- ✅ **资源清理**: 自动清理所有创建的资源

## 数据结构

示例中创建的集合包含以下字段：

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `id` | Int64 | 主键，自增 | 1, 2, 3... |
| `vector` | FloatVector | 向量字段，128维 | [0.1, 0.2, 0.3, ...] |
| `text` | VarChar | 文本字段 | "示例文本_1_123" |
| `category` | Int32 | 分类字段 | 1, 2, 3 |
| `score` | Float | 分数字段 | 0.85, 0.92, 0.67 |

## 运行示例

### 1. 确保 Milvus 服务器运行

确保您的 Milvus 服务器正在运行，默认地址为 `192.168.103.113:19530`。

### 2. 修改配置（如需要）

编辑 `main.go` 文件中的配置常量：

```go
const (
    address  = "192.168.103.113:19530"  // 修改为您的 Milvus 地址
    username = "root"                    // 修改为您的用户名
    password = ""                        // 修改为您的密码
    dbName   = "default"                 // 修改数据库名称
    // ... 其他配置
)
```

### 3. 运行示例

```bash
# 进入 bin 目录
cd bin

# 运行示例
go run main.go
```

### 4. 预期输出

程序将按顺序执行以下操作并显示进度：

```
✅ 成功创建Milvus客户端

📁 步骤1: 创建数据库
⚠️ 数据库已存在或创建失败: database already exist: default
ℹ️ 使用默认数据库: default
✅ 成功切换到数据库: default

📚 步骤2: 创建集合
⚠️ 集合已存在，先删除: example_collection
✅ 成功删除已存在的集合: example_collection
✅ 成功创建集合: example_collection

🗂️ 步骤3: 创建分区
✅ 成功创建分区: example_partition

🔍 步骤4: 创建索引
✅ 成功为字段 'vector' 创建IVF_FLAT索引

⚡ 步骤5: 加载集合
✅ 成功加载集合: example_collection

➕ 步骤6: 插入数据
✅ 成功插入 100 条数据，ID范围: &{0xc00011a3f0}

🔍 步骤7: 查询数据
✅ 查询到 4 个字段的数据
📊 前5条数据示例:
  字段 0: text (长度: 0)
  字段 1: category (长度: 0)
  字段 2: score (长度: 0)
  字段 3: id (长度: 0)
✅ 查询到 category==1 的数据，共 3 个字段

🔎 步骤8: 搜索数据
✅ 搜索完成，返回 3 个结果集
🔍 搜索向量 1 的相似结果:
🔍 搜索向量 2 的相似结果:
🔍 搜索向量 3 的相似结果:
✅ 带过滤条件的搜索完成，返回 3 个结果集

✏️ 步骤9: 更新数据
✅ 成功删除 category==2 的数据
✅ 成功插入 10 条更新数据，ID范围: &{0xc00011b030}

🗑️ 步骤10: 删除数据
✅ 成功删除 score < 0.5 的数据
📊 剩余数据: 78 条

🧹 步骤11: 清理资源
✅ 成功释放集合: example_collection
✅ 成功删除集合: example_collection
ℹ️ 保留默认数据库: default
🎉 所有操作执行完成！
```

## 主要操作说明

### 1. 连接池使用

```go
// 创建连接池
pool := milvus.NewPool()
defer pool.Close()

// 添加客户端
err := pool.Add("main_client",
    client.WithAddress(address),
    client.WithAuth(username, password),
    client.WithDatabase(dbName),
    client.WithRetry(3, 2*time.Second),
)

// 获取客户端
cli, err := pool.Get("main_client")
```

### 2. 向量搜索

```go
// 生成搜索向量
searchVectors := generateVectors(3, vectorDim)
searchVectorEntities := make([]entity.Vector, len(searchVectors))
for i, v := range searchVectors {
    searchVectorEntities[i] = entity.FloatVector(v)
}

// 执行搜索
results, err := cli.Search(ctx, collectionName, []string{partitionName}, 
    []string{"text", "category", "score"}, searchVectorEntities, "vector", 
    entity.L2, 5, "", nil)
```

### 3. 条件查询

```go
// 查询所有数据
columns, err := cli.Query(ctx, collectionName, []string{partitionName}, 
    "id > 0", []string{"text", "category", "score"})

// 查询特定条件
columns, err := cli.Query(ctx, collectionName, []string{partitionName}, 
    "category == 1", []string{"text", "score"})
```

### 4. 数据插入

```go
// 创建列数据
vectorColumn := column.NewColumnFloatVector("vector", vectorDim, vectors)
textColumn := column.NewColumnVarChar("text", texts)
categoryColumn := column.NewColumnInt32("category", categories)
scoreColumn := column.NewColumnFloat("score", scores)

// 插入数据
ids, err := cli.Insert(ctx, collectionName, partitionName, 
    vectorColumn, textColumn, categoryColumn, scoreColumn)
```

## 表达式语法说明

Milvus 使用特定的表达式语法，注意以下要点：

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

## 注意事项

1. **服务器连接**: 确保 Milvus 服务器正在运行且可访问
2. **权限配置**: 确保用户有足够的权限进行数据库和集合操作
3. **资源清理**: 程序会自动清理所有创建的资源，避免数据残留
4. **错误处理**: 程序包含完整的错误处理，会显示详细的错误信息
5. **并发安全**: 连接池是并发安全的，可以在多个 goroutine 中使用
6. **表达式语法**: 注意使用 `==` 而不是 `=` 进行比较操作

## 故障排除

### 连接失败
- 检查 Milvus 服务器是否运行
- 验证地址、用户名、密码是否正确
- 检查网络连接

### 权限错误
- 确保用户有创建数据库的权限
- 检查用户是否有集合操作权限

### 内存不足
- 减少 `vectorCount` 的值
- 确保 Milvus 服务器有足够内存

### 表达式语法错误
- 使用 `==` 而不是 `=` 进行比较
- 检查表达式语法是否正确
- 参考上面的表达式语法说明

## 扩展功能

您可以基于这个示例扩展更多功能：

- 批量操作优化
- 异步操作
- 错误重试机制
- 性能监控
- 数据备份和恢复
- 多集合管理
- 复杂查询条件
- 自定义相似度度量
- 数据验证和清洗
- 结果缓存机制

## 性能测试

示例程序包含基本的性能测试，您可以通过修改以下参数来测试不同规模的数据：

```go
const (
    vectorDim   = 128    // 向量维度
    vectorCount = 100    // 向量数量
)
```

建议的测试规模：
- 小规模测试：100-1000 条数据
- 中等规模测试：1000-10000 条数据
- 大规模测试：10000+ 条数据

## 相关文档

- [主项目README](../README.md)
- [Milvus官方文档](https://milvus.io/docs)
- [Go客户端API文档](https://github.com/milvus-io/milvus-sdk-go)
