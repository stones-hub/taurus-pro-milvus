# Milvus Client API 文档

这是 `taurus-pro-milvus` 项目的客户端包，提供了完整的 Milvus 向量数据库操作接口。

## 包结构

```
pkg/milvus/client/
├── client.go      # 客户端实现和接口定义
├── gate.go        # 可取消等待的读写门禁
├── recovery.go    # Search/Query 只读恢复（默认启用）
├── options.go     # 配置选项和选项函数
└── *_test.go      # 单元测试（fake gRPC）与集成测试
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
| `WithRetry` | `maxRetry uint, maxBackoff time.Duration` | 兼容性入口：当前 SDK v2.6.5 不应用此配置，不能据此控制实际重试 | `5, 3*time.Second` |
| `WithReadRecovery` | `enable bool` | Search/Query 只读恢复开关，**默认启用**；传 `false` 关闭。见下文"只读恢复" | `false` |
| `WithReadRecoveryObserver` | `fn func(ReadRecoveryEvent)` | Search/Query 失败与恢复结果回调（首次直接成功除外，每次调用一次） | 见下文 |

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

`vectorField` 作为 `anns_field`（多向量字段集合必须指定）；`metricType` 非空时随请求发送，与索引的 metric 不一致时由服务端返回错误；`params` 写入搜索参数 `params`，形如 `"10"`、`"0.5"`、`"true"` 的值按数字/布尔编码（`nprobe` 即编码为数字 10）；`NaN`/`Inf` 等无法编码为 JSON 的值会被本地拒绝并返回错误。

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

同步协议（可取消的读写门禁）：
- 只读操作（HasCollection、Query、Search、DescribeCollection 等）共享门禁，可并发；变更操作与 `UseDatabase` 独占门禁。门禁按到达顺序（FIFO）授予，写操作不会被持续到达的读操作饿死。
- 请求的 `ctx` 同时约束等待门禁的阶段：被长写操作（如 LoadCollection）占用时，短 deadline/已取消的请求会按 ctx 及时返回 `context.DeadlineExceeded` / `context.Canceled`，且不发出 RPC、不改变数据库状态。门禁在 RPC（含 Await）期间仍被占用，因此长写操作本身会让其他请求排队，直到它们的 ctx 到期。
- `UseDatabase` 与其他操作互斥；若切换失败（含连接握手失败或超时），SDK 内部的当前库会回滚为切换前的库（记录的是 SDK 解析后的库名：`WithDatabase` 显式值优先，为空时取自 `WithAddress` URL 路径中的库名）。
- 错误契约分两阶段：**等待门禁**阶段的取消/超时返回 context 错误（`errors.Is(err, context.Canceled/DeadlineExceeded)` 成立）；**已发出的 RPC**（含 Await 轮询）沿用 SDK/gRPC 错误，超时表现为 gRPC status `DeadlineExceeded`，`errors.Is(err, context.DeadlineExceeded)` 不保证成立，调用方应结合 `status.Code(err)` 判断。
- `Close()` 无 ctx：先拒绝新请求并唤醒所有等待者（返回 `client is closed`），再等待在途操作结束后仅关闭一次 SDK；并发重复调用等待首次关闭完成后返回 nil。`GetClient()` 不阻塞，关闭开始后返回 nil。
- `GetClient()` 返回的裸 SDK 句柄不受上述同步保护，调用方不得让它与 `Close` / `UseDatabase` 并发使用。只读恢复默认启用，此时还不得与 `Search` / `Query` 并发使用，见下文。

## 只读恢复（默认启用）

Milvus 正常重启后，已有连接可能中断，首次检索也可能返回 `channel distribution is not serviceable`（错误码 503）或 `channel tsafe stalled`（错误码 505）。`New`、`NewWithOptions` 以及连接池 `Add`/`MustGet` 创建的客户端**默认启用**只读恢复，无需传入任何选项：`Search` / `Query` 遇下列暂时性故障时在预算内重建底层连接并重试，超出预算明确失败；不保证服务故障期间一定成功。

默认启用意味着：
- 每次 `Search` / `Query` 自调用开始最多 60 秒（`ReadRecoveryBudget`），即使调用方 ctx 更长或为 `context.Background()`；调用方 ctx 更短时以 ctx 为准。需要更长的单次查询时请关闭恢复。
- 需要原有的单次执行语义时，显式传 `client.WithReadRecovery(false)` 关闭。选项按传入顺序生效，后者覆盖前者。

```go
// 默认即启用，observer 可选；只记录固定 Op、错误分类、次数与耗时，不输出错误原文与集合名
cli, err := client.NewWithOptions(ctx,
    client.WithAddress("localhost:19530"),
    client.WithAuth("root", ""),
    client.WithReadRecoveryObserver(func(ev client.ReadRecoveryEvent) {
        log.Printf("milvus %s read recovery: attempts=%d elapsed=%s first=%s result=%s",
            ev.Op, ev.Attempts, ev.Elapsed, classify(ev.FirstErr), classify(ev.Err))
    }),
)

// 关闭只读恢复
cli, err := client.NewWithOptions(ctx,
    client.WithAddress("localhost:19530"),
    client.WithReadRecovery(false),
)

// classify 按 errors.Is 白名单返回固定分类，从不使用错误原文。
// merr 为 github.com/milvus-io/milvus/pkg/v2/util/merr。Milvus 业务错误码（1/2/503/505 等）
// 经 status.Code 多为 Unknown，必须用 errors.Is 判断；status.Code 只作传输层兜底。
func classify(err error) string {
    switch {
    case err == nil:
        return "ok"
    case errors.Is(err, client.ErrReadRecoveryExhausted):
        return "recovery_exhausted"
    case errors.Is(err, client.ErrReadRecoveryDatabaseChanged):
        return "database_changed"
    case errors.Is(err, context.DeadlineExceeded):
        return "deadline"
    case errors.Is(err, context.Canceled):
        return "canceled"
    case errors.Is(err, merr.ErrChannelNotAvailable):
        return "channel_not_available"
    case errors.Is(err, merr.ErrChannelTSafeStalled):
        return "tsafe_stalled"
    case errors.Is(err, merr.ErrServiceNotReady), errors.Is(err, merr.ErrServiceUnavailable):
        return "service_not_ready"
    }
    switch status.Code(err) {
    case codes.Unavailable:
        return "unavailable"
    case codes.DeadlineExceeded:
        return "deadline"
    case codes.Canceled:
        return "canceled"
    }
    return "other"
}
```

**适用范围**：仅 `Search`、`Query`。本库从不对写操作（Insert/Delete/Create*/Drop*/Load*/Release* 等）、`UseDatabase` 及其他读方法自动重试。关闭恢复（`WithReadRecovery(false)`）时，`Search` / `Query` 单次执行并原样返回错误，`Close` 等待在途查询自然结束，与引入恢复之前的版本一致。注意：这与 SDK 自身的重试无关——Milvus Go SDK v2.6.5 对所有一元 RPC（含写操作）内置拦截器，遇 gRPC `Unavailable` / `ResourceExhausted` 最多尝试 6 次（退避合计约 22 秒），不受本选项控制。

**可恢复错误**（按 gRPC 状态码 / Milvus 错误码判断，不做字符串匹配）：
- gRPC `Unavailable`：连接中断或拒绝（SDK 内部已对其短暂重试）；
- Milvus `ErrServiceNotReady`(1)、`ErrServiceUnavailable`(2)：服务启动中或暂不可用；
- Milvus `ErrChannelNotAvailable`(503)：重启后查询分发尚未可服务；
- Milvus `ErrChannelTSafeStalled`(505)：容器 stop/start 后通道 tSafe 滞后。tSafe 由服务端推进，重建连接不保证能修好；持续出现时按预算以 `ErrReadRecoveryExhausted` 明确失败。

其余错误立即原样返回，包括 ctx 取消/超时、认证、权限、参数、集合未加载/不存在、限流、相邻的通道错误码（如 504、506）及其他标记为 retryable 但未列出的错误码，以及文本里带 "503"、"tsafe stalled" 等字样的其他错误。

**流程与预算**：启用时（默认），`Search` / `Query` 自调用开始即受总预算 `ReadRecoveryBudget`（60 秒）约束。预算覆盖首次门禁等待与首次查询，以及之后的全部恢复阶段，并服从调用方更短的 ctx；调用方 ctx 为 `context.Background()` 也不会无限阻塞。首次失败可恢复时，每轮等待 `ReadRecoveryInterval`（5 秒）→ 重建底层 SDK 客户端（握手受同一预算限时）→ 若集合/所查分区处于 Loading 则等待其完成 → 重试查询。服务端完全不可达时，重建拨号（阻塞式）会一直等到连上或预算用尽，而不是每 5 秒重拨。预算到期的处理：
- 由 ctx 截断引起的错误，以及到期时仍属可恢复的错误，记为 `ErrReadRecoveryExhausted`；
- 恰在到期时出现的其他真实错误（如权限、集合未加载）原样保留：首次查询原样返回，重试时作为 `Last`；
- 调用方自身 ctx 先到期时，首次查询按原样返回 ctx/gRPC 错误。

恢复只由 `Search` / `Query` 请求驱动：本库没有后台重连、健康轮询或自动重放。一次调用失败（含预算耗尽、调用方超时/取消）后，本库不再为它拨号、查询或查询加载状态；下一次 `Search` / `Query` 以自己的 ctx 和预算从头执行，若仍遇可恢复错误则重新按上述流程重建。gRPC 对已有连接的内部重连不属于本库的恢复。

恢复后的结果须通过校验：Search 的结果集数等于查询向量数且各结果集无解析错误；Query 的各列行数一致。

**Close**：启用恢复时，`Close` 会先取消进行中的 `Search` / `Query`（包括首次查询）和恢复，再等待已开始的重建结束，然后按原语义拒绝新请求、等待其他在途操作并关闭当前 SDK 客户端。
- 被 Close 终止的 `Search` / `Query` 视时机返回原样的 `client is closed`，或 `Last` 为它的 `*ReadRecoveryError`（两者都会触发一次 observer）。两种情况下 `err.Error()` 都包含 `client is closed`，请据此判断，不要依赖具体类型。
- `Close` 返回时，本库创建的 SDK 客户端（包括重建中尚未安装的候选）都已调用过关闭；握手失败的连接也已关闭。服务端感知 TCP 断开是异步的，被终止的查询 goroutine 也可能稍后才返回。
- 等待重建依赖其 ctx 已被取消、握手与门禁等待及时返回；该重建不持有门禁，不会与 Close 互锁。
- 关闭恢复（`WithReadRecovery(false)`）时，`Close` 照旧等待在途查询自然结束。

**数据库一致性**：重试只在首次查询所用的库上进行，以下情况都以 `ErrReadRecoveryDatabaseChanged` 停止恢复，绝不改到其他库上查询或查询加载状态：
- 恢复期间 `UseDatabase` 切换了库：本次重建结果作废，保留调用方的切换。同库的并发等待者共享这一结果，不会各自再建连。
- 重建时 SDK 实际解析的库与之不同：例如地址 URL 带库（`http://host:19530/urldb`）而当前库被 `UseDatabase("")` 置空，SDK 新建客户端会改用 URL 库。此时放弃新客户端，该客户端在这种状态下无法恢复。

**加载意图**：恢复从不发起 `LoadCollection` / `LoadPartitions` / `Release*`，不会改变副本等加载配置，也不会把调用方已释放的集合重新加载。集合处于 Loading 时只等待；处于 NotLoad 时直接重试查询，由服务端返回真实错误（如 collection not loaded）。

**连接替换**：客户端对象本身保持不变（连接池 `Get`/`MustGet` 返回的对象继续可用）。内部按代际替换底层 SDK 客户端：
- 同时至多进行一个重建，同库同代上的并发失败共享其结果；
- 加载状态查询失败时，下一轮重建该查询实际使用的代际；
- 替换在写门禁下进行，因此经本库发出的旧代在途请求都结束后才关闭旧客户端；
- 重建继承创建时的全部配置（地址、认证/APIKey、TLS、拨号/keepalive/退避、消息大小、DisableConn），数据库取首次查询所用的库；
- 内部替换不会调用对外的 `Close`，也不会让客户端进入终态。

**失败与诊断**：恢复仍失败、或首次门禁等待/首次查询被预算/Close 截断时，返回 `*client.ReadRecoveryError`。`Last` 为终止原因：不可恢复错误、调用方 ctx 错误、`client.ErrReadRecoveryExhausted`、`client.ErrReadRecoveryDatabaseChanged`，或 `client is closed`。`First` 为首次失败，只用于诊断（字段与错误文本）。首次即不可恢复、或调用方自身 ctx 先结束时不进入恢复，错误原样返回（不包装）。
- **判定只依据 `Last`**：`Unwrap` 只返回 `Last`，因此 `errors.Is` / `errors.As` 不会命中首次失败（往往是可恢复错误）。
- **`status.Code(err)` 映射**：`context.DeadlineExceeded` 与 `ErrReadRecoveryExhausted` 为 `DeadlineExceeded`；`context.Canceled` 为 `Canceled`；`Last` 自带 gRPC 状态时沿用其码；其余为 `Unknown`。

**observer 约定**：仅在启用恢复时生效（`WithReadRecovery(false)` 关闭后设置会被忽略、从不回调）。启用时，`Search` / `Query` 首次查询直接成功不回调；其余每次调用在返回前**恰好回调一次**，覆盖全部最终出口：
- 首次即不可恢复的错误、调用方自身 ctx 超时/取消（含首次门禁等待中）、Close 开始后被门禁拒绝：错误原样返回，`ev.Err` 与 `ev.FirstErr` 为同一值；
- 首次门禁等待或首次查询被预算/Close 截断：`ev.Err` 为 `*ReadRecoveryError`；
- 进入恢复后失败：`ev.Err` 为 `*ReadRecoveryError`；进入恢复后成功：`ev.Err` 为 nil（此时首次失败只能经 `ev.FirstErr` 获得）。

事件字段：
- `ev.Err` 与返回给调用方的错误是同一个值；`ev.FirstErr` 为首次尝试（含首次门禁等待）的错误，事件中恒非 nil。
- `ev.Attempts` 为越过门禁（及库校验）后实际执行查询的次数（含首次）。`0` 表示首次门禁等待即失败、未发出查询；计入的执行也不保证请求到达服务端（如 Search 参数在本地构建即失败）；SDK 内部对 `Unavailable` 的重试不计入。`*ReadRecoveryError.Attempts` 同此语义。
- `ev.Elapsed` 为自调用开始到产生事件的耗时。

其他约定：
- 回调在查询所在的 goroutine 中同步执行，多个查询可能并发调用它，因此回调必须并发安全、不得阻塞。
- 回调耗时既不计入预算，也不受调用方 ctx 约束，慢回调会让调用晚于预算/deadline 返回。回调中的 panic 会传到 `Search` / `Query` 的调用方。
- 本库不在错误与事件中加入认证信息、向量或过滤表达式，但**不对错误与集合名脱敏**：
  - `FirstErr` / `Err` 是服务端/SDK 的原样错误，文本可能含集合、通道、用户名、服务地址等上下文，不要输出错误原文（包括 `%v` / `err.Error()`）。
  - `ev.Collection` 是调用方传入的原值，本库不校验也不转义，可能含租户/用户标识或换行等控制字符，默认不要记录。确需区分集合时，由调用方把它映射为自己定义的安全标识（如固定的业务枚举）再记录；仅做转义不等于脱敏。
  - 默认只记录固定的 `Op`、错误分类、`Attempts`、`Elapsed`。错误分类用 `errors.Is` 白名单（见上方示例 `classify`）：`status.Code` 只反映 gRPC 状态，Milvus 业务错误（如 merr 1/2/503/505）多为 `Unknown`，无法据此精确分类，只能作传输层兜底。
- 本库自身不输出日志。

**`GetClient()` 边界**：裸 SDK 句柄不参与恢复，本库不承诺它透明恢复：恢复只替换本库内部持有的 SDK 客户端，调用方先前取得的裸句柄会被关闭而不会自动更新。本库也不为它提供任何恢复相关的并发安全保证。恢复默认启用，任何并发的 `Search` / `Query` 都可能触发重建，在裸句柄调用途中把它关闭。SDK 的 `Close` 会无锁清空内部连接字段，因此这种并发不只是调用失败，而是数据竞争。重新调用 `GetClient()` 只能避开已经完成的替换，无法保护随后仍在进行的调用。因此在默认配置下，二选一：
- 需要裸 SDK 能力时，另建一个以 `WithReadRecovery(false)` 关闭恢复的独立客户端；
- 或由调用方确保使用裸句柄期间没有并发的 `Search` / `Query` / `Close` / `UseDatabase`，且不长期缓存裸句柄。

关闭恢复时，裸句柄的生命周期与引入恢复之前一致。

**限制**：重建需要获取写门禁，因此会等待经本库的在途请求，包括仍在 SDK 内部对 `Unavailable` 做重试的请求（最长约 22 秒）。这段时间内新请求排队，仍受各自 ctx 与预算约束。

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
