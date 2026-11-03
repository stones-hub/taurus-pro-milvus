# Milvus 连接池

这是 `taurus-pro-milvus` 项目的连接池包，提供了高效的 Milvus 客户端连接池管理功能。

## 包结构

```
pkg/milvus/
├── pool.go      # 连接池实现
├── pool_test.go # 连接池测试
└── client/      # 客户端包
    ├── client.go
    ├── options.go
    └── client_test.go
```

## 核心接口

### Pool 接口

```go
type Pool interface {
    // Get 获取指定名称的客户端，如果不存在则返回错误
    Get(name string) (client.Client, error)

    // MustGet 获取指定名称的客户端，如果不存在则创建新的客户端
    MustGet(name string, opts ...client.Option) (client.Client, error)

    // Add 添加一个新的客户端
    Add(name string, opts ...client.Option) error

    // Remove 移除一个客户端
    Remove(name string) error

    // Has 检查是否存在指定名称的客户端
    Has(name string) bool

    // List 列出所有已添加的客户端名称
    List() []string

    // Close 关闭所有客户端连接
    Close() error
}
```

## 快速开始

### 创建连接池

```go
import (
    "github.com/stones-hub/taurus-pro-milvus/pkg/milvus"
    "github.com/stones-hub/taurus-pro-milvus/pkg/milvus/client"
)

// 创建连接池
pool := milvus.NewPool()
defer pool.Close()
```

### 添加客户端

```go
// 添加客户端到连接池
err := pool.Add("main_client",
    client.WithAddress("localhost:19530"),
    client.WithAuth("root", ""),
    client.WithDatabase("my_db"),
    client.WithRetry(3, 2*time.Second),
)
if err != nil {
    log.Fatal(err)
}
```

### 获取客户端

```go
// 获取已存在的客户端
cli, err := pool.Get("main_client")
if err != nil {
    log.Fatal(err)
}

// 获取或创建客户端（推荐）
cli, err := pool.MustGet("auto_client",
    client.WithAddress("localhost:19530"),
    client.WithAuth("root", ""),
)
if err != nil {
    log.Fatal(err)
}
```

## API 方法详解

### NewPool

```go
// 创建一个新的 Milvus 客户端连接池
func NewPool() Pool
```

**返回值**：
- `Pool`: 连接池实例

**示例**：
```go
pool := milvus.NewPool()
defer pool.Close()
```

### Add

```go
// 添加一个新的客户端
func (p *pool) Add(name string, opts ...client.Option) error
```

**参数**：
- `name string`: 客户端名称，用于标识客户端
- `opts ...client.Option`: 客户端配置选项

**返回值**：
- `error`: 错误信息，如果客户端已存在则返回错误

**示例**：
```go
err := pool.Add("client1",
    client.WithAddress("localhost:19530"),
    client.WithAuth("root", ""),
    client.WithDatabase("my_db"),
)
```

### Get

```go
// 获取指定名称的客户端，如果不存在则返回错误
func (p *pool) Get(name string) (client.Client, error)
```

**参数**：
- `name string`: 客户端名称

**返回值**：
- `client.Client`: 客户端实例
- `error`: 错误信息，如果客户端不存在则返回错误

**示例**：
```go
cli, err := pool.Get("client1")
if err != nil {
    log.Printf("客户端不存在: %v", err)
    return
}
```

### MustGet

```go
// 获取指定名称的客户端，如果不存在则创建新的客户端
func (p *pool) MustGet(name string, opts ...client.Option) (client.Client, error)
```

**参数**：
- `name string`: 客户端名称
- `opts ...client.Option`: 客户端配置选项（仅在创建新客户端时使用）

**返回值**：
- `client.Client`: 客户端实例
- `error`: 错误信息

**示例**：
```go
cli, err := pool.MustGet("client1",
    client.WithAddress("localhost:19530"),
    client.WithAuth("root", ""),
)
if err != nil {
    log.Fatal(err)
}
```

### Remove

```go
// 移除一个客户端
func (p *pool) Remove(name string) error
```

**参数**：
- `name string`: 客户端名称

**返回值**：
- `error`: 错误信息，如果客户端不存在则返回错误

**示例**：
```go
err := pool.Remove("client1")
if err != nil {
    log.Printf("移除客户端失败: %v", err)
}
```

### Has

```go
// 检查是否存在指定名称的客户端
func (p *pool) Has(name string) bool
```

**参数**：
- `name string`: 客户端名称

**返回值**：
- `bool`: 是否存在

**示例**：
```go
if pool.Has("client1") {
    log.Println("客户端存在")
} else {
    log.Println("客户端不存在")
}
```

### List

```go
// 列出所有已添加的客户端名称
func (p *pool) List() []string
```

**返回值**：
- `[]string`: 客户端名称列表

**示例**：
```go
clients := pool.List()
log.Printf("当前连接池中有 %d 个客户端: %v", len(clients), clients)
```

### Close

```go
// 取消进行中的创建并关闭所有客户端；Close 是终态
func (p *pool) Close() error
```

Close 之后连接池不可复用：`Add`/`MustGet`/`Get`/`Remove` 返回 `ErrPoolClosed`，不会"复活"；重复调用 `Close` 返回 nil。Close 时仍在建连的客户端会被取消，建好后也会立即关闭，不会泄漏。

**返回值**：
- `error`: 错误信息，如果有客户端关闭失败则返回错误

**示例**：
```go
err := pool.Close()
if err != nil {
    log.Printf("关闭连接池时出错: %v", err)
}
```

## 连接池语义

- 连接池是**按名称注册、复用客户端**的注册表，不做请求级负载均衡。
- 建连在池锁之外执行，一个名称连不上不会阻塞其他名称的 `Get`/`Has`/`List`/`Remove`/`Close`。
- 同名并发 `MustGet` 共享同一次创建（成功或失败），后到调用方的配置被忽略；`Add` 遇到已存在或正在创建的名称返回 `already exists`。
- 建连默认超时 `DefaultCreateTimeout`（10 秒），可用 `WithCreateTimeout` 调整。
- `Pool` 接口保持不变。需要 context 控制时使用 `NewContextPool`，其 `AddContext` / `MustGetContext` 在 ctx 取消或超时时立即返回；等待同一次创建的调用方可各自独立取消，最后一个等待者离开时创建被取消。
- `Remove` 正在创建的名称会取消创建；`Remove` 在池锁之外关闭客户端。

```go
pool := milvus.NewContextPool(milvus.WithCreateTimeout(5 * time.Second))
defer pool.Close()

ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()
cli, err := pool.MustGetContext(ctx, "main", client.WithAddress("localhost:19530"))
```

## 使用模式

### 1. 基本使用模式

```go
func main() {
    // 创建连接池
    pool := milvus.NewPool()
    defer pool.Close()

    // 添加客户端
    err := pool.Add("main_client",
        client.WithAddress("localhost:19530"),
        client.WithAuth("root", ""),
    )
    if err != nil {
        log.Fatal(err)
    }

    // 获取客户端
    cli, err := pool.Get("main_client")
    if err != nil {
        log.Fatal(err)
    }

    // 使用客户端进行操作
    // ...
}
```

### 2. 自动创建模式

```go
func main() {
    pool := milvus.NewPool()
    defer pool.Close()

    // 自动创建客户端
    cli, err := pool.MustGet("auto_client",
        client.WithAddress("localhost:19530"),
        client.WithAuth("root", ""),
    )
    if err != nil {
        log.Fatal(err)
    }

    // 使用客户端进行操作
    // ...
}
```

### 3. 多客户端模式

```go
func main() {
    pool := milvus.NewPool()
    defer pool.Close()

    // 添加多个客户端
    clients := []struct {
        name     string
        address  string
        username string
        password string
    }{
        {"client1", "server1:19530", "root", ""},
        {"client2", "server2:19530", "root", ""},
        {"client3", "server3:19530", "root", ""},
    }

    for _, c := range clients {
        err := pool.Add(c.name,
            client.WithAddress(c.address),
            client.WithAuth(c.username, c.password),
        )
        if err != nil {
            log.Printf("添加客户端 %s 失败: %v", c.name, err)
        }
    }

    // 使用不同的客户端
    cli1, _ := pool.Get("client1")
    cli2, _ := pool.Get("client2")
    cli3, _ := pool.Get("client3")

    // 并行使用多个客户端
    // ...
}
```

### 4. 动态管理模式

```go
func main() {
    pool := milvus.NewPool()
    defer pool.Close()

    // 动态添加客户端
    for i := 0; i < 5; i++ {
        clientName := fmt.Sprintf("client_%d", i)
        err := pool.Add(clientName,
            client.WithAddress("localhost:19530"),
            client.WithAuth("root", ""),
        )
        if err != nil {
            log.Printf("添加客户端 %s 失败: %v", clientName, err)
        }
    }

    // 列出所有客户端
    clients := pool.List()
    log.Printf("当前有 %d 个客户端: %v", len(clients), clients)

    // 动态移除客户端
    for i := 0; i < 3; i++ {
        clientName := fmt.Sprintf("client_%d", i)
        err := pool.Remove(clientName)
        if err != nil {
            log.Printf("移除客户端 %s 失败: %v", clientName, err)
        }
    }

    // 检查剩余客户端
    clients = pool.List()
    log.Printf("移除后剩余 %d 个客户端: %v", len(clients), clients)
}
```

## 并发安全

连接池是线程安全的，可以在多个 goroutine 中并发使用：

```go
func main() {
    pool := milvus.NewPool()
    defer pool.Close()

    // 添加客户端
    pool.Add("main_client",
        client.WithAddress("localhost:19530"),
        client.WithAuth("root", ""),
    )

    // 并发使用
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            
            cli, err := pool.Get("main_client")
            if err != nil {
                log.Printf("Goroutine %d: 获取客户端失败: %v", id, err)
                return
            }
            
            // 使用客户端进行操作
            // ...
        }(i)
    }
    
    wg.Wait()
}
```

## 错误处理

连接池提供了详细的错误信息，建议进行适当的错误处理：

```go
// 添加客户端时的错误处理
err := pool.Add("client1", opts...)
if err != nil {
    if strings.Contains(err.Error(), "already exists") {
        log.Println("客户端已存在，跳过创建")
    } else {
        log.Fatalf("创建客户端失败: %v", err)
    }
}

// 获取客户端时的错误处理
cli, err := pool.Get("client1")
if err != nil {
    if strings.Contains(err.Error(), "not found") {
        log.Println("客户端不存在，尝试创建")
        cli, err = pool.MustGet("client1", opts...)
        if err != nil {
            log.Fatalf("创建客户端失败: %v", err)
        }
    } else {
        log.Fatalf("获取客户端失败: %v", err)
    }
}
```

## 最佳实践

### 1. 资源管理

```go
func main() {
    pool := milvus.NewPool()
    defer func() {
        if err := pool.Close(); err != nil {
            log.Printf("关闭连接池时出错: %v", err)
        }
    }()
    
    // 使用连接池
    // ...
}
```

### 2. 客户端命名

```go
// 使用有意义的客户端名称
pool.Add("main_database_client", opts...)
pool.Add("analytics_client", opts...)
pool.Add("backup_client", opts...)
```

### 3. 配置管理

```go
// 为不同用途的客户端使用不同的配置
pool.Add("read_client",
    client.WithAddress("read-server:19530"),
    client.WithRetry(3, 1*time.Second),
)

pool.Add("write_client",
    client.WithAddress("write-server:19530"),
    client.WithRetry(5, 2*time.Second),
)
```

### 4. 错误重试

```go
func getClientWithRetry(pool milvus.Pool, name string, maxRetries int) (client.Client, error) {
    for i := 0; i < maxRetries; i++ {
        cli, err := pool.Get(name)
        if err == nil {
            return cli, nil
        }
        
        if i < maxRetries-1 {
            time.Sleep(time.Duration(i+1) * time.Second)
        }
    }
    return nil, fmt.Errorf("获取客户端失败，已重试 %d 次", maxRetries)
}
```

## 性能考虑

1. **连接复用**：连接池自动管理连接，避免频繁创建和销毁
2. **并发访问**：支持多个 goroutine 并发访问
3. **内存管理**：及时移除不需要的客户端
4. **错误处理**：避免因单个客户端错误影响整个应用

## 相关文档

- [客户端API文档](./client/README.md)
- [示例程序](../../bin/README.md)
- [主项目README](../../README.md)
- [Milvus官方文档](https://milvus.io/docs)
