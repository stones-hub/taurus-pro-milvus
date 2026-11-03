package milvus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/stones-hub/taurus-pro-milvus/pkg/milvus/client"
)

// DefaultCreateTimeout 是创建单个客户端（建立连接）的默认超时时间
const DefaultCreateTimeout = 10 * time.Second

// ErrPoolClosed 表示连接池已关闭。Close 是终态，关闭后的池不会被重新启用。
var ErrPoolClosed = errors.New("pool is closed")

// Pool 是 Milvus 客户端连接池，用于管理多个客户端实例。
// 它是一个按名称注册、复用客户端的注册表，不做请求级负载均衡。
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

// ContextPool 在 Pool 基础上提供可由调用方 context 控制的创建入口。
// 单独定义为新接口，避免扩展 Pool 破坏第三方实现；NewContextPool 返回的实现同时满足 Pool。
type ContextPool interface {
	Pool

	// AddContext 同 Add，ctx 取消或超时后立即返回 ctx.Err()
	AddContext(ctx context.Context, name string, opts ...client.Option) error

	// MustGetContext 同 MustGet，ctx 取消或超时后立即返回 ctx.Err()
	MustGetContext(ctx context.Context, name string, opts ...client.Option) (client.Client, error)
}

// PoolOption 是连接池配置选项
type PoolOption func(*pool)

// WithCreateTimeout 设置创建单个客户端的超时时间，d<=0 时使用 DefaultCreateTimeout。
// 调用方传入的 context 更短时以调用方为准。
func WithCreateTimeout(d time.Duration) PoolOption {
	return func(p *pool) {
		if d > 0 {
			p.createTimeout = d
		}
	}
}

// pending 表示一次进行中的客户端创建，同名并发请求共享同一次创建结果
type pending struct {
	cancel  context.CancelFunc
	done    chan struct{} // finished 后关闭
	waiters int           // 仍在等待的调用方数量，受 pool.mu 保护
	reason  error         // 创建失去归属（Close/Remove/无人等待）的原因，受 pool.mu 保护

	// 以下字段在 done 关闭前由创建 goroutine 写入，关闭后只读
	cli client.Client
	err error
}

// pool 实现 Pool 和 ContextPool 接口
type pool struct {
	mu      sync.RWMutex
	clients map[string]client.Client
	pending map[string]*pending
	closed  bool

	baseCtx    context.Context // Close 时取消，终止所有进行中的创建
	baseCancel context.CancelFunc

	createTimeout time.Duration
	// newClient 创建客户端，测试中可替换
	newClient func(ctx context.Context, opts ...client.Option) (client.Client, error)
}

// NewPool 创建一个新的 Milvus 客户端连接池，创建客户端使用 DefaultCreateTimeout
func NewPool() Pool {
	return newPool()
}

// NewContextPool 创建支持 context 的连接池，可通过 PoolOption 配置创建超时
func NewContextPool(opts ...PoolOption) ContextPool {
	return newPool(opts...)
}

func newPool(opts ...PoolOption) *pool {
	p := &pool{
		clients:       make(map[string]client.Client),
		pending:       make(map[string]*pending),
		createTimeout: DefaultCreateTimeout,
		newClient:     client.NewWithOptions,
	}
	p.baseCtx, p.baseCancel = context.WithCancel(context.Background())
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Get 获取指定名称的客户端。不会被其他名称正在进行的连接创建阻塞。
func (p *pool) Get(name string) (client.Client, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, ErrPoolClosed
	}
	if cli, ok := p.clients[name]; ok {
		return cli, nil
	}
	return nil, fmt.Errorf("client %s not found", name)
}

// MustGet 获取指定名称的客户端，如果不存在则创建新的客户端。
// 同名并发调用共享同一次创建结果（含失败），后到调用方的 opts 被忽略。
func (p *pool) MustGet(name string, opts ...client.Option) (client.Client, error) {
	return p.MustGetContext(context.Background(), name, opts...)
}

// MustGetContext 同 MustGet，ctx 控制等待
func (p *pool) MustGetContext(ctx context.Context, name string, opts ...client.Option) (client.Client, error) {
	return p.acquire(ctx, name, true, opts)
}

// Add 添加一个新的客户端，名称已存在（含正在创建）时返回错误
func (p *pool) Add(name string, opts ...client.Option) error {
	return p.AddContext(context.Background(), name, opts...)
}

// AddContext 同 Add，ctx 控制等待
func (p *pool) AddContext(ctx context.Context, name string, opts ...client.Option) error {
	_, err := p.acquire(ctx, name, false, opts)
	return err
}

// acquire 返回已有客户端（shared 为 true）、加入进行中的创建（shared 为 true），或发起新的创建
func (p *pool) acquire(ctx context.Context, name string, shared bool, opts []client.Option) (client.Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	if cli, ok := p.clients[name]; ok {
		p.mu.Unlock()
		if shared {
			return cli, nil
		}
		return nil, fmt.Errorf("client %s already exists", name)
	}

	e, ok := p.pending[name]
	if ok && !shared {
		p.mu.Unlock()
		return nil, fmt.Errorf("client %s already exists", name)
	}
	if !ok {
		createCtx, cancel := context.WithTimeout(p.baseCtx, p.createTimeout)
		e = &pending{cancel: cancel, done: make(chan struct{})}
		p.pending[name] = e
		go p.create(createCtx, name, e, opts)
	}
	e.waiters++
	p.mu.Unlock()

	return p.wait(ctx, name, e)
}

// create 在锁外建立连接，完成后在锁内决定发布还是丢弃
func (p *pool) create(ctx context.Context, name string, e *pending, opts []client.Option) {
	cli, err := p.newClient(ctx, opts...)
	if err != nil {
		err = errors.Wrap(err, "failed to create new client")
	}

	p.mu.Lock()
	switch {
	case p.pending[name] == e && err == nil:
		// 仍拥有该名称：发布
		delete(p.pending, name)
		p.clients[name] = cli
		e.cli = cli
	case p.pending[name] == e:
		delete(p.pending, name)
		e.err = err
	default:
		// 已被 Close/Remove/无人等待取消，失去归属
		e.err = e.reason
		if e.err == nil {
			e.err = errors.New("client creation canceled")
		}
	}
	e.cancel()
	close(e.done)
	p.mu.Unlock()

	if e.err != nil && cli != nil && err == nil {
		_ = cli.Close() // 失去归属的客户端，避免泄漏
	}
}

// wait 等待创建完成。调用方取消时退出等待；最后一个等待者退出会取消创建。
func (p *pool) wait(ctx context.Context, name string, e *pending) (client.Client, error) {
	select {
	case <-e.done:
		return e.cli, e.err
	case <-ctx.Done():
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-e.done:
		return e.cli, e.err
	default:
	}
	e.waiters--
	if e.waiters == 0 {
		// 无人关心此次创建：取消并释放名称，创建 goroutine 会清理已建好的连接
		e.reason = ctx.Err()
		e.cancel()
		if p.pending[name] == e {
			delete(p.pending, name)
		}
	}
	return nil, ctx.Err()
}

// Remove 移除一个客户端；若该名称正在创建则取消创建
func (p *pool) Remove(name string) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPoolClosed
	}
	if e, ok := p.pending[name]; ok {
		e.reason = fmt.Errorf("client %s removed during creation", name)
		e.cancel()
		delete(p.pending, name)
		p.mu.Unlock()
		return nil
	}
	cli, ok := p.clients[name]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("client %s not found", name)
	}
	delete(p.clients, name)
	p.mu.Unlock()

	// 关闭可能等待客户端内部锁，放在池锁之外
	if err := cli.Close(); err != nil {
		return errors.Wrap(err, "failed to close client")
	}
	return nil
}

// Has 检查是否存在指定名称的客户端（不含正在创建的）
func (p *pool) Has(name string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	_, exists := p.clients[name]
	return exists
}

// List 列出所有已添加的客户端名称
func (p *pool) List() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	names := make([]string, 0, len(p.clients))
	for name := range p.clients {
		names = append(names, name)
	}
	return names
}

// Close 关闭连接池：取消进行中的创建，关闭所有客户端。
// Close 是终态，之后 Add/MustGet 返回 ErrPoolClosed；重复调用返回 nil。
func (p *pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.baseCancel()
	for name, e := range p.pending {
		e.reason = ErrPoolClosed
		e.cancel()
		delete(p.pending, name)
	}
	clients := p.clients
	p.clients = make(map[string]client.Client)
	p.mu.Unlock()

	var errs []error
	for name, cli := range clients {
		if err := cli.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close client %s: %v", name, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to close some clients: %v", errs)
	}
	return nil
}
