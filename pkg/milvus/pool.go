package milvus

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"

	"github.com/stones-hub/taurus-pro-milvus/pkg/milvus/client"
)

// Pool 是 Milvus 客户端连接池，用于管理多个客户端实例
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

// pool 实现 Pool 接口
type pool struct {
	clients map[string]client.Client
	mu      sync.RWMutex
}

// NewPool 创建一个新的 Milvus 客户端连接池
func NewPool() Pool {
	return &pool{
		clients: make(map[string]client.Client),
	}
}

// Get 获取指定名称的客户端
func (p *pool) Get(name string) (client.Client, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if cli, ok := p.clients[name]; ok {
		return cli, nil
	}
	return nil, fmt.Errorf("client %s not found", name)
}

// MustGet 获取指定名称的客户端，如果不存在则创建新的客户端
func (p *pool) MustGet(name string, opts ...client.Option) (client.Client, error) {
	// 先尝试获取已存在的客户端
	if cli, err := p.Get(name); err == nil {
		return cli, nil
	}

	// 如果不存在，则创建新的客户端
	return p.add(name, opts...)
}

// Add 添加一个新的客户端
func (p *pool) Add(name string, opts ...client.Option) error {
	_, err := p.add(name, opts...)
	return err
}

// add 内部方法，添加一个新的客户端
func (p *pool) add(name string, opts ...client.Option) (client.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 检查客户端是否已存在
	if _, exists := p.clients[name]; exists {
		return nil, fmt.Errorf("client %s already exists", name)
	}

	// 创建新的客户端
	cli, err := client.NewWithOptions(context.Background(), opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new client")
	}

	// 添加客户端到连接池
	p.clients[name] = cli
	return cli, nil
}

// Remove 移除一个客户端
func (p *pool) Remove(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if cli, ok := p.clients[name]; ok {
		// 关闭客户端连接
		if err := cli.Close(); err != nil {
			return errors.Wrap(err, "failed to close client")
		}
		// 从连接池中移除客户端
		delete(p.clients, name)
		return nil
	}
	return fmt.Errorf("client %s not found", name)
}

// Has 检查是否存在指定名称的客户端
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

// Close 关闭所有客户端连接
func (p *pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error
	for name, cli := range p.clients {
		if err := cli.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close client %s: %v", name, err))
		}
	}

	// 清空客户端映射
	p.clients = make(map[string]client.Client)

	if len(errs) > 0 {
		return fmt.Errorf("failed to close some clients: %v", errs)
	}
	return nil
}
