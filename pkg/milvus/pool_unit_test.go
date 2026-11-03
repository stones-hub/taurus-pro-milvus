package milvus

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stones-hub/taurus-pro-milvus/pkg/milvus/client"
)

// 本文件的测试不依赖 Milvus 服务器，通过注入 newClient 工厂验证并发与生命周期语义。

// fakeClient 嵌入 nil 接口，仅实现池用到的 Close
type fakeClient struct {
	client.Client
	closed    atomic.Bool
	closeHold chan struct{} // 非 nil 时 Close 阻塞直到关闭
}

func (f *fakeClient) Close() error {
	if f.closeHold != nil {
		<-f.closeHold
	}
	f.closed.Store(true)
	return nil
}

// factory 记录调用并按 behave 决定行为
type factory struct {
	calls   atomic.Int32
	behave  func(ctx context.Context, name string) (client.Client, error)
	created chan *fakeClient
}

func newTestPool(t *testing.T, f *factory, opts ...PoolOption) *pool {
	t.Helper()
	p := newPool(opts...)
	p.newClient = func(ctx context.Context, opts ...client.Option) (client.Client, error) {
		o := client.DefaultOptions()
		for _, opt := range opts {
			opt(o)
		}
		f.calls.Add(1)
		return f.behave(ctx, o.Address)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func okBehave(f *factory) func(context.Context, string) (client.Client, error) {
	return func(ctx context.Context, name string) (client.Client, error) {
		c := &fakeClient{}
		if f.created != nil {
			f.created <- c
		}
		return c, nil
	}
}

// blockBehave 阻塞到 ctx 结束
func blockBehave(ctx context.Context, name string) (client.Client, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	require.Eventually(t, cond, 2*time.Second, 5*time.Millisecond)
}

func TestPoolMustGetConcurrentSameName(t *testing.T) {
	f := &factory{}
	release := make(chan struct{})
	f.behave = func(ctx context.Context, name string) (client.Client, error) {
		<-release // 保证所有 goroutine 都已进入
		return &fakeClient{}, nil
	}
	p := newTestPool(t, f)

	const n = 50
	results := make([]client.Client, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = p.MustGet("same", client.WithAddress("same"))
		}(i)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		assert.Same(t, results[0], results[i])
	}
	assert.EqualValues(t, 1, f.calls.Load())
	assert.Equal(t, []string{"same"}, p.List())
}

func TestPoolAddDuplicate(t *testing.T) {
	f := &factory{}
	f.behave = okBehave(f)
	p := newTestPool(t, f)

	require.NoError(t, p.Add("a"))
	err := p.Add("a")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
	assert.EqualValues(t, 1, f.calls.Load())

	// 正在创建中的名称同样视为已存在
	started := make(chan struct{})
	f.behave = func(ctx context.Context, name string) (client.Client, error) {
		close(started)
		return blockBehave(ctx, name)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = p.AddContext(ctx, "b") }()
	<-started
	err = p.Add("b")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestPoolMustGetSharedFailure(t *testing.T) {
	f := &factory{}
	release := make(chan struct{})
	f.behave = func(ctx context.Context, name string) (client.Client, error) {
		<-release
		return nil, errors.New("boom")
	}
	p := newTestPool(t, f)

	const n = 10
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = p.MustGet("bad")
		}(i)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	assert.EqualValues(t, 1, f.calls.Load())
	for _, err := range errs {
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create new client")
	}
	assert.False(t, p.Has("bad"))

	// 失败后名称释放，可重试
	f.behave = okBehave(f)
	_, err := p.MustGet("bad")
	require.NoError(t, err)
	assert.True(t, p.Has("bad"))
}

func TestPoolStuckCreationDoesNotBlockOthers(t *testing.T) {
	f := &factory{}
	started := make(chan struct{})
	f.behave = func(ctx context.Context, name string) (client.Client, error) {
		if name == "stuck" {
			close(started)
			return blockBehave(ctx, name)
		}
		return &fakeClient{}, nil
	}
	p := newTestPool(t, f)

	require.NoError(t, p.Add("good", client.WithAddress("good")))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _ = p.MustGetContext(ctx, "stuck", client.WithAddress("stuck")) }()
	<-started

	done := make(chan struct{})
	go func() {
		defer close(done)
		cli, err := p.Get("good")
		assert.NoError(t, err)
		assert.NotNil(t, cli)
		assert.True(t, p.Has("good"))
		assert.Equal(t, []string{"good"}, p.List())
		_, err = p.MustGet("good")
		assert.NoError(t, err)
		assert.NoError(t, p.Add("other", client.WithAddress("other")))
		assert.NoError(t, p.Remove("other"))
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("池操作被其他名称的创建阻塞")
	}

	// Close 也不被阻塞，且取消卡住的创建
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	select {
	case err := <-closed:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Close 被进行中的创建阻塞")
	}
}

func TestPoolCreateTimeout(t *testing.T) {
	f := &factory{behave: blockBehave}
	p := newTestPool(t, f, WithCreateTimeout(50*time.Millisecond))

	start := time.Now()
	_, err := p.MustGet("slow")
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.False(t, p.Has("slow"))

	assert.Equal(t, DefaultCreateTimeout, newPool().createTimeout)
	assert.Equal(t, DefaultCreateTimeout, newPool(WithCreateTimeout(0)).createTimeout)
}

func TestPoolCallerContextShorterThanTimeout(t *testing.T) {
	f := &factory{}
	ctxSeen := make(chan context.Context, 1)
	f.behave = func(ctx context.Context, name string) (client.Client, error) {
		ctxSeen <- ctx
		return blockBehave(ctx, name)
	}
	p := newTestPool(t, f) // 默认 10s 超时

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := p.MustGetContext(ctx, "x")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 2*time.Second)

	// 唯一等待者离开后创建被取消，名称释放
	createCtx := <-ctxSeen
	eventually(t, func() bool { return createCtx.Err() != nil })

	f.behave = okBehave(f)
	_, err = p.MustGet("x")
	assert.NoError(t, err)

	// 已取消的 ctx 不发起创建
	cancelled, cancel2 := context.WithCancel(context.Background())
	cancel2()
	calls := f.calls.Load()
	_, err = p.MustGetContext(cancelled, "y")
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, calls, f.calls.Load())
}

func TestPoolWaitersCancelIndependently(t *testing.T) {
	f := &factory{}
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	f.behave = func(ctx context.Context, name string) (client.Client, error) {
		started <- struct{}{}
		select {
		case <-release:
			return &fakeClient{}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	p := newTestPool(t, f)

	ctx1, cancel1 := context.WithCancel(context.Background())
	res1 := make(chan error, 1)
	go func() { _, err := p.MustGetContext(ctx1, "shared"); res1 <- err }()
	<-started

	res2 := make(chan error, 1)
	go func() { _, err := p.MustGet("shared"); res2 <- err }()
	eventually(t, func() bool {
		p.mu.RLock()
		defer p.mu.RUnlock()
		e := p.pending["shared"]
		return e != nil && e.waiters == 2
	})

	cancel1()
	assert.ErrorIs(t, <-res1, context.Canceled)

	// 第二个等待者仍在，创建不应被取消
	select {
	case err := <-res2:
		t.Fatalf("第二个等待者不应结束: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-res2)
	assert.True(t, p.Has("shared"))
	assert.EqualValues(t, 1, f.calls.Load())
}

func TestPoolCloseDuringCreation(t *testing.T) {
	f := &factory{created: make(chan *fakeClient, 1)}
	started := make(chan struct{})
	proceed := make(chan struct{})
	f.behave = func(ctx context.Context, name string) (client.Client, error) {
		close(started)
		<-proceed // 即使被取消，也模拟创建仍然成功返回
		c := &fakeClient{}
		f.created <- c
		return c, nil
	}
	p := newTestPool(t, f)

	res := make(chan error, 1)
	go func() { _, err := p.MustGet("late"); res <- err }()
	<-started

	require.NoError(t, p.Close())
	close(proceed)

	assert.ErrorIs(t, <-res, ErrPoolClosed)
	c := <-f.created
	eventually(t, func() bool { return c.closed.Load() }) // 失去归属的客户端必须关闭
	assert.False(t, p.Has("late"))
	assert.Empty(t, p.List())
}

func TestPoolRemoveDuringCreation(t *testing.T) {
	f := &factory{created: make(chan *fakeClient, 1)}
	started := make(chan struct{})
	proceed := make(chan struct{})
	f.behave = func(ctx context.Context, name string) (client.Client, error) {
		close(started)
		<-proceed
		c := &fakeClient{}
		f.created <- c
		return c, nil
	}
	p := newTestPool(t, f)

	res := make(chan error, 1)
	go func() { _, err := p.MustGet("rm"); res <- err }()
	<-started

	require.NoError(t, p.Remove("rm"))
	close(proceed)

	require.Error(t, <-res)
	c := <-f.created
	eventually(t, func() bool { return c.closed.Load() })
	assert.False(t, p.Has("rm"))

	// 名称已释放，可重新创建
	f.behave = okBehave(&factory{})
	_, err := p.MustGet("rm")
	assert.NoError(t, err)
}

func TestPoolClosedIsTerminal(t *testing.T) {
	f := &factory{}
	f.behave = okBehave(f)
	p := newTestPool(t, f)

	cli, err := p.MustGet("a")
	require.NoError(t, err)
	require.NoError(t, p.Close())
	assert.True(t, cli.(*fakeClient).closed.Load())
	assert.NoError(t, p.Close(), "重复 Close 应幂等")

	calls := f.calls.Load()
	assert.ErrorIs(t, p.Add("b"), ErrPoolClosed)
	_, err = p.MustGet("b")
	assert.ErrorIs(t, err, ErrPoolClosed)
	_, err = p.MustGet("a")
	assert.ErrorIs(t, err, ErrPoolClosed)
	_, err = p.Get("a")
	assert.ErrorIs(t, err, ErrPoolClosed)
	assert.ErrorIs(t, p.Remove("a"), ErrPoolClosed)
	assert.False(t, p.Has("a"))
	assert.Empty(t, p.List())
	assert.Equal(t, calls, f.calls.Load(), "关闭后不得再创建客户端")
}

func TestPoolRemoveClosesOutsideLock(t *testing.T) {
	f := &factory{}
	hold := make(chan struct{})
	f.behave = func(ctx context.Context, name string) (client.Client, error) {
		if name == "slowclose" {
			return &fakeClient{closeHold: hold}, nil
		}
		return &fakeClient{}, nil
	}
	p := newTestPool(t, f)
	require.NoError(t, p.Add("slowclose", client.WithAddress("slowclose")))
	require.NoError(t, p.Add("fast", client.WithAddress("fast")))

	removed := make(chan error, 1)
	go func() { removed <- p.Remove("slowclose") }()
	eventually(t, func() bool { return !p.Has("slowclose") })

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := p.Get("fast")
		assert.NoError(t, err)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Remove 的 Close 持有池锁")
	}
	close(hold)
	assert.NoError(t, <-removed)
}

func TestPoolImplementsContextPool(t *testing.T) {
	var _ Pool = NewPool()
	var _ ContextPool = NewContextPool()
}
