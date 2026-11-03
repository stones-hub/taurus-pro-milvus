package client

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// 本文件的测试全部使用门禁单元测试或本机 fake gRPC，不依赖真实 Milvus。

// ---- gate 单元测试 ----

func TestGateReadersShareWriterExcludes(t *testing.T) {
	var g gate
	ctx := context.Background()
	require.NoError(t, g.acquire(ctx, false))
	require.NoError(t, g.acquire(ctx, false))

	wctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, g.acquire(wctx, true), context.DeadlineExceeded)

	g.release(false)
	g.release(false)
	require.NoError(t, g.acquire(ctx, true))
	rctx, cancel2 := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel2()
	assert.ErrorIs(t, g.acquire(rctx, false), context.DeadlineExceeded)
	g.release(true)
}

func TestGateCancelledContextDoesNotAcquire(t *testing.T) {
	var g gate
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, g.acquire(ctx, false), context.Canceled)
	assert.ErrorIs(t, g.acquire(ctx, true), context.Canceled)
	// 空闲门禁未被占用
	require.NoError(t, g.acquire(context.Background(), true))
	g.release(true)
}

// 写者排队后，新来的读者必须排在写者之后（写不被读饿死）
func TestGateWriterNotStarvedByReaders(t *testing.T) {
	var g gate
	ctx := context.Background()
	require.NoError(t, g.acquire(ctx, false))

	var order []string
	var omu sync.Mutex
	rec := func(s string) { omu.Lock(); order = append(order, s); omu.Unlock() }

	wDone := make(chan struct{})
	go func() {
		require.NoError(t, g.acquire(ctx, true))
		rec("w")
		g.release(true)
		close(wDone)
	}()
	require.Eventually(t, func() bool { g.mu.Lock(); defer g.mu.Unlock(); return len(g.queue) == 1 }, time.Second, time.Millisecond)

	rDone := make(chan struct{})
	go func() {
		require.NoError(t, g.acquire(ctx, false))
		rec("r")
		g.release(false)
		close(rDone)
	}()
	require.Eventually(t, func() bool { g.mu.Lock(); defer g.mu.Unlock(); return len(g.queue) == 2 }, time.Second, time.Millisecond)

	g.release(false)
	<-wDone
	<-rDone
	assert.Equal(t, []string{"w", "r"}, order)
}

// 队首写者取消后，其后的读者应立即获得（读者仍被先前的读者而非写者阻塞）
func TestGateCancelHeadWakesFollowers(t *testing.T) {
	var g gate
	require.NoError(t, g.acquire(context.Background(), false))

	wctx, wcancel := context.WithCancel(context.Background())
	werr := make(chan error, 1)
	go func() { werr <- g.acquire(wctx, true) }()
	require.Eventually(t, func() bool { g.mu.Lock(); defer g.mu.Unlock(); return len(g.queue) == 1 }, time.Second, time.Millisecond)

	rerr := make(chan error, 1)
	go func() { rerr <- g.acquire(context.Background(), false) }()
	require.Eventually(t, func() bool { g.mu.Lock(); defer g.mu.Unlock(); return len(g.queue) == 2 }, time.Second, time.Millisecond)

	wcancel()
	assert.ErrorIs(t, <-werr, context.Canceled)
	select {
	case err := <-rerr:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("写者取消后读者未被唤醒")
	}
	g.release(false)
	g.release(false)
}

func TestGateCloseWakesWaitersAndWaitsForHolders(t *testing.T) {
	var g gate
	require.NoError(t, g.acquire(context.Background(), true))

	waiterErr := make(chan error, 1)
	go func() { waiterErr <- g.acquire(context.Background(), false) }()
	require.Eventually(t, func() bool { g.mu.Lock(); defer g.mu.Unlock(); return len(g.queue) == 1 }, time.Second, time.Millisecond)

	var fnCalls atomic.Int32
	closeDone := make(chan error, 1)
	go func() { closeDone <- g.close(func() error { fnCalls.Add(1); return nil }) }()

	// 等待者被 Close 唤醒为 closed；写者仍在途，Close 未完成
	select {
	case err := <-waiterErr:
		assert.ErrorIs(t, err, errClientClosed)
	case <-time.After(time.Second):
		t.Fatal("等待者未被 Close 唤醒")
	}
	select {
	case <-closeDone:
		t.Fatal("在途操作未结束时 Close 不应完成")
	case <-time.After(50 * time.Millisecond):
	}
	// closing 后的新请求立即 closed，即便已有 ctx 期限
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	assert.ErrorIs(t, g.acquire(ctx, false), errClientClosed)
	assert.Less(t, time.Since(start), 100*time.Millisecond)
	assert.True(t, g.isClosing())

	// 并发的重复 Close 等待首个完成
	dup := make(chan error, 1)
	go func() { dup <- g.close(func() error { fnCalls.Add(1); return errors.New("不应调用") }) }()
	select {
	case <-dup:
		t.Fatal("重复 Close 不应先于首个 Close 完成")
	case <-time.After(50 * time.Millisecond):
	}

	g.release(true)
	require.NoError(t, <-closeDone)
	require.NoError(t, <-dup)
	assert.EqualValues(t, 1, fnCalls.Load())
	// 之后的重复 Close 仍只返回 nil
	require.NoError(t, g.close(func() error { fnCalls.Add(1); return nil }))
	assert.EqualValues(t, 1, fnCalls.Load())
}

// 高并发多轮：取消的等待者不泄漏 goroutine，写者能前进，门禁最终归零
func TestGateStressNoLeakNoStarvation(t *testing.T) {
	var g gate
	before := runtime.NumGoroutine()
	var writes, reads atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				write := (i+j)%8 == 0
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(j%5+1)*time.Millisecond)
				if g.acquire(ctx, write) == nil {
					if write {
						writes.Add(1)
					} else {
						reads.Add(1)
					}
					time.Sleep(50 * time.Microsecond)
					g.release(write)
				}
				cancel()
			}
		}(i)
	}
	wg.Wait()
	assert.Greater(t, writes.Load(), int64(0), "写者不应被读者饿死")
	assert.Greater(t, reads.Load(), int64(0))

	g.mu.Lock()
	assert.Zero(t, g.readers)
	assert.False(t, g.writer)
	assert.Empty(t, g.queue)
	g.mu.Unlock()
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= before+2 }, 2*time.Second, 10*time.Millisecond)
}

// ---- fake gRPC 场景 ----

type opCounts struct {
	has, load, query, search, connect, progress atomic.Int32
}

type lockFake struct {
	milvuspb.UnimplementedMilvusServiceServer
	n         opCounts
	loadGate  chan struct{} // LoadCollection 阻塞到关闭
	loadEnter chan struct{} // LoadCollection 进入后通知
	pending   atomic.Bool   // true 时加载进度恒为 0（Await 不结束）
	hasGate   chan struct{} // 非 nil 时 HasCollection 阻塞到关闭
	hasEnter  chan struct{}
	dbs       []string // 每次 Connect 看到的 db 头
	dbMu      sync.Mutex
	hasDBs    []string // 每次 HasCollection 看到的 db 头
}

func header(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("dbname"); len(v) > 0 {
			return v[0]
		}
	}
	return ""
}

func (f *lockFake) Connect(ctx context.Context, _ *milvuspb.ConnectRequest) (*milvuspb.ConnectResponse, error) {
	f.n.connect.Add(1)
	db := header(ctx)
	f.dbMu.Lock()
	f.dbs = append(f.dbs, db)
	f.dbMu.Unlock()
	if db == "slow" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if db == "bad" {
		return nil, status.Error(codes.PermissionDenied, "no such database")
	}
	return &milvuspb.ConnectResponse{Status: &commonpb.Status{}, Identifier: 1}, nil
}

func (f *lockFake) LoadCollection(ctx context.Context, _ *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	f.n.load.Add(1)
	if f.loadEnter != nil {
		select {
		case f.loadEnter <- struct{}{}:
		default:
		}
	}
	select {
	case <-f.loadGate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &commonpb.Status{}, nil
}

func (f *lockFake) GetLoadingProgress(context.Context, *milvuspb.GetLoadingProgressRequest) (*milvuspb.GetLoadingProgressResponse, error) {
	f.n.progress.Add(1)
	if f.pending.Load() {
		return &milvuspb.GetLoadingProgressResponse{Status: &commonpb.Status{}, Progress: 0}, nil
	}
	return &milvuspb.GetLoadingProgressResponse{Status: &commonpb.Status{}, Progress: 100}, nil
}

// SDK 的 HasCollection / Query 等均通过 DescribeCollection 探测集合，这里把它作为受控的"读 RPC"
func (f *lockFake) DescribeCollection(ctx context.Context, _ *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	f.n.has.Add(1)
	f.dbMu.Lock()
	f.hasDBs = append(f.hasDBs, header(ctx))
	f.dbMu.Unlock()
	if f.hasGate != nil {
		if f.hasEnter != nil {
			select {
			case f.hasEnter <- struct{}{}:
			default:
			}
		}
		select {
		case <-f.hasGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &milvuspb.DescribeCollectionResponse{Status: &commonpb.Status{}, CollectionName: "c", Schema: &schemapb.CollectionSchema{Name: "c"}}, nil
}

func (f *lockFake) Query(context.Context, *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
	f.n.query.Add(1)
	return &milvuspb.QueryResults{Status: &commonpb.Status{}}, nil
}

func newLockClient(t *testing.T, f *lockFake, opts ...Option) *client {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	milvuspb.RegisterMilvusServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := NewWithOptions(ctx, append([]Option{WithAddress(lis.Addr().String())}, opts...)...)
	require.NoError(t, err)
	c := cli.(*client)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func shortCtx(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// 长写 RPC 占用期间，短 deadline / 主动取消的读与写在预算内返回 ctx 错误，且不产生任何服务端 RPC
func TestShortDeadlineNotBlockedByLongWrite(t *testing.T) {
	f := &lockFake{loadGate: make(chan struct{}), loadEnter: make(chan struct{}, 1)}
	c := newLockClient(t, f)

	loadDone := make(chan error, 1)
	go func() { loadDone <- c.LoadCollection(context.Background(), "c") }()
	<-f.loadEnter

	hasBefore, queryBefore, loadBefore := f.n.has.Load(), f.n.query.Load(), f.n.load.Load()
	ops := map[string]func(ctx context.Context) error{
		"has":   func(ctx context.Context) error { _, err := c.HasCollection(ctx, "c"); return err },
		"query": func(ctx context.Context) error { _, err := c.Query(ctx, "c", nil, "id>0", nil); return err },
		"write": func(ctx context.Context) error { return c.ReleaseCollection(ctx, "c") },
		"usedb": func(ctx context.Context) error { return c.UseDatabase(ctx, "other") },
	}
	for name, op := range ops {
		t.Run(name+"/deadline", func(t *testing.T) {
			ctx, cancel := shortCtx(100 * time.Millisecond)
			defer cancel()
			start := time.Now()
			err := op(ctx)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
			assert.Less(t, time.Since(start), 400*time.Millisecond)
		})
		t.Run(name+"/cancel", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(50*time.Millisecond, cancel)
			start := time.Now()
			assert.ErrorIs(t, op(ctx), context.Canceled)
			assert.Less(t, time.Since(start), 400*time.Millisecond)
		})
	}
	assert.Equal(t, hasBefore, f.n.has.Load(), "被取消的请求不得发出 RPC")
	assert.Equal(t, queryBefore, f.n.query.Load())
	assert.Equal(t, loadBefore, f.n.load.Load())
	assert.EqualValues(t, 1, f.n.connect.Load(), "取消的 UseDatabase 不得重连")

	close(f.loadGate)
	require.NoError(t, <-loadDone)
	// 取消后数据库未变化，客户端仍正常
	ok, err := c.HasCollection(context.Background(), "c")
	require.NoError(t, err)
	assert.True(t, ok)
	f.dbMu.Lock()
	for _, db := range f.hasDBs {
		assert.Equal(t, "default", db)
	}
	f.dbMu.Unlock()
}

// 已取消的 ctx 不发 RPC
func TestCancelledContextSendsNoRPC(t *testing.T) {
	f := &lockFake{}
	c := newLockClient(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.HasCollection(ctx, "c")
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, c.DropCollection(ctx, "c"), context.Canceled)
	assert.Zero(t, f.n.has.Load())
}

// 阻塞的读 RPC 期间，写等待可被取消；其他读并发不受影响
func TestWriteWaitCancellableBehindRead(t *testing.T) {
	f := &lockFake{hasGate: make(chan struct{}), hasEnter: make(chan struct{}, 4)}
	c := newLockClient(t, f)

	readDone := make(chan error, 2)
	go func() { _, err := c.HasCollection(context.Background(), "c"); readDone <- err }()
	<-f.hasEnter
	// 第二个读与第一个并发进入服务端
	go func() { _, err := c.HasCollection(context.Background(), "c"); readDone <- err }()
	select {
	case <-f.hasEnter:
	case <-time.After(2 * time.Second):
		t.Fatal("读操作应并发")
	}

	wctx, cancel := shortCtx(100 * time.Millisecond)
	defer cancel()
	start := time.Now()
	assert.ErrorIs(t, c.LoadCollection(wctx, "c"), context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 400*time.Millisecond)
	assert.Zero(t, f.n.load.Load())

	close(f.hasGate)
	require.NoError(t, <-readDone)
	require.NoError(t, <-readDone)
}

// Close 等待在途长操作期间：新请求立即 closed，GetClient 为 nil；释放后 Close 完成，重复 Close 安全
func TestCloseWaitsForInflightAndRejectsNew(t *testing.T) {
	f := &lockFake{loadGate: make(chan struct{}), loadEnter: make(chan struct{}, 1)}
	c := newLockClient(t, f)
	require.NotNil(t, c.GetClient())

	loadDone := make(chan error, 1)
	go func() { loadDone <- c.LoadCollection(context.Background(), "c") }()
	<-f.loadEnter

	// 在 Close 前排队的等待者也要被唤醒为 closed
	waiterErr := make(chan error, 1)
	go func() { _, err := c.HasCollection(context.Background(), "c"); waiterErr <- err }()
	require.Eventually(t, func() bool { c.gate.mu.Lock(); defer c.gate.mu.Unlock(); return len(c.gate.queue) == 1 }, time.Second, time.Millisecond)

	closeDone := make(chan error, 2)
	go func() { closeDone <- c.Close() }()
	go func() { closeDone <- c.Close() }()

	select {
	case err := <-waiterErr:
		assert.EqualError(t, err, "client is closed")
	case <-time.After(time.Second):
		t.Fatal("排队的等待者未被 Close 唤醒")
	}
	assert.Nil(t, c.GetClient())
	ctx, cancel := shortCtx(time.Second)
	defer cancel()
	start := time.Now()
	_, err := c.Query(ctx, "c", nil, "id>0", nil)
	assert.EqualError(t, err, "client is closed")
	assert.Less(t, time.Since(start), 200*time.Millisecond)
	select {
	case <-closeDone:
		t.Fatal("在途 LoadCollection 未结束，Close 不应返回")
	case <-time.After(100 * time.Millisecond):
	}

	close(f.loadGate)
	require.NoError(t, <-loadDone)
	require.NoError(t, <-closeDone)
	require.NoError(t, <-closeDone)
	require.NoError(t, c.Close())
	assert.Zero(t, f.n.query.Load())
}

// UseDatabase 与操作互斥，正常切换后后续请求带新库头；取消不改变库
func TestUseDatabaseExclusiveAndSwitches(t *testing.T) {
	f := &lockFake{hasGate: make(chan struct{}), hasEnter: make(chan struct{}, 1)}
	c := newLockClient(t, f)

	readDone := make(chan error, 1)
	go func() { _, err := c.HasCollection(context.Background(), "c"); readDone <- err }()
	<-f.hasEnter

	// 读在途时 UseDatabase 必须等待（不交错）
	useDone := make(chan error, 1)
	go func() { useDone <- c.UseDatabase(context.Background(), "db2") }()
	select {
	case <-useDone:
		t.Fatal("UseDatabase 不应与在途读交错")
	case <-time.After(100 * time.Millisecond):
	}
	assert.EqualValues(t, 1, f.n.connect.Load())

	close(f.hasGate)
	require.NoError(t, <-readDone)
	require.NoError(t, <-useDone)

	f.hasGate = nil
	_, err := c.HasCollection(context.Background(), "c")
	require.NoError(t, err)
	f.dbMu.Lock()
	defer f.dbMu.Unlock()
	require.Len(t, f.hasDBs, 2)
	assert.Equal(t, "default", f.hasDBs[0], "切换前的在途请求使用原库")
	assert.Equal(t, "db2", f.hasDBs[1])
	assert.Equal(t, "db2", f.dbs[len(f.dbs)-1])
	assert.Equal(t, "db2", c.db)
}

// UseDatabase 重连失败时 SDK 内部库名被回滚（SDK 自身先改写库名且不回滚），后续请求仍使用原库
func TestUseDatabaseFailureRollsBack(t *testing.T) {
	f := &lockFake{}
	c := newLockClient(t, f, WithDatabase("db1"))

	assert.Error(t, c.UseDatabase(context.Background(), "bad"))
	assert.Equal(t, "db1", c.db)

	_, err := c.HasCollection(context.Background(), "c")
	require.NoError(t, err)
	f.dbMu.Lock()
	defer f.dbMu.Unlock()
	assert.Equal(t, "db1", f.hasDBs[len(f.hasDBs)-1])
	assert.EqualValues(t, 2, f.n.connect.Load(), "回滚不得再发 Connect")
}

// 取消的等待者不泄漏 goroutine，高并发多轮后客户端仍可用
func TestClientCancelledWaitersNoLeak(t *testing.T) {
	f := &lockFake{loadGate: make(chan struct{}), loadEnter: make(chan struct{}, 1)}
	c := newLockClient(t, f)
	loadDone := make(chan error, 1)
	go func() { loadDone <- c.LoadCollection(context.Background(), "c") }()
	<-f.loadEnter

	before := runtime.NumGoroutine()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := shortCtx(time.Duration(i%10+1) * time.Millisecond)
			defer cancel()
			if i%2 == 0 {
				_, _ = c.HasCollection(ctx, "c")
			} else {
				_ = c.ReleaseCollection(ctx, "c")
			}
		}(i)
	}
	wg.Wait()
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= before+2 }, 2*time.Second, 10*time.Millisecond)

	close(f.loadGate)
	require.NoError(t, <-loadDone)
	_, err := c.HasCollection(context.Background(), "c")
	require.NoError(t, err)
}

// ---- 库名来源 / Close 防御 / Await 阶段 / 错误契约 ----

// newLockClientAt 与 newLockClient 相同，但地址带 URL 路径（http://host:port/<path>）
func newLockClientAt(t *testing.T, f *lockFake, path string, opts ...Option) *client {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	milvuspb.RegisterMilvusServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	ctx, cancel := shortCtx(5 * time.Second)
	defer cancel()
	cli, err := NewWithOptions(ctx, append([]Option{WithAddress("http://" + lis.Addr().String() + "/" + path)}, opts...)...)
	require.NoError(t, err)
	c := cli.(*client)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func (f *lockFake) lastDescribeDB() string {
	f.dbMu.Lock()
	defer f.dbMu.Unlock()
	return f.hasDBs[len(f.hasDBs)-1]
}

// URL 路径中的库名在 WithDatabase("") 时生效；UseDatabase 失败后必须回到 URL 库而不是 default
func TestUseDatabaseFailureRollsBackToURLDatabase(t *testing.T) {
	f := &lockFake{}
	c := newLockClientAt(t, f, "urldb", WithDatabase(""))
	assert.Equal(t, "urldb", c.db)

	_, err := c.HasCollection(context.Background(), "c")
	require.NoError(t, err)
	assert.Equal(t, "urldb", f.lastDescribeDB(), "初始请求应带 URL 库")
	f.dbMu.Lock()
	assert.Equal(t, "urldb", f.dbs[0], "握手应带 URL 库")
	f.dbMu.Unlock()

	assert.Error(t, c.UseDatabase(context.Background(), "bad"))
	assert.Equal(t, "urldb", c.db)
	_, err = c.HasCollection(context.Background(), "c")
	require.NoError(t, err)
	assert.Equal(t, "urldb", f.lastDescribeDB(), "失败后不得退回 default")

	require.NoError(t, c.UseDatabase(context.Background(), "db2"))
	_, err = c.HasCollection(context.Background(), "c")
	require.NoError(t, err)
	assert.Equal(t, "db2", f.lastDescribeDB())
}

// 显式库优先于 URL 路径
func TestExplicitDatabaseWinsOverURL(t *testing.T) {
	f := &lockFake{}
	c := newLockClientAt(t, f, "urldb", WithDatabase("explicit"))
	assert.Equal(t, "explicit", c.db)
	_, err := c.HasCollection(context.Background(), "c")
	require.NoError(t, err)
	assert.Equal(t, "explicit", f.lastDescribeDB())

	assert.Error(t, c.UseDatabase(context.Background(), "bad"))
	_, err = c.HasCollection(context.Background(), "c")
	require.NoError(t, err)
	assert.Equal(t, "explicit", f.lastDescribeDB())
}

// SDK 为 nil 的 client（防御）Close 不 panic；重复 Close 不卡死
func TestCloseNilSDKAndRepeatedClose(t *testing.T) {
	c := &client{}
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
	assert.Nil(t, c.GetClient())
}

// 关闭函数 panic 时 panic 继续传播，但并发的重复 Close 不得永久阻塞
func TestGateClosePanicStillNotifiesDuplicates(t *testing.T) {
	var g gate
	require.NoError(t, g.acquire(context.Background(), false))

	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		_ = g.close(func() error { panic("boom") })
	}()
	require.Eventually(t, func() bool { return g.isClosing() }, time.Second, time.Millisecond)

	dup := make(chan error, 1)
	go func() { dup <- g.close(func() error { return nil }) }()
	time.Sleep(30 * time.Millisecond)
	g.release(false)

	assert.Equal(t, "boom", <-panicked)
	select {
	case err := <-dup:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("重复 Close 在首个 Close panic 后被永久阻塞")
	}
	require.NoError(t, g.close(func() error { return nil }))
}

// 零值 client 拒绝 NaN：具体错误、发 RPC 之前，且门禁已释放
func TestSearchNaNErrorReleasesGate(t *testing.T) {
	c := &client{}
	_, err := c.Search(context.Background(), "coll", nil, nil, []entity.Vector{entity.FloatVector([]float32{1})},
		"vector", entity.L2, 1, "", map[string]string{"radius": "NaN"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `invalid search param "radius"`)
	assert.Contains(t, err.Error(), "non-finite")

	ctx, cancel := shortCtx(200 * time.Millisecond)
	defer cancel()
	require.NoError(t, c.gate.acquire(ctx, true), "校验失败后门禁必须已释放")
	c.gate.release(true)
}

// Await 阶段超时：Load RPC 已成功、进度轮询一直未完成，ctx 到期后返回错误并释放门禁
func TestAwaitTimeoutReleasesGate(t *testing.T) {
	f := &lockFake{loadGate: make(chan struct{})}
	close(f.loadGate)
	f.pending.Store(true)
	c := newLockClient(t, f)

	ctx, cancel := shortCtx(500 * time.Millisecond)
	defer cancel()
	start := time.Now()
	err := c.LoadCollection(ctx, "c")
	require.Error(t, err)
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.EqualValues(t, 1, f.n.load.Load())
	assert.Greater(t, f.n.progress.Load(), int32(0), "应处于 Await 轮询阶段")

	// 门禁已释放：写门禁立即可得，后续读成功
	wctx, wcancel := shortCtx(200 * time.Millisecond)
	defer wcancel()
	require.NoError(t, c.gate.acquire(wctx, true))
	c.gate.release(true)
	ok, err := c.HasCollection(context.Background(), "c")
	require.NoError(t, err)
	assert.True(t, ok)

	// 同一任务在进度完成后可正常 Await 成功（任务状态可观察）
	f.pending.Store(false)
	require.NoError(t, c.LoadCollection(context.Background(), "c"))
}

// 两阶段错误契约：等待门禁用 context 错误；已发出的 RPC 沿 SDK/gRPC 错误（status.Code 判断）
func TestTimeoutErrorContractPerPhase(t *testing.T) {
	f := &lockFake{loadGate: make(chan struct{}), loadEnter: make(chan struct{}, 1)}
	c := newLockClient(t, f)
	loadDone := make(chan error, 1)
	go func() { loadDone <- c.LoadCollection(context.Background(), "c") }()
	<-f.loadEnter

	// 阶段一：等待门禁
	ctx, cancel := shortCtx(100 * time.Millisecond)
	err := c.UseDatabase(ctx, "slow")
	cancel()
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.EqualValues(t, 1, f.n.connect.Load(), "等待阶段超时不得发 RPC")

	close(f.loadGate)
	require.NoError(t, <-loadDone)

	// 阶段二：已发出的 RPC（Connect 阻塞到客户端超时）——返回 gRPC status 错误
	ctx2, cancel2 := shortCtx(200 * time.Millisecond)
	defer cancel2()
	err = c.UseDatabase(ctx2, "slow")
	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err), "RPC 阶段沿 gRPC 错误，用 status.Code 判断")
	assert.Equal(t, "default", c.db, "超时的切换必须回滚")
	_, err = c.HasCollection(context.Background(), "c")
	require.NoError(t, err)
	assert.Equal(t, "default", f.lastDescribeDB())
}

// cancel 与 grant 竞争：无论哪一方先发生，门禁最终必归零且不泄漏
func TestGateCancelGrantRaceInvariant(t *testing.T) {
	var g gate
	cancelledAfterGrant := 0
	for i := 0; i < 300; i++ {
		require.NoError(t, g.acquire(context.Background(), true))
		ctx, cancel := context.WithCancel(context.Background())
		res := make(chan error, 1)
		go func() { res <- g.acquire(ctx, true) }()
		require.Eventually(t, func() bool { g.mu.Lock(); defer g.mu.Unlock(); return len(g.queue) == 1 }, time.Second, 50*time.Microsecond)

		// 持有 mu 时先取消、再模拟 release 的授予：等待者醒来后只能看到"已授予且 ctx 已取消"
		g.mu.Lock()
		cancel()
		g.writer = false
		g.dispatch()
		g.mu.Unlock()

		err := <-res
		if err != nil {
			cancelledAfterGrant++
			assert.ErrorIs(t, err, context.Canceled)
		} else {
			g.release(true) // select 选中了授予分支：合法地获得了门禁
		}
		g.mu.Lock()
		assert.False(t, g.writer, "iter %d: 门禁泄漏", i)
		assert.Zero(t, g.readers)
		assert.Empty(t, g.queue)
		g.mu.Unlock()
	}
	assert.Greater(t, cancelledAfterGrant, 0, "应覆盖到授予后发现取消的路径")
}
