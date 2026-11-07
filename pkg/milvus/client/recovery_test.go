package client

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// 本文件使用本机 fake gRPC 服务器与真实 SDK 客户端验证只读恢复，不依赖 Milvus。

// chanNotServiceable 复现真实重启后首次检索的服务端错误（merr 503）
func chanNotServiceable() error {
	return merr.WrapErrChannelNotAvailable("by-dev-rootcoord-dml_0_1v0", "channel distribution is not serviceable")
}

// tsafeStalled 复现真实 docker stop/start 后首次检索的服务端错误（merr 505）
func tsafeStalled() error {
	return merr.WrapErrChannelTSafeStalled("by-dev-rootcoord-dml_10_1v1", "fail to search on QueryNode 10: lag(2h20m49.177s) max(3s)")
}

// recFake 的行为函数参数 n 为该方法第几次被调用（从 1 计）；返回 gRPC 状态错误时按传输层返回，其余按 Milvus 业务状态返回
type recFake struct {
	milvuspb.UnimplementedMilvusServiceServer
	connect      func(ctx context.Context, n int32) error
	search       func(ctx context.Context, n int32) error
	searchResult func(n int32) *schemapb.SearchResultData // nil 时返回 okSearchData
	query        func(ctx context.Context, n int32) error
	loadState    func(n int32) commonpb.LoadState // nil 时恒为 Loaded
	loadStateErr func(n int32) error              // 非 nil 且返回非 nil 时 GetLoadState 失败
	slowGate     chan struct{}                    // 非 nil 时 Describe("slow") 阻塞到关闭
	slowEnter    chan struct{}
	deleteErr    error // nil 时 Delete 返回 chanNotServiceable

	nConnect, nSearch, nQuery, nLoadState, nLoad, nDelete atomic.Int32

	mu        sync.Mutex
	connDBs   []string
	connAuth  []string
	searchDBs []string
}

func reply(err error) (*commonpb.Status, error) {
	if err == nil {
		return &commonpb.Status{}, nil
	}
	if _, ok := status.FromError(err); ok {
		return nil, err
	}
	return merr.Status(err), nil
}

func (f *recFake) Connect(ctx context.Context, _ *milvuspb.ConnectRequest) (*milvuspb.ConnectResponse, error) {
	n := f.nConnect.Add(1)
	auth := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("authorization"); len(v) > 0 {
			auth = v[0]
		}
	}
	f.mu.Lock()
	f.connDBs = append(f.connDBs, header(ctx))
	f.connAuth = append(f.connAuth, auth)
	f.mu.Unlock()
	var err error
	if f.connect != nil {
		err = f.connect(ctx, n)
	}
	st, rpcErr := reply(err)
	if rpcErr != nil {
		return nil, rpcErr
	}
	return &milvuspb.ConnectResponse{Status: st, Identifier: int64(n)}, nil
}

func (f *recFake) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	if req.GetCollectionName() == "slow" && f.slowGate != nil {
		f.slowEnter <- struct{}{}
		select {
		case <-f.slowGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &milvuspb.DescribeCollectionResponse{
		Status:         &commonpb.Status{},
		CollectionName: req.GetCollectionName(),
		Schema: &schemapb.CollectionSchema{
			Name: req.GetCollectionName(),
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
			},
		},
	}, nil
}

func okSearchData() *schemapb.SearchResultData {
	return &schemapb.SearchResultData{
		NumQueries: 1, TopK: 1, Topks: []int64{1}, Scores: []float32{0},
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{42}}}},
	}
}

func (f *recFake) Search(ctx context.Context, _ *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	n := f.nSearch.Add(1)
	f.mu.Lock()
	f.searchDBs = append(f.searchDBs, header(ctx))
	f.mu.Unlock()
	var err error
	if f.search != nil {
		err = f.search(ctx, n)
	}
	st, rpcErr := reply(err)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if err != nil {
		return &milvuspb.SearchResults{Status: st}, nil
	}
	data := okSearchData()
	if f.searchResult != nil {
		data = f.searchResult(n)
	}
	return &milvuspb.SearchResults{Status: st, Results: data}, nil
}

func (f *recFake) Query(ctx context.Context, _ *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
	n := f.nQuery.Add(1)
	var err error
	if f.query != nil {
		err = f.query(ctx, n)
	}
	st, rpcErr := reply(err)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if err != nil {
		return &milvuspb.QueryResults{Status: st}, nil
	}
	return &milvuspb.QueryResults{
		Status:       st,
		OutputFields: []string{"id"},
		FieldsData: []*schemapb.FieldData{{
			Type: schemapb.DataType_Int64, FieldName: "id", FieldId: 100,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{42}}}}},
		}},
	}, nil
}

func (f *recFake) GetLoadState(_ context.Context, _ *milvuspb.GetLoadStateRequest) (*milvuspb.GetLoadStateResponse, error) {
	n := f.nLoadState.Add(1)
	if f.loadStateErr != nil {
		if err := f.loadStateErr(n); err != nil {
			st, rpcErr := reply(err)
			if rpcErr != nil {
				return nil, rpcErr
			}
			return &milvuspb.GetLoadStateResponse{Status: st}, nil
		}
	}
	state := commonpb.LoadState_LoadStateLoaded
	if f.loadState != nil {
		state = f.loadState(n)
	}
	return &milvuspb.GetLoadStateResponse{Status: &commonpb.Status{}, State: state}, nil
}

func (f *recFake) GetLoadingProgress(context.Context, *milvuspb.GetLoadingProgressRequest) (*milvuspb.GetLoadingProgressResponse, error) {
	return &milvuspb.GetLoadingProgressResponse{Status: &commonpb.Status{}, Progress: 50}, nil
}

func (f *recFake) LoadCollection(context.Context, *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	f.nLoad.Add(1)
	return &commonpb.Status{}, nil
}

func (f *recFake) Delete(context.Context, *milvuspb.DeleteRequest) (*milvuspb.MutationResult, error) {
	f.nDelete.Add(1)
	err := chanNotServiceable()
	if f.deleteErr != nil {
		err = f.deleteErr
	}
	return &milvuspb.MutationResult{Status: merr.Status(err)}, nil
}

// newRecClient 启动 fake 并创建启用只读恢复的客户端（opts 可覆盖），恢复间隔缩短以便测试
func newRecClient(t *testing.T, f *recFake, opts ...Option) (*client, *trackedListener) {
	t.Helper()
	return newRecClientAt(t, f, "", opts...)
}

// newRecClientAt 同 newRecClient；path 非空时地址带 URL 路径（http://host:port/<path>）
func newRecClientAt(t *testing.T, f *recFake, path string, opts ...Option) (*client, *trackedListener) {
	t.Helper()
	addr, tl := startRecFake(t, f, path)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := NewWithOptions(ctx, append([]Option{WithAddress(addr), WithReadRecovery(true)}, opts...)...)
	require.NoError(t, err)
	c := cli.(*client)
	shortenRecovery(c)
	t.Cleanup(func() { _ = c.Close() })
	return c, tl
}

// startRecFake 启动 fake 并返回客户端地址；path 非空时地址带 URL 路径
func startRecFake(t *testing.T, f *recFake, path string) (string, *trackedListener) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tl := &trackedListener{Listener: lis}
	srv := grpc.NewServer()
	milvuspb.RegisterMilvusServiceServer(srv, f)
	go func() { _ = srv.Serve(tl) }()
	t.Cleanup(srv.Stop)

	addr := lis.Addr().String()
	if path != "" {
		addr = "http://" + addr + "/" + path
	}
	return addr, tl
}

// shortenRecovery 缩短恢复间隔与加载轮询以便测试；未启用恢复时不做任何事
func shortenRecovery(c *client) {
	if c.rec != nil {
		c.rec.interval = 10 * time.Millisecond
		c.rec.loadPoll = 5 * time.Millisecond
	}
}

func searchVec(ctx context.Context, c *client, coll string) ([]milvusclient.ResultSet, error) {
	return c.Search(ctx, coll, nil, nil, []entity.Vector{entity.FloatVector([]float32{0, 0})}, "vec", entity.L2, 1, "", nil)
}

func failFirst(err error) func(context.Context, int32) error {
	return func(_ context.Context, n int32) error {
		if n == 1 {
			return err
		}
		return nil
	}
}

func always(err error) func(context.Context, int32) error {
	return func(context.Context, int32) error { return err }
}

func requireTopID42(t *testing.T, results []milvusclient.ResultSet) {
	t.Helper()
	require.Len(t, results, 1)
	require.Equal(t, 1, results[0].ResultCount)
	id, err := results[0].IDs.GetAsInt64(0)
	require.NoError(t, err)
	assert.EqualValues(t, 42, id)
}

// 事件收集器（观察回调可能并发）
type eventLog struct {
	mu  sync.Mutex
	evs []ReadRecoveryEvent
}

func (l *eventLog) add(ev ReadRecoveryEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.evs = append(l.evs, ev)
}

func (l *eventLog) all() []ReadRecoveryEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]ReadRecoveryEvent(nil), l.evs...)
}

// ---- 错误分类 ----

func TestIsRecoverableReadErr(t *testing.T) {
	// 服务端业务错误经 SDK 还原后的形态
	server := func(err error) error { return merr.Error(merr.Status(err)) }

	type tc struct {
		name string
		err  error
	}
	recoverable := []tc{
		{"grpc Unavailable", status.Error(codes.Unavailable, "connection refused")},
		{"包装后的 grpc Unavailable", errors.Wrap(status.Error(codes.Unavailable, "x"), "ctx")},
		{"merr 503 channel not available", server(chanNotServiceable())},
		{"merr 505 tsafe stalled", server(tsafeStalled())},
		{"merr 505 直接错误", tsafeStalled()},
		{"包装后的 merr 505", errors.Wrap(server(tsafeStalled()), "search")},
		{"merr 505 Status 未标 retriable 仍按码识别", merr.Error(&commonpb.Status{Code: 505, Reason: "lag"})},
		{"merr 1 service not ready", server(merr.WrapErrServiceNotReady("proxy", 1, "Initializing"))},
		{"merr 2 service unavailable", server(merr.WrapErrServiceUnavailable("starting"))},
		{"旧版 ErrorCode NotReadyServe", merr.Error(&commonpb.Status{ErrorCode: commonpb.ErrorCode_NotReadyServe, Reason: "not ready"})},
	}
	for _, c := range recoverable {
		assert.True(t, isRecoverableReadErr(c.err), c.name)
	}

	permanent := []tc{
		{"nil", nil},
		{"ctx canceled", context.Canceled},
		{"ctx deadline", errors.Wrap(context.DeadlineExceeded, "x")},
		{"grpc DeadlineExceeded", status.Error(codes.DeadlineExceeded, "x")},
		{"grpc Canceled", status.Error(codes.Canceled, "x")},
		{"grpc Unauthenticated", status.Error(codes.Unauthenticated, "x")},
		{"grpc PermissionDenied", status.Error(codes.PermissionDenied, "x")},
		{"grpc Unimplemented", status.Error(codes.Unimplemented, "x")},
		{"merr 未认证", server(merr.ErrPrivilegeNotAuthenticated)},
		{"merr 无权限", server(merr.ErrPrivilegeNotPermitted)},
		{"merr 参数错误", server(merr.ErrParameterInvalid)},
		{"merr 集合未加载", server(merr.ErrCollectionNotLoaded)},
		{"merr 集合不存在", server(merr.ErrCollectionNotFound)},
		{"merr 副本不可用(未经验证不纳入)", server(merr.ErrReplicaNotAvailable)},
		{"merr 限流", server(merr.ErrServiceRateLimit)},
		{"文本含 503 的未知错误码", merr.Error(&commonpb.Status{Code: 65535, Reason: "503 service unavailable"})},
		{"普通文本错误", errors.New("rpc error: 503 Service Unavailable")},
		{"merr 504 checkpoint 滞后(邻接码)", server(merr.WrapErrChannelCPExceededMaxLag("ch", "lag"))},
		{"merr 506 dropped sentinel(邻接码)", server(merr.WrapErrChannelDroppedSentinel("ch"))},
		{"merr 507 misrouted(同为 retryable 但未纳入)", server(merr.ErrChannelMisrouted)},
		{"标记 retriable 的未知错误码", merr.Error(&commonpb.Status{Code: 65535, Retriable: true, Reason: "x"})},
		{"文本含 tsafe stalled 的未知错误码", merr.Error(&commonpb.Status{Code: 65535, Reason: "channel tsafe stalled"})},
		{"文本含 tsafe stalled 的普通错误", errors.New("merr 505: channel tsafe stalled")},
		{"客户端已关闭", errClientClosed},
		{"恢复预算耗尽", ErrReadRecoveryExhausted},
		{"恢复期间切库", ErrReadRecoveryDatabaseChanged},
		{"恢复结果校验失败", errors.Wrap(errors.New("bad"), "recovered result failed validation")},
	}
	for _, c := range permanent {
		assert.False(t, isRecoverableReadErr(c.err), c.name)
	}
}

// ---- 默认行为 ----

// 默认启用；WithReadRecovery 与其他选项一样按传入顺序生效，后者覆盖前者
func TestReadRecoveryDefaultOptions(t *testing.T) {
	assert.True(t, DefaultOptions().ReadRecovery, "默认启用只读恢复")

	apply := func(opts ...Option) bool {
		o := DefaultOptions()
		for _, opt := range opts {
			opt(o)
		}
		return o.ReadRecovery
	}
	assert.True(t, apply(WithAddress("x"), WithAuth("u", "p"), WithReadRecoveryObserver(func(ReadRecoveryEvent) {})), "其他选项不改变默认启用")
	assert.False(t, apply(WithReadRecovery(false)), "显式 false 关闭")
	assert.False(t, apply(WithReadRecovery(false), WithAddress("x"), WithReadRecoveryObserver(func(ReadRecoveryEvent) {})), "其他选项不改变显式关闭")
	assert.True(t, apply(WithReadRecovery(false), WithReadRecovery(true)), "后者覆盖前者")
	assert.False(t, apply(WithReadRecovery(true), WithReadRecovery(false)), "后者覆盖前者")
}

// 默认创建路径（不传 WithReadRecovery）遇可恢复错误时确实重建底层客户端并重试成功
func TestReadRecoveryEnabledByDefault(t *testing.T) {
	t.Run("NewWithOptions未传WithReadRecovery", func(t *testing.T) {
		var log eventLog
		f := &recFake{search: failFirst(chanNotServiceable()), query: failFirst(tsafeStalled())}
		addr, tl := startRecFake(t, f, "")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cli, err := NewWithOptions(ctx, WithAddress(addr), WithReadRecoveryObserver(log.add))
		require.NoError(t, err)
		c := cli.(*client)
		t.Cleanup(func() { _ = c.Close() })
		require.NotNil(t, c.rec, "未传 WithReadRecovery 也应启用恢复")
		assert.Equal(t, ReadRecoveryInterval, c.rec.interval, "默认间隔")
		assert.Equal(t, ReadRecoveryBudget, c.rec.budget, "默认预算")
		shortenRecovery(c)
		old := c.GetClient()

		results, err := searchVec(context.Background(), c, "c")
		require.NoError(t, err)
		requireTopID42(t, results)
		assert.EqualValues(t, 2, f.nSearch.Load())
		assert.EqualValues(t, 2, f.nConnect.Load(), "Search 应重建一次")
		assert.Zero(t, f.nLoad.Load(), "恢复不得发起 Load")
		assert.NotSame(t, old, c.GetClient(), "底层客户端已替换")
		waitOpen(t, tl, 1)

		cols, err := c.Query(context.Background(), "c", nil, "id > 0", []string{"id"})
		require.NoError(t, err)
		require.Len(t, cols, 1)
		v, err := cols[0].GetAsInt64(0)
		require.NoError(t, err)
		assert.EqualValues(t, 42, v)
		assert.EqualValues(t, 2, f.nQuery.Load())
		assert.EqualValues(t, 3, f.nConnect.Load(), "Query 应再重建一次")
		assert.Zero(t, f.nLoad.Load())
		waitOpen(t, tl, 1)

		evs := log.all()
		require.Len(t, evs, 2)
		assert.Equal(t, "Search", evs[0].Op)
		assert.True(t, errors.Is(evs[0].FirstErr, merr.ErrChannelNotAvailable), "首次失败保留")
		assert.NoError(t, evs[0].Err)
		assert.Equal(t, 2, evs[0].Attempts)
		assert.Equal(t, "Query", evs[1].Op)
		assert.True(t, errors.Is(evs[1].FirstErr, merr.ErrChannelTSafeStalled), "首次失败保留")
		assert.NoError(t, evs[1].Err)
		assert.Equal(t, 2, evs[1].Attempts)
	})

	t.Run("New便利入口", func(t *testing.T) {
		f := &recFake{search: failFirst(merr.WrapErrServiceNotReady("proxy", 1, "Initializing"))}
		addr, tl := startRecFake(t, f, "")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cli, err := New(ctx, addr, "root", "pw")
		require.NoError(t, err)
		c := cli.(*client)
		t.Cleanup(func() { _ = c.Close() })
		require.NotNil(t, c.rec, "New 应默认启用恢复")
		shortenRecovery(c)
		old := c.GetClient()

		results, err := searchVec(context.Background(), c, "c")
		require.NoError(t, err)
		requireTopID42(t, results)
		assert.EqualValues(t, 2, f.nSearch.Load())
		assert.EqualValues(t, 2, f.nConnect.Load(), "应重建一次")
		assert.NotSame(t, old, c.GetClient(), "底层客户端已替换")
		waitOpen(t, tl, 1)

		f.mu.Lock()
		defer f.mu.Unlock()
		require.Len(t, f.connAuth, 2)
		assert.NotEmpty(t, f.connAuth[0])
		assert.Equal(t, f.connAuth[0], f.connAuth[1], "重建保留 New 传入的认证")
	})

	// 默认启用不放宽边界：不可恢复错误原样返回、写操作不重试
	t.Run("默认路径不可恢复错误与写操作不重试", func(t *testing.T) {
		f := &recFake{search: always(status.Error(codes.PermissionDenied, "denied")), deleteErr: tsafeStalled()}
		addr, _ := startRecFake(t, f, "")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cli, err := NewWithOptions(ctx, WithAddress(addr))
		require.NoError(t, err)
		c := cli.(*client)
		t.Cleanup(func() { _ = c.Close() })
		require.NotNil(t, c.rec)
		shortenRecovery(c)

		var rerr *ReadRecoveryError
		_, err = searchVec(context.Background(), c, "c")
		require.Error(t, err)
		assert.False(t, errors.As(err, &rerr), "不可恢复错误原样返回")
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		err = c.Delete(context.Background(), "c", "", "id > 0")
		require.Error(t, err)
		assert.True(t, errors.Is(err, merr.ErrChannelTSafeStalled))
		assert.False(t, errors.As(err, &rerr))

		assert.EqualValues(t, 1, f.nSearch.Load())
		assert.EqualValues(t, 1, f.nDelete.Load())
		assert.EqualValues(t, 1, f.nConnect.Load(), "不得重建")
		assert.Zero(t, f.nLoadState.Load())
	})
}

// 显式 WithReadRecovery(false) 关闭：Search/Query 单次执行并原样返回，不重建
func TestReadRecoveryDisabledExplicitly(t *testing.T) {
	o := DefaultOptions()
	WithReadRecovery(false)(o)
	assert.False(t, o.ReadRecovery)

	f := &recFake{search: failFirst(chanNotServiceable()), query: failFirst(chanNotServiceable())}
	c, _ := newRecClient(t, f, WithReadRecovery(false))
	require.Nil(t, c.rec)

	_, err := searchVec(context.Background(), c, "c")
	require.Error(t, err)
	var rerr *ReadRecoveryError
	assert.False(t, errors.As(err, &rerr), "未启用时原样返回 SDK 错误")
	assert.True(t, errors.Is(err, merr.ErrChannelNotAvailable))

	_, err = c.Query(context.Background(), "c", nil, "id > 0", []string{"id"})
	require.Error(t, err)
	assert.False(t, errors.As(err, &rerr))

	assert.EqualValues(t, 1, f.nSearch.Load())
	assert.EqualValues(t, 1, f.nQuery.Load())
	assert.EqualValues(t, 1, f.nConnect.Load(), "未启用时不得重建")
	assert.Zero(t, f.nLoadState.Load())

	// 后续调用照常
	results, err := searchVec(context.Background(), c, "c")
	require.NoError(t, err)
	requireTopID42(t, results)
}

// 505 只在启用时可恢复，未启用时仍单次执行并原样返回
func TestReadRecoveryDisabledTSafeStalledNotRetried(t *testing.T) {
	f := &recFake{search: always(tsafeStalled()), query: always(tsafeStalled())}
	c, _ := newRecClient(t, f, WithReadRecovery(false))
	require.Nil(t, c.rec)

	var rerr *ReadRecoveryError
	_, err := searchVec(context.Background(), c, "c")
	require.Error(t, err)
	assert.False(t, errors.As(err, &rerr))
	assert.True(t, errors.Is(err, merr.ErrChannelTSafeStalled))

	_, err = c.Query(context.Background(), "c", nil, "id > 0", []string{"id"})
	require.Error(t, err)
	assert.False(t, errors.As(err, &rerr))
	assert.True(t, errors.Is(err, merr.ErrChannelTSafeStalled))

	assert.EqualValues(t, 1, f.nSearch.Load())
	assert.EqualValues(t, 1, f.nQuery.Load())
	assert.EqualValues(t, 1, f.nConnect.Load(), "未启用时不得重建")
	assert.Zero(t, f.nLoadState.Load())
}

// ---- 恢复成功 ----

func TestReadRecoverySearchRecovers(t *testing.T) {
	var log eventLog
	f := &recFake{search: failFirst(chanNotServiceable())}
	c, tl := newRecClient(t, f, WithReadRecoveryObserver(log.add))
	old := c.GetClient()

	results, err := searchVec(context.Background(), c, "c")
	require.NoError(t, err)
	requireTopID42(t, results)

	assert.EqualValues(t, 2, f.nSearch.Load())
	assert.EqualValues(t, 2, f.nConnect.Load(), "应重建一次")
	assert.Zero(t, f.nLoad.Load(), "恢复不得发起 Load")
	assert.NotSame(t, old, c.GetClient(), "底层客户端已替换")
	waitOpen(t, tl, 1) // 旧连接已关闭

	// 内部重建不是对外终态 Close
	assert.False(t, c.gate.isClosing())
	ok, err := c.HasCollection(context.Background(), "c")
	require.NoError(t, err)
	assert.True(t, ok)

	evs := log.all()
	require.Len(t, evs, 1)
	assert.Equal(t, "Search", evs[0].Op)
	assert.Equal(t, "c", evs[0].Collection)
	assert.True(t, errors.Is(evs[0].FirstErr, merr.ErrChannelNotAvailable), "首次失败保留")
	assert.NoError(t, evs[0].Err)
	assert.Equal(t, 2, evs[0].Attempts)
}

// 真实 docker stop/start 后的 505 经 SDK 还原后进入恢复，Search/Query 重建后重试成功
func TestReadRecoveryTSafeStalledRecovers(t *testing.T) {
	t.Run("Search", func(t *testing.T) {
		var log eventLog
		f := &recFake{search: failFirst(tsafeStalled())}
		c, tl := newRecClient(t, f, WithReadRecoveryObserver(log.add))
		old := c.GetClient()

		results, err := searchVec(context.Background(), c, "c")
		require.NoError(t, err)
		requireTopID42(t, results)
		assert.EqualValues(t, 2, f.nSearch.Load())
		assert.EqualValues(t, 2, f.nConnect.Load(), "应重建一次")
		assert.Zero(t, f.nLoad.Load(), "恢复不得发起 Load")
		assert.NotSame(t, old, c.GetClient())
		waitOpen(t, tl, 1)

		evs := log.all()
		require.Len(t, evs, 1)
		assert.Equal(t, "Search", evs[0].Op)
		assert.True(t, errors.Is(evs[0].FirstErr, merr.ErrChannelTSafeStalled), "首次失败保留")
		assert.Contains(t, evs[0].FirstErr.Error(), "channel tsafe stalled")
		assert.NoError(t, evs[0].Err)
		assert.Equal(t, 2, evs[0].Attempts)
	})

	t.Run("Query", func(t *testing.T) {
		var log eventLog
		f := &recFake{query: failFirst(tsafeStalled())}
		c, tl := newRecClient(t, f, WithReadRecoveryObserver(log.add))

		cols, err := c.Query(context.Background(), "c", nil, "id > 0", []string{"id"})
		require.NoError(t, err)
		require.Len(t, cols, 1)
		v, err := cols[0].GetAsInt64(0)
		require.NoError(t, err)
		assert.EqualValues(t, 42, v)
		assert.EqualValues(t, 2, f.nQuery.Load())
		assert.EqualValues(t, 2, f.nConnect.Load(), "应重建一次")
		assert.Zero(t, f.nLoad.Load(), "恢复不得发起 Load")
		waitOpen(t, tl, 1)

		evs := log.all()
		require.Len(t, evs, 1)
		assert.Equal(t, "Query", evs[0].Op)
		assert.True(t, errors.Is(evs[0].FirstErr, merr.ErrChannelTSafeStalled), "首次失败保留")
		assert.NoError(t, evs[0].Err)
		assert.Equal(t, 2, evs[0].Attempts)
	})
}

func TestReadRecoveryQueryLoadStates(t *testing.T) {
	t.Run("Loading时等待完成且不发起Load", func(t *testing.T) {
		f := &recFake{
			query: failFirst(merr.WrapErrServiceNotReady("proxy", 1, "Initializing")),
			loadState: func(n int32) commonpb.LoadState {
				if n <= 3 {
					return commonpb.LoadState_LoadStateLoading
				}
				return commonpb.LoadState_LoadStateLoaded
			},
		}
		c, _ := newRecClient(t, f)
		cols, err := c.Query(context.Background(), "c", nil, "id > 0", []string{"id"})
		require.NoError(t, err)
		require.Len(t, cols, 1)
		v, err := cols[0].GetAsInt64(0)
		require.NoError(t, err)
		assert.EqualValues(t, 42, v)
		assert.EqualValues(t, 4, f.nLoadState.Load())
		assert.EqualValues(t, 2, f.nQuery.Load())
		assert.Zero(t, f.nLoad.Load())
	})

	t.Run("NotLoad时不加载并返回服务端错误", func(t *testing.T) {
		f := &recFake{
			query: func(_ context.Context, n int32) error {
				if n == 1 {
					return chanNotServiceable()
				}
				return merr.ErrCollectionNotLoaded
			},
			loadState: func(int32) commonpb.LoadState { return commonpb.LoadState_LoadStateNotLoad },
		}
		c, _ := newRecClient(t, f)
		_, err := c.Query(context.Background(), "c", nil, "id > 0", []string{"id"})
		var rerr *ReadRecoveryError
		require.True(t, errors.As(err, &rerr))
		assert.True(t, errors.Is(rerr.Last, merr.ErrCollectionNotLoaded))
		assert.True(t, errors.Is(rerr.First, merr.ErrChannelNotAvailable))
		assert.EqualValues(t, 2, f.nQuery.Load(), "不可恢复错误后停止")
		assert.Zero(t, f.nLoad.Load(), "不得改变调用方的加载意图")
	})
}

// ---- 不可恢复 ----

func TestReadRecoveryNonRecoverableReturnsImmediately(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"grpc权限", status.Error(codes.PermissionDenied, "denied")},
		{"merr无权限", merr.ErrPrivilegeNotPermitted},
		{"merr参数错误", merr.ErrParameterInvalid},
		{"文本含503的未知码", errors.New("503 service unavailable")},
		{"merr504邻接码", merr.WrapErrChannelCPExceededMaxLag("ch", "lag")},
		{"merr506邻接码", merr.WrapErrChannelDroppedSentinel("ch")},
		{"文本含tsafe stalled的未知码", errors.New("channel tsafe stalled")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &recFake{search: always(tc.err)}
			c, _ := newRecClient(t, f)
			_, err := searchVec(context.Background(), c, "c")
			require.Error(t, err)
			var rerr *ReadRecoveryError
			assert.False(t, errors.As(err, &rerr), "不可恢复错误原样返回")
			assert.EqualValues(t, 1, f.nSearch.Load())
			assert.EqualValues(t, 1, f.nConnect.Load())
		})
	}
}

// 写操作与 Search/Query 以外的读操作从不自动重试
func TestReadRecoveryNoWriteOrOtherRetry(t *testing.T) {
	f := &recFake{}
	c, _ := newRecClient(t, f)

	err := c.Delete(context.Background(), "c", "", "id > 0")
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrChannelNotAvailable))
	var rerr *ReadRecoveryError
	assert.False(t, errors.As(err, &rerr))
	assert.EqualValues(t, 1, f.nDelete.Load())
	assert.EqualValues(t, 1, f.nConnect.Load())
}

// 写操作遇 505 同样不重试
func TestReadRecoveryNoWriteRetryOnTSafeStalled(t *testing.T) {
	f := &recFake{deleteErr: tsafeStalled()}
	c, _ := newRecClient(t, f)

	err := c.Delete(context.Background(), "c", "", "id > 0")
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrChannelTSafeStalled))
	var rerr *ReadRecoveryError
	assert.False(t, errors.As(err, &rerr))
	assert.EqualValues(t, 1, f.nDelete.Load())
	assert.EqualValues(t, 1, f.nConnect.Load())
}

// ---- 预算 / ctx ----

func TestReadRecoveryBudgetExhausted(t *testing.T) {
	const password = "s3cr3t-pw"
	var log eventLog
	f := &recFake{search: always(chanNotServiceable())}
	c, tl := newRecClient(t, f, WithAuth("root", password), WithReadRecoveryObserver(log.add))
	c.rec.budget = 200 * time.Millisecond

	start := time.Now()
	_, err := searchVec(context.Background(), c, "c")
	elapsed := time.Since(start)

	var rerr *ReadRecoveryError
	require.True(t, errors.As(err, &rerr))
	assert.True(t, errors.Is(err, ErrReadRecoveryExhausted))
	assert.False(t, errors.Is(err, merr.ErrChannelNotAvailable), "首次失败不参与 Unwrap，避免误判可恢复")
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err), "预算耗尽映射为 DeadlineExceeded")
	assert.True(t, errors.Is(rerr.First, merr.ErrChannelNotAvailable))
	assert.GreaterOrEqual(t, rerr.Attempts, 2)
	assert.Less(t, elapsed, 2*time.Second)
	assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond)
	assert.Contains(t, err.Error(), "channel distribution is not serviceable")
	assert.NotContains(t, err.Error(), password)

	evs := log.all()
	require.Len(t, evs, 1)
	assert.Same(t, err, evs[0].Err)
	waitOpen(t, tl, 1) // 每代旧连接都已关闭
}

func TestReadRecoveryRespectsCallerContext(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		f := &recFake{search: always(chanNotServiceable())}
		c, _ := newRecClient(t, f)
		c.rec.interval = 10 * time.Second

		ctx, cancel := shortCtx(200 * time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := searchVec(ctx, c, "c")
		assert.Less(t, time.Since(start), time.Second)
		assert.True(t, errors.Is(err, context.DeadlineExceeded))
		var rerr *ReadRecoveryError
		require.True(t, errors.As(err, &rerr))
		assert.True(t, errors.Is(rerr.First, merr.ErrChannelNotAvailable))
		assert.EqualValues(t, 1, f.nConnect.Load())
	})

	t.Run("cancel", func(t *testing.T) {
		f := &recFake{search: always(chanNotServiceable())}
		c, _ := newRecClient(t, f)
		c.rec.interval = 10 * time.Second

		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(100*time.Millisecond, cancel)
		start := time.Now()
		_, err := searchVec(ctx, c, "c")
		assert.Less(t, time.Since(start), time.Second)
		assert.True(t, errors.Is(err, context.Canceled))
	})
}

// 重连不保证修好服务端 tSafe：持续 505 时在自调用开始计的预算内明确失败，更短 ctx 与 Close 照常截断
func TestReadRecoveryTSafeStalledPersistent(t *testing.T) {
	t.Run("预算耗尽", func(t *testing.T) {
		var log eventLog
		f := &recFake{search: always(tsafeStalled())}
		c, tl := newRecClient(t, f, WithReadRecoveryObserver(log.add))
		c.rec.budget = 200 * time.Millisecond

		start := time.Now()
		_, err := searchVec(context.Background(), c, "c")
		elapsed := time.Since(start)

		var rerr *ReadRecoveryError
		require.True(t, errors.As(err, &rerr))
		assert.True(t, errors.Is(err, ErrReadRecoveryExhausted))
		assert.False(t, errors.Is(err, merr.ErrChannelTSafeStalled), "首次失败不参与 Unwrap")
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
		assert.True(t, errors.Is(rerr.First, merr.ErrChannelTSafeStalled))
		assert.GreaterOrEqual(t, rerr.Attempts, 2)
		assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond)
		assert.Less(t, elapsed, 2*time.Second, "多轮重建不重置预算")
		assert.Contains(t, err.Error(), "channel tsafe stalled")

		evs := log.all()
		require.Len(t, evs, 1)
		assert.Same(t, err, evs[0].Err)
		assert.True(t, errors.Is(evs[0].FirstErr, merr.ErrChannelTSafeStalled))
		waitOpen(t, tl, 1)
	})

	t.Run("调用方ctx更短", func(t *testing.T) {
		f := &recFake{query: always(tsafeStalled())}
		c, _ := newRecClient(t, f)
		c.rec.interval = 10 * time.Second

		ctx, cancel := shortCtx(200 * time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := c.Query(ctx, "c", nil, "id > 0", []string{"id"})
		assert.Less(t, time.Since(start), time.Second)
		assert.True(t, errors.Is(err, context.DeadlineExceeded))
		var rerr *ReadRecoveryError
		require.True(t, errors.As(err, &rerr))
		assert.True(t, errors.Is(rerr.First, merr.ErrChannelTSafeStalled))
		assert.EqualValues(t, 1, f.nQuery.Load())
		assert.EqualValues(t, 1, f.nConnect.Load())
	})

	t.Run("等待间隔中Close", func(t *testing.T) {
		f := &recFake{search: always(tsafeStalled())}
		c, tl := newRecClient(t, f)
		c.rec.interval = 10 * time.Second

		done := make(chan error, 1)
		go func() { _, err := searchVec(context.Background(), c, "c"); done <- err }()
		require.Eventually(t, func() bool { return f.nSearch.Load() == 1 }, 2*time.Second, time.Millisecond)
		time.Sleep(50 * time.Millisecond)

		start := time.Now()
		require.NoError(t, c.Close())
		select {
		case err := <-done:
			assert.True(t, errors.Is(err, errClientClosed))
			var rerr *ReadRecoveryError
			require.True(t, errors.As(err, &rerr))
			assert.True(t, errors.Is(rerr.First, merr.ErrChannelTSafeStalled))
		case <-time.After(time.Second):
			t.Fatal("Close 未终止恢复")
		}
		assert.Less(t, time.Since(start), time.Second)
		assert.EqualValues(t, 1, f.nConnect.Load())
		waitOpen(t, tl, 0)
	})
}

// 重建握手受 ctx 限时，超时后握手连接被释放
func TestReadRecoveryHandshakeBoundedByContext(t *testing.T) {
	f := &recFake{
		search: failFirst(chanNotServiceable()),
		connect: func(ctx context.Context, n int32) error {
			if n == 1 {
				return nil
			}
			<-ctx.Done()
			return status.FromContextError(ctx.Err()).Err()
		},
	}
	c, tl := newRecClient(t, f)

	ctx, cancel := shortCtx(300 * time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := searchVec(ctx, c, "c")
	assert.Less(t, time.Since(start), 1500*time.Millisecond)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
	assert.EqualValues(t, 1, f.nSearch.Load())
	waitOpen(t, tl, 1)
}

// blockFirstSearch 让第 1 次 Search 在服务端阻塞到客户端取消，并在进入时通知
func blockFirstSearch(entered chan<- struct{}) func(context.Context, int32) error {
	return func(ctx context.Context, n int32) error {
		if n != 1 {
			return nil
		}
		if entered != nil {
			entered <- struct{}{}
		}
		<-ctx.Done()
		return status.FromContextError(ctx.Err()).Err()
	}
}

// 启用恢复时，总预算自调用开始即约束首次门禁等待与首次查询（调用方 ctx 为 Background 也不例外）
func TestReadRecoveryBudgetCoversFirstAttempt(t *testing.T) {
	t.Run("首次RPC阻塞", func(t *testing.T) {
		var log eventLog
		f := &recFake{search: blockFirstSearch(nil)}
		c, _ := newRecClient(t, f, WithReadRecoveryObserver(log.add))
		c.rec.budget = 200 * time.Millisecond

		start := time.Now()
		_, err := searchVec(context.Background(), c, "c")
		elapsed := time.Since(start)
		var rerr *ReadRecoveryError
		require.True(t, errors.As(err, &rerr))
		assert.True(t, errors.Is(err, ErrReadRecoveryExhausted))
		assert.Equal(t, 1, rerr.Attempts)
		assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond)
		assert.Less(t, elapsed, 1500*time.Millisecond)
		assert.EqualValues(t, 1, f.nSearch.Load())
		assert.EqualValues(t, 1, f.nConnect.Load(), "首次被截断不重建")
		require.Len(t, log.all(), 1)
	})

	t.Run("首次门禁等待", func(t *testing.T) {
		f := &recFake{}
		c, _ := newRecClient(t, f)
		c.rec.budget = 200 * time.Millisecond
		require.NoError(t, c.gate.acquire(context.Background(), true)) // 模拟长写占用门禁
		defer c.gate.release(true)

		start := time.Now()
		_, err := searchVec(context.Background(), c, "c")
		assert.Less(t, time.Since(start), 1500*time.Millisecond)
		assert.True(t, errors.Is(err, ErrReadRecoveryExhausted))
		assert.Zero(t, f.nSearch.Load())
	})

	t.Run("调用方ctx更短时原样返回", func(t *testing.T) {
		f := &recFake{search: blockFirstSearch(nil)}
		c, _ := newRecClient(t, f)
		ctx, cancel := shortCtx(150 * time.Millisecond)
		defer cancel()
		_, err := searchVec(ctx, c, "c")
		require.Error(t, err)
		var rerr *ReadRecoveryError
		assert.False(t, errors.As(err, &rerr), "调用方自身 ctx 到期与未启用时一致")
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	})
}

// 启用恢复时 Close 取消进行中的首次查询，及时返回且不泄漏连接
func TestReadRecoveryCloseCancelsFirstAttempt(t *testing.T) {
	entered := make(chan struct{}, 1)
	f := &recFake{search: blockFirstSearch(entered)}
	c, tl := newRecClient(t, f)

	done := make(chan error, 1)
	go func() { _, err := searchVec(context.Background(), c, "c"); done <- err }()
	<-entered

	closeDone := make(chan error, 1)
	go func() { closeDone <- c.Close() }()
	select {
	case err := <-done:
		assert.True(t, errors.Is(err, errClientClosed))
	case <-time.After(time.Second):
		t.Fatal("Close 未取消首次查询")
	}
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close 被首次查询阻塞")
	}
	assert.EqualValues(t, 1, f.nConnect.Load())
	waitOpen(t, tl, 0)
}

// 未启用恢复时保持原语义：Close 等待在途查询自然结束，不取消它
func TestReadRecoveryDisabledCloseWaitsFirstAttempt(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	f := &recFake{search: func(ctx context.Context, n int32) error {
		if n == 1 {
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return status.FromContextError(ctx.Err()).Err()
			}
		}
		return nil
	}}
	c, _ := newRecClient(t, f, WithReadRecovery(false))

	done := make(chan error, 1)
	go func() { _, err := searchVec(context.Background(), c, "c"); done <- err }()
	<-entered

	closeDone := make(chan error, 1)
	go func() { closeDone <- c.Close() }()
	select {
	case <-closeDone:
		t.Fatal("未启用恢复时 Close 不应先于在途查询完成")
	case err := <-done:
		t.Fatalf("未启用恢复时 Close 不应取消在途查询: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-done)
	require.NoError(t, <-closeDone)
}

// ---- 并发与代际 ----

// 同代并发失败只重建一次
func TestReadRecoveryConcurrentSingleRebuild(t *testing.T) {
	const n = 16
	arrived := make(chan struct{})
	f := &recFake{}
	f.search = func(ctx context.Context, i int32) error {
		if i > n {
			return nil
		}
		if i == n {
			close(arrived)
		}
		// 所有首次请求同时在旧代上在途，再一起失败
		select {
		case <-arrived:
		case <-ctx.Done():
			return ctx.Err()
		}
		return chanNotServiceable()
	}
	var log eventLog
	c, tl := newRecClient(t, f, WithReadRecoveryObserver(log.add))
	var dials atomic.Int32
	dial := c.rec.dial
	c.rec.dial = func(ctx context.Context, o *Options) (*milvusclient.Client, string, error) {
		dials.Add(1)
		return dial(ctx, o)
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := shortCtx(10 * time.Second)
			defer cancel()
			var results []milvusclient.ResultSet
			results, errs[i] = searchVec(ctx, c, "c")
			if errs[i] == nil && len(results) != 1 {
				errs[i] = errors.New("bad results")
			}
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
	assert.EqualValues(t, 1, dials.Load(), "同代故障只重建一次")
	assert.EqualValues(t, 2, f.nConnect.Load())
	assert.EqualValues(t, 2*n, f.nSearch.Load())
	assert.Len(t, log.all(), n)
	waitOpen(t, tl, 1)
}

// 替换等待旧代在途请求结束后才关闭旧客户端
func TestReadRecoveryProtectsInflightOldGeneration(t *testing.T) {
	f := &recFake{search: failFirst(chanNotServiceable()), slowGate: make(chan struct{}), slowEnter: make(chan struct{}, 1)}
	c, tl := newRecClient(t, f)
	old := c.GetClient()

	aDone := make(chan error, 1)
	go func() {
		ok, err := c.HasCollection(context.Background(), "slow")
		if err == nil && !ok {
			err = errors.New("collection should exist")
		}
		aDone <- err
	}()
	<-f.slowEnter

	bDone := make(chan error, 1)
	go func() { _, err := searchVec(context.Background(), c, "c"); bDone <- err }()

	// 新客户端已握手，但替换须等待旧代在途请求
	require.Eventually(t, func() bool { return f.nConnect.Load() == 2 }, 2*time.Second, time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Same(t, old, c.GetClient())
	select {
	case err := <-bDone:
		t.Fatalf("在途旧代请求结束前不应完成替换: %v", err)
	case err := <-aDone:
		t.Fatalf("在途请求不应被打断: %v", err)
	default:
	}

	close(f.slowGate)
	require.NoError(t, <-aDone, "旧代在途请求正常完成")
	require.NoError(t, <-bDone)
	assert.NotSame(t, old, c.GetClient())
	waitOpen(t, tl, 1)
}

// 恢复期间 UseDatabase 切库：放弃新客户端、保留调用方的切换，并停止恢复，绝不改查其他库
func TestReadRecoveryStopsOnDatabaseSwitch(t *testing.T) {
	t.Run("重建握手中切库", func(t *testing.T) {
		f := &recFake{search: failFirst(chanNotServiceable())}
		c, tl := newRecClient(t, f)
		old := c.GetClient()

		entered := make(chan struct{})
		release := make(chan struct{})
		dial := c.rec.dial
		c.rec.dial = func(ctx context.Context, o *Options) (*milvusclient.Client, string, error) {
			close(entered)
			<-release
			return dial(ctx, o)
		}

		done := make(chan error, 1)
		go func() { _, err := searchVec(context.Background(), c, "c"); done <- err }()
		<-entered
		require.NoError(t, c.UseDatabase(context.Background(), "db2"))
		close(release)

		err := <-done
		var rerr *ReadRecoveryError
		require.True(t, errors.As(err, &rerr))
		assert.True(t, errors.Is(rerr.Last, ErrReadRecoveryDatabaseChanged))
		assert.True(t, errors.Is(rerr.First, merr.ErrChannelNotAvailable))
		assert.Same(t, old, c.GetClient(), "切库后不得用旧库名的新客户端覆盖")
		assert.Equal(t, "db2", c.db)
		assert.EqualValues(t, 3, f.nConnect.Load()) // 初始 + UseDatabase + 被放弃的重建
		assert.Zero(t, f.nLoadState.Load(), "不得查询其他库的加载状态")
		f.mu.Lock()
		assert.Equal(t, []string{"default"}, f.searchDBs, "只在原库查询过一次")
		f.mu.Unlock()
		waitOpen(t, tl, 1)
	})

	t.Run("等待加载中切库", func(t *testing.T) {
		loading := make(chan struct{}, 1)
		f := &recFake{
			search: failFirst(chanNotServiceable()),
			loadState: func(n int32) commonpb.LoadState {
				if n == 1 {
					loading <- struct{}{}
				}
				return commonpb.LoadState_LoadStateLoading
			},
		}
		c, _ := newRecClient(t, f)
		c.rec.loadPoll = 200 * time.Millisecond // 让 UseDatabase 先于下一次轮询排队

		done := make(chan error, 1)
		go func() { _, err := searchVec(context.Background(), c, "c"); done <- err }()
		<-loading
		require.NoError(t, c.UseDatabase(context.Background(), "db2"))

		err := <-done
		var rerr *ReadRecoveryError
		require.True(t, errors.As(err, &rerr))
		assert.True(t, errors.Is(rerr.Last, ErrReadRecoveryDatabaseChanged))
		assert.EqualValues(t, 1, f.nLoadState.Load(), "切库后不再查询加载状态")
		assert.EqualValues(t, 1, f.nSearch.Load(), "切库后不得在新库重试")
	})

	// 地址 URL 带库、当前库被 UseDatabase 置空：SDK 重建时会改用 URL 库，必须明确停止而不是静默换库
	t.Run("URL带库且当前库为空", func(t *testing.T) {
		f := &recFake{search: failFirst(chanNotServiceable())}
		c, tl := newRecClientAt(t, f, "urldb", WithDatabase(""))
		require.Equal(t, "urldb", c.db)
		require.NoError(t, c.UseDatabase(context.Background(), ""))
		require.Equal(t, "", c.db)
		old := c.GetClient()

		_, err := searchVec(context.Background(), c, "c")
		var rerr *ReadRecoveryError
		require.True(t, errors.As(err, &rerr))
		assert.True(t, errors.Is(err, ErrReadRecoveryDatabaseChanged))
		assert.EqualValues(t, 1, f.nSearch.Load(), "不得在 URL 库上重试")
		assert.Zero(t, f.nLoadState.Load())
		assert.Same(t, old, c.GetClient(), "不得安装解析到其他库的客户端")
		assert.Equal(t, "", c.db)
		f.mu.Lock()
		assert.Equal(t, []string{"urldb", "", "urldb"}, f.connDBs) // 初始、UseDatabase("")、被放弃的重建
		assert.Equal(t, []string{""}, f.searchDBs)
		f.mu.Unlock()
		waitOpen(t, tl, 1)
	})

	// 切库后同库并发等待者共享同一停止结果，不各自再建连
	t.Run("并发等待者共享切库结果", func(t *testing.T) {
		const n = 8
		arrived := make(chan struct{})
		f := &recFake{}
		f.search = func(ctx context.Context, i int32) error {
			if i > n {
				return nil
			}
			if i == n {
				close(arrived)
			}
			select {
			case <-arrived:
			case <-ctx.Done():
				return ctx.Err()
			}
			return chanNotServiceable()
		}
		c, tl := newRecClient(t, f)

		var dials atomic.Int32
		entered := make(chan struct{})
		release := make(chan struct{})
		dial := c.rec.dial
		c.rec.dial = func(ctx context.Context, o *Options) (*milvusclient.Client, string, error) {
			if dials.Add(1) == 1 {
				close(entered)
				<-release
			}
			return dial(ctx, o)
		}

		var wg sync.WaitGroup
		errs := make([]error, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, errs[i] = searchVec(context.Background(), c, "c")
			}(i)
		}
		<-entered
		require.NoError(t, c.UseDatabase(context.Background(), "db2"))
		close(release)
		wg.Wait()

		for _, err := range errs {
			assert.True(t, errors.Is(err, ErrReadRecoveryDatabaseChanged), "%v", err)
		}
		assert.EqualValues(t, 1, dials.Load(), "切库后不得为旧库再建连")
		assert.EqualValues(t, n, f.nSearch.Load())
		assert.EqualValues(t, 3, f.nConnect.Load()) // 初始 + UseDatabase + 被放弃的重建
		waitOpen(t, tl, 1)
	})
}

// ---- 重建失败 ----

func TestReadRecoveryRebuildFailure(t *testing.T) {
	t.Run("暂时性失败后下一轮成功", func(t *testing.T) {
		f := &recFake{
			search: failFirst(chanNotServiceable()),
			connect: func(_ context.Context, n int32) error {
				if n == 2 {
					return merr.WrapErrServiceNotReady("proxy", 1, "Initializing")
				}
				return nil
			},
		}
		c, tl := newRecClient(t, f)
		results, err := searchVec(context.Background(), c, "c")
		require.NoError(t, err)
		requireTopID42(t, results)
		assert.EqualValues(t, 3, f.nConnect.Load())
		assert.EqualValues(t, 2, f.nSearch.Load())
		waitOpen(t, tl, 1) // 失败握手的连接已释放
	})

	t.Run("不可恢复失败立即停止且原客户端可用", func(t *testing.T) {
		f := &recFake{
			search: failFirst(chanNotServiceable()),
			connect: func(_ context.Context, n int32) error {
				if n >= 2 {
					return status.Error(codes.PermissionDenied, "denied")
				}
				return nil
			},
		}
		c, tl := newRecClient(t, f)
		old := c.GetClient()
		_, err := searchVec(context.Background(), c, "c")
		var rerr *ReadRecoveryError
		require.True(t, errors.As(err, &rerr))
		assert.Equal(t, codes.PermissionDenied, status.Code(err), "status.Code 取终止原因")
		assert.True(t, errors.Is(rerr.First, merr.ErrChannelNotAvailable))
		assert.EqualValues(t, 1, f.nSearch.Load())
		assert.EqualValues(t, 2, f.nConnect.Load())
		waitOpen(t, tl, 1)

		assert.Same(t, old, c.GetClient())
		results, err := searchVec(context.Background(), c, "c")
		require.NoError(t, err)
		requireTopID42(t, results)
	})
}

// ---- 结果校验 ----

func TestReadRecoveryValidatesRecoveredResult(t *testing.T) {
	f := &recFake{
		search:       failFirst(chanNotServiceable()),
		searchResult: func(int32) *schemapb.SearchResultData { return &schemapb.SearchResultData{NumQueries: 0} },
	}
	c, _ := newRecClient(t, f)
	results, err := searchVec(context.Background(), c, "c")
	assert.Nil(t, results)
	var rerr *ReadRecoveryError
	require.True(t, errors.As(err, &rerr))
	assert.Contains(t, rerr.Last.Error(), "recovered result failed validation")
	assert.EqualValues(t, 2, f.nSearch.Load(), "校验失败不再重试")
}

// ---- 终止原因与错误判定 ----

// 恰在预算到期时出现的真实错误不得被"预算耗尽"抹掉；只有 ctx 引起或仍可恢复的错误才归一
func TestReadRecoveryKeepsRealErrorAtBudgetExpiry(t *testing.T) {
	ok := func() error { return nil }
	serverErr := func(err error) error { return merr.Error(merr.Status(err)) }

	t.Run("首次不可恢复错误原样返回", func(t *testing.T) {
		c, _ := newRecClient(t, &recFake{})
		c.rec.budget = 100 * time.Millisecond
		err := c.read(context.Background(), "Search", "c", nil, func(ctx context.Context, _ *milvusclient.Client) error {
			<-ctx.Done()
			return status.Error(codes.PermissionDenied, "denied")
		}, ok)
		var rerr *ReadRecoveryError
		assert.False(t, errors.As(err, &rerr))
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
	})

	t.Run("重试时不可恢复错误保留为Last", func(t *testing.T) {
		c, _ := newRecClient(t, &recFake{})
		c.rec.budget = 300 * time.Millisecond
		var n atomic.Int32
		err := c.read(context.Background(), "Query", "c", nil, func(ctx context.Context, _ *milvusclient.Client) error {
			if n.Add(1) == 1 {
				return serverErr(chanNotServiceable())
			}
			<-ctx.Done()
			return serverErr(merr.ErrCollectionNotLoaded)
		}, ok)
		var rerr *ReadRecoveryError
		require.True(t, errors.As(err, &rerr))
		assert.True(t, errors.Is(rerr.Last, merr.ErrCollectionNotLoaded))
		assert.False(t, errors.Is(err, ErrReadRecoveryExhausted))
		assert.EqualValues(t, 2, n.Load())
	})

	t.Run("到期时仍可恢复的错误归一为预算耗尽", func(t *testing.T) {
		c, _ := newRecClient(t, &recFake{})
		c.rec.budget = 300 * time.Millisecond
		var n atomic.Int32
		err := c.read(context.Background(), "Query", "c", nil, func(ctx context.Context, _ *milvusclient.Client) error {
			if n.Add(1) > 1 {
				<-ctx.Done()
			}
			return serverErr(chanNotServiceable())
		}, ok)
		assert.True(t, errors.Is(err, ErrReadRecoveryExhausted))
		assert.EqualValues(t, 2, n.Load())
	})
}

// GRPCStatus 只由 Last 决定，首次失败（常为可恢复错误）不会让调用方误判为可重试
func TestReadRecoveryErrorStatusUsesLastOnly(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "down")
	cases := []struct {
		name string
		last error
		want codes.Code
	}{
		{"预算耗尽", ErrReadRecoveryExhausted, codes.DeadlineExceeded},
		{"调用方超时", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"调用方取消", errors.Wrap(context.Canceled, "x"), codes.Canceled},
		{"gRPC权限", status.Error(codes.PermissionDenied, "x"), codes.PermissionDenied},
		{"gRPC超时", status.Error(codes.DeadlineExceeded, "x"), codes.DeadlineExceeded},
		{"merr集合未加载", merr.Error(merr.Status(merr.ErrCollectionNotLoaded)), codes.Unknown},
		{"客户端已关闭", errClientClosed, codes.Unknown},
		{"切库", ErrReadRecoveryDatabaseChanged, codes.Unknown},
	}
	for _, tc := range cases {
		err := error(&ReadRecoveryError{Op: "Search", Collection: "c", First: unavailable, Last: tc.last, Attempts: 2})
		assert.Equal(t, tc.want, status.Code(err), tc.name)
		assert.False(t, isRecoverableReadErr(err), tc.name)
		s, ok := status.FromError(err)
		assert.True(t, ok, tc.name)
		assert.Contains(t, s.Message(), "first error", tc.name)
	}

	err := &ReadRecoveryError{First: merr.Error(merr.Status(merr.WrapErrServiceNotReady("proxy", 1, "x"))), Last: ErrReadRecoveryExhausted}
	assert.False(t, errors.Is(err, merr.ErrServiceNotReady), "First 不参与 Unwrap")
	assert.True(t, errors.Is(err, ErrReadRecoveryExhausted))
}

// 加载状态查询以可恢复错误失败时，下一轮重建该查询实际使用的代际，而不是按旧代际永久跳过
func TestReadRecoveryRebuildsGenerationObservedByLoadState(t *testing.T) {
	f := &recFake{
		search: failFirst(chanNotServiceable()),
		loadStateErr: func(n int32) error {
			if n == 1 {
				return merr.WrapErrServiceNotReady("proxy", 1, "Initializing")
			}
			return nil
		},
	}
	c, tl := newRecClient(t, f)
	var dials atomic.Int32
	dial := c.rec.dial
	c.rec.dial = func(ctx context.Context, o *Options) (*milvusclient.Client, string, error) {
		dials.Add(1)
		return dial(ctx, o)
	}

	results, err := searchVec(context.Background(), c, "c")
	require.NoError(t, err)
	requireTopID42(t, results)
	assert.EqualValues(t, 2, dials.Load(), "第二轮应重建加载状态查询失败时所用的代际")
	assert.EqualValues(t, 3, f.nConnect.Load())
	assert.EqualValues(t, 2, f.nLoadState.Load())
	assert.EqualValues(t, 2, f.nSearch.Load())
	assert.EqualValues(t, 2, c.generation())
	waitOpen(t, tl, 1)
}

// Close 返回前，已握手但尚未安装的候选客户端必须已关闭
func TestReadRecoveryCloseWaitsUninstalledCandidate(t *testing.T) {
	f := &recFake{search: failFirst(chanNotServiceable())}
	c, tl := newRecClient(t, f)
	dialed := make(chan *milvusclient.Client, 1)
	dial := c.rec.dial
	c.rec.dial = func(ctx context.Context, o *Options) (*milvusclient.Client, string, error) {
		cli, db, err := dial(ctx, o)
		if err == nil {
			dialed <- cli
			<-ctx.Done() // 握手已完成，持有候选客户端直到 Close 取消
		}
		return cli, db, err
	}

	done := make(chan error, 1)
	go func() { _, err := searchVec(context.Background(), c, "c"); done <- err }()
	candidate := <-dialed

	require.NoError(t, c.Close())
	assert.Nil(t, candidate.GetService(), "Close 返回时候选客户端必须已关闭")
	assert.True(t, errors.Is(<-done, errClientClosed))
	assert.EqualValues(t, 0, c.generation(), "候选客户端未被安装")
	waitOpen(t, tl, 0)
}

// ---- Close 竞态 ----

func TestReadRecoveryCloseRace(t *testing.T) {
	t.Run("等待间隔中Close", func(t *testing.T) {
		f := &recFake{search: always(chanNotServiceable())}
		c, tl := newRecClient(t, f)
		c.rec.interval = 10 * time.Second

		done := make(chan error, 1)
		go func() { _, err := searchVec(context.Background(), c, "c"); done <- err }()
		require.Eventually(t, func() bool { return f.nSearch.Load() == 1 }, 2*time.Second, time.Millisecond)
		time.Sleep(50 * time.Millisecond)

		start := time.Now()
		require.NoError(t, c.Close())
		select {
		case err := <-done:
			assert.True(t, errors.Is(err, errClientClosed))
		case <-time.After(time.Second):
			t.Fatal("Close 未终止恢复")
		}
		assert.Less(t, time.Since(start), time.Second)
		assert.EqualValues(t, 1, f.nConnect.Load())
		waitOpen(t, tl, 0)
	})

	t.Run("重建握手中Close", func(t *testing.T) {
		entered := make(chan struct{}, 1)
		f := &recFake{
			search: failFirst(chanNotServiceable()),
			connect: func(ctx context.Context, n int32) error {
				if n == 1 {
					return nil
				}
				entered <- struct{}{}
				<-ctx.Done()
				return status.FromContextError(ctx.Err()).Err()
			},
		}
		c, tl := newRecClient(t, f)

		done := make(chan error, 1)
		go func() { _, err := searchVec(context.Background(), c, "c"); done <- err }()
		<-entered

		require.NoError(t, c.Close())
		select {
		case err := <-done:
			assert.True(t, errors.Is(err, errClientClosed))
		case <-time.After(time.Second):
			t.Fatal("Close 未中止重建握手")
		}
		waitOpen(t, tl, 0)
	})

	t.Run("等待替换时Close", func(t *testing.T) {
		f := &recFake{search: failFirst(chanNotServiceable()), slowGate: make(chan struct{}), slowEnter: make(chan struct{}, 1)}
		c, tl := newRecClient(t, f)

		aDone := make(chan error, 1)
		go func() { _, err := c.HasCollection(context.Background(), "slow"); aDone <- err }()
		<-f.slowEnter

		bDone := make(chan error, 1)
		go func() { _, err := searchVec(context.Background(), c, "c"); bDone <- err }()
		require.Eventually(t, func() bool { return f.nConnect.Load() == 2 }, 2*time.Second, time.Millisecond)
		require.Eventually(t, func() bool { c.gate.mu.Lock(); defer c.gate.mu.Unlock(); return len(c.gate.queue) == 1 }, 2*time.Second, time.Millisecond)

		closeDone := make(chan error, 1)
		go func() { closeDone <- c.Close() }()
		select {
		case err := <-bDone:
			assert.True(t, errors.Is(err, errClientClosed))
		case <-time.After(time.Second):
			t.Fatal("等待替换的恢复未被 Close 唤醒")
		}
		select {
		case <-closeDone:
			t.Fatal("在途请求未结束，Close 不应返回")
		case <-time.After(50 * time.Millisecond):
		}

		close(f.slowGate)
		require.NoError(t, <-aDone)
		require.NoError(t, <-closeDone)
		waitOpen(t, tl, 0) // 旧客户端与未启用的新客户端均已关闭
	})
}

// ---- 配置继承 ----

func TestReadRecoveryInheritsConfig(t *testing.T) {
	opts := []Option{
		WithAuth("root", "pw"),
		WithDatabase("db1"),
		WithRetry(3, time.Second),
		WithGrpcOpts(7*time.Second, 9*time.Second, false, 50*time.Millisecond, 1.5, 0.1, 2*time.Second, 4*time.Second, 8<<20),
	}
	f := &recFake{search: func(_ context.Context, n int32) error {
		if n == 1 || n == 3 {
			return chanNotServiceable()
		}
		return nil
	}}
	c, _ := newRecClient(t, f, opts...)

	want := DefaultOptions()
	for _, opt := range append([]Option{WithAddress(c.rec.opts.Address), WithReadRecovery(true)}, opts...) {
		opt(want)
	}
	assert.Equal(t, *want, c.rec.opts, "恢复保存完整原配置")

	var mu sync.Mutex
	var seen []Options
	dial := c.rec.dial
	c.rec.dial = func(ctx context.Context, o *Options) (*milvusclient.Client, string, error) {
		mu.Lock()
		seen = append(seen, *o)
		mu.Unlock()
		return dial(ctx, o)
	}

	_, err := searchVec(context.Background(), c, "c")
	require.NoError(t, err)
	require.NoError(t, c.UseDatabase(context.Background(), "db2"))
	_, err = searchVec(context.Background(), c, "c")
	require.NoError(t, err)

	mu.Lock()
	require.Len(t, seen, 2)
	assert.Equal(t, *want, seen[0])
	want2 := *want
	want2.DBName = "db2"
	assert.Equal(t, want2, seen[1], "重建使用当前库，其余配置不变")
	mu.Unlock()

	f.mu.Lock()
	defer f.mu.Unlock()
	require.Len(t, f.connDBs, 4) // 初始、重建、UseDatabase、重建
	assert.Equal(t, []string{"db1", "db1", "db2", "db2"}, f.connDBs)
	for _, auth := range f.connAuth {
		assert.Equal(t, f.connAuth[0], auth)
	}
	assert.NotEmpty(t, f.connAuth[0], "重建保留认证头")
}

// TLS / APIKey 等不便以明文 fake 握手验证的字段：保存的配置与原配置逐字段一致
func TestNewRecoveryKeepsAllOptions(t *testing.T) {
	o := DefaultOptions()
	for _, opt := range []Option{WithAddress("https://example.invalid:443/urldb"), WithAPIKey("key"), WithTLS(), WithDisableConn(true), WithReadRecovery(true)} {
		opt(o)
	}
	r := newRecovery(*o)
	assert.Equal(t, *o, r.opts)
	assert.Equal(t, ReadRecoveryInterval, r.interval)
	assert.Equal(t, ReadRecoveryBudget, r.budget)
	r.stop()
	assert.True(t, strings.HasPrefix(r.opts.Address, "https://"))
}

// ---- 请求驱动：通用工装 ----

// readOp 以统一形式发起 Search 或 Query：失败时原样返回 read 的错误，成功时校验返回数据（id 42）。
// calls 返回该方法在 fake 上的调用次数，set 设置该方法的 fake 行为
type readOp struct {
	name  string
	calls func(f *recFake) int32
	set   func(f *recFake, fn func(context.Context, int32) error)
	do    func(ctx context.Context, c *client) error
}

var readOps = []readOp{
	{
		name:  "Search",
		calls: func(f *recFake) int32 { return f.nSearch.Load() },
		set:   func(f *recFake, fn func(context.Context, int32) error) { f.search = fn },
		do: func(ctx context.Context, c *client) error {
			results, err := searchVec(ctx, c, "c")
			if err != nil {
				return err
			}
			return checkSearchID42(results)
		},
	},
	{
		name:  "Query",
		calls: func(f *recFake) int32 { return f.nQuery.Load() },
		set:   func(f *recFake, fn func(context.Context, int32) error) { f.query = fn },
		do: func(ctx context.Context, c *client) error {
			cols, err := c.Query(ctx, "c", nil, "id > 0", []string{"id"})
			if err != nil {
				return err
			}
			return checkQueryID42(cols)
		},
	},
}

func checkSearchID42(results []milvusclient.ResultSet) error {
	if len(results) != 1 || results[0].ResultCount != 1 {
		return errors.Errorf("unexpected search results: %d sets", len(results))
	}
	id, err := results[0].IDs.GetAsInt64(0)
	if err != nil {
		return err
	}
	if id != 42 {
		return errors.Errorf("search got id %d, want 42", id)
	}
	return nil
}

func checkQueryID42(cols []column.Column) error {
	if len(cols) != 1 || cols[0].Len() != 1 {
		return errors.Errorf("unexpected query columns: %d", len(cols))
	}
	v, err := cols[0].GetAsInt64(0)
	if err != nil {
		return err
	}
	if v != 42 {
		return errors.Errorf("query got id %d, want 42", v)
	}
	return nil
}

// freshPeers 记录放行（arm）之后完成 Connect 握手的 TCP 连接（按客户端地址）。查询只在这些连接上成功，
// 由此证明成功来自包装层重建并安装的新 SDK 客户端：旧句柄及 gRPC 对旧连接的内部重连都不发 Connect，不会被计入
type freshPeers struct {
	mu    sync.Mutex
	armed bool
	addrs map[string]bool
}

func (p *freshPeers) arm() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed = true
}

// connected 在 fake 的 Connect 中调用
func (p *freshPeers) connected(ctx context.Context) {
	pr, ok := peer.FromContext(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	if ok && p.armed {
		if p.addrs == nil {
			p.addrs = map[string]bool{}
		}
		p.addrs[pr.Addr.String()] = true
	}
}

// read 作为 fake 的查询行为：旧连接上返回白名单内的 503，放行后新握手的连接上成功
func (p *freshPeers) read(ctx context.Context, _ int32) error {
	pr, ok := peer.FromContext(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	if ok && p.addrs[pr.Addr.String()] {
		return nil
	}
	return chanNotServiceable()
}

// countDials 统计包装层经 rec.dial 发起的重建拨号
func countDials(c *client) *atomic.Int32 {
	n := new(atomic.Int32)
	dial := c.rec.dial
	c.rec.dial = func(ctx context.Context, o *Options) (*milvusclient.Client, string, error) {
		n.Add(1)
		return dial(ctx, o)
	}
	return n
}

// wrapperCounts 是包装层可观察的全部主动活动：重建拨号、握手、查询、加载状态查询与加载
type wrapperCounts struct{ dials, connects, searches, queries, loadStates, loads int32 }

func countWrapper(f *recFake, dials *atomic.Int32) wrapperCounts {
	return wrapperCounts{dials.Load(), f.nConnect.Load(), f.nSearch.Load(), f.nQuery.Load(), f.nLoadState.Load(), f.nLoad.Load()}
}

// requireNoRebuild 断言没有进行中的重建（重建结束后 building 必须清除）
func requireNoRebuild(t *testing.T, c *client) {
	t.Helper()
	c.rec.mu.Lock()
	defer c.rec.mu.Unlock()
	require.Nil(t, c.rec.building, "重建结束后应清除 building")
}

type whoKey struct{}

// who 返回调用方 ctx 上标记的请求身份（恢复 ctx 派生自调用方 ctx，因此在拨号与共享等待中可见）
func who(ctx context.Context) string {
	s, _ := ctx.Value(whoKey{}).(string)
	return s
}

func withWho(name string) context.Context {
	return context.WithValue(context.Background(), whoKey{}, name)
}

// traceRebuild 记录各次重建拨号的发起者，并在调用方开始等待他人发起的重建时发送其身份
func traceRebuild(c *client) (dialers func() []string, sharing chan string) {
	var mu sync.Mutex
	var names []string
	ch := make(chan string, 8)
	dial := c.rec.dial
	c.rec.dial = func(ctx context.Context, o *Options) (*milvusclient.Client, string, error) {
		mu.Lock()
		names = append(names, who(ctx))
		mu.Unlock()
		return dial(ctx, o)
	}
	c.rec.onShare = func(ctx context.Context) { ch <- who(ctx) }
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), names...)
	}, ch
}

func recvWithin[T any](t *testing.T, ch chan T, d time.Duration, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(d):
		t.Fatalf("%s 内未发生: %s", d, what)
	}
	var zero T
	return zero
}

// ---- 请求驱动：失败后不残留恢复，下一请求独立恢复 ----

// idleWindow 是请求失败后的基本空闲观察窗口，远长于测试中的恢复间隔（10ms）
const idleWindow = 300 * time.Millisecond

// waitPhaseInterval 是 A 停在等待间隔阶段时的恢复间隔：明显大于 300ms 的预算/deadline，A 必在间隔内失败；
// 又足够短，空闲窗口（取其与 idleWindow 的较大者再加余量）能覆盖它到期：取消后若等待转入后台继续，到期后的拨号会落在窗口内
const waitPhaseInterval = 600 * time.Millisecond

// 同一客户端上请求 A 停在等待间隔或重建握手时，因内部预算、调用方 deadline 或 cancel 失败：
// A 及时返回、未安装半成品、握手连接已释放；空闲窗口（超过 A 所用的恢复间隔）内包装层不再拨号、握手、查询或查询加载状态
// （无后台重连/健康轮询/自动重放/延迟重建）；
// 之后以新 ctx 发起的请求 B 仍先在旧连接上遇白名单 503，必须经真实重建、在新握手的连接上查询才能成功，且数据正确
func TestReadRecoveryNextRequestRecoversAfterFailure(t *testing.T) {
	for _, op := range readOps {
		for _, inHandshake := range []bool{false, true} {
			phase := "等待间隔"
			if inHandshake {
				phase = "重建握手"
			}
			for _, cause := range []string{"内部预算", "调用方deadline", "调用方cancel"} {
				t.Run(op.name+"/"+phase+"/"+cause, func(t *testing.T) {
					testFailThenNextRecovers(t, op, inHandshake, cause)
				})
			}
		}
	}
}

func testFailThenNextRecovers(t *testing.T, op readOp, inHandshake bool, cause string) {
	var fp freshPeers
	var hang atomic.Bool
	handshaking := make(chan struct{}, 1)
	firstRead := make(chan struct{})
	var firstOnce sync.Once
	f := &recFake{connect: func(ctx context.Context, _ int32) error {
		if hang.Load() {
			select {
			case handshaking <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return status.FromContextError(ctx.Err()).Err()
		}
		fp.connected(ctx)
		return nil
	}}
	op.set(f, func(ctx context.Context, n int32) error {
		firstOnce.Do(func() { close(firstRead) })
		return fp.read(ctx, n)
	})
	var log eventLog
	c, tl := newRecClient(t, f, WithReadRecoveryObserver(log.add))
	dials := countDials(c)
	old := c.GetClient()

	// 请求 A：首次查询遇 503 进入恢复，停在等待间隔（间隔拉长到 waitPhaseInterval）或重建握手（握手挂起）时失败
	aInterval := c.rec.interval
	if inHandshake {
		hang.Store(true)
	} else {
		aInterval = waitPhaseInterval
		c.rec.interval = aInterval
	}
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	var want error
	switch cause {
	case "内部预算":
		c.rec.budget = 300 * time.Millisecond
		want = ErrReadRecoveryExhausted
	case "调用方deadline":
		var cancel context.CancelFunc
		ctxA, cancel = context.WithTimeout(ctxA, 300*time.Millisecond)
		defer cancel()
		want = context.DeadlineExceeded
	case "调用方cancel":
		want = context.Canceled
	default:
		t.Fatalf("unknown cause %q", cause)
	}

	// A 在独立 goroutine 中执行，取消时机的同步与断言都留在测试 goroutine；
	// 测试提前失败时 defer cancelA 使 A 结束，aDone 有缓冲，A 不会阻塞在发送上
	start := time.Now()
	aDone := make(chan error, 1)
	go func(ctx context.Context) { aDone <- op.do(ctx, c) }(ctxA)
	if cause == "调用方cancel" {
		if inHandshake {
			recvWithin(t, handshaking, 2*time.Second, "A 的重建握手在服务端挂起")
		} else {
			// 首次查询已到达服务端，且首次读门禁已释放（503 已回到客户端、A 已离开首次 readOnce）后取消：
			// 此后 A 只可能处于等待间隔，结果断言（已包装、Attempts=1、未拨号）再次确认所处阶段
			recvWithin(t, firstRead, 2*time.Second, "A 的首次查询到达服务端")
			require.Eventually(t, func() bool {
				c.gate.mu.Lock()
				defer c.gate.mu.Unlock()
				return c.gate.readers == 0
			}, 2*time.Second, time.Millisecond, "A 释放首次读门禁")
		}
		cancelA()
	}
	errA := recvWithin(t, aDone, 3*time.Second, "A 按预算/ctx 结束")
	assert.Less(t, time.Since(start), 2*time.Second, "A 应按预算/ctx 及时结束")
	var rerr *ReadRecoveryError
	require.True(t, errors.As(errA, &rerr), "A 应已进入恢复: %v", errA)
	assert.True(t, errors.Is(errA, want), "%v", errA)
	assert.True(t, errors.Is(rerr.First, merr.ErrChannelNotAvailable))
	assert.Equal(t, 1, rerr.Attempts, "A 只执行了首次查询")
	if inHandshake {
		assert.EqualValues(t, 1, dials.Load(), "A 停在握手：恰好拨号一次")
		assert.EqualValues(t, 2, f.nConnect.Load(), "初始 + A 被放弃的握手")
	} else {
		assert.Zero(t, dials.Load(), "A 停在等待间隔：未拨号")
		assert.EqualValues(t, 1, f.nConnect.Load())
	}
	assert.EqualValues(t, 1, op.calls(f))
	assert.Zero(t, c.generation(), "未完成的重建不得安装")
	assert.Same(t, old, c.GetClient())
	requireNoRebuild(t, c)
	waitOpen(t, tl, 1) // 被放弃的握手连接已释放，只剩原连接
	evs := log.all()
	require.Len(t, evs, 1)
	assert.Same(t, errA, evs[0].Err)

	// 空闲窗口：缩短恢复间隔后观察，包装层不得自行拨号、握手、查询或查询加载状态。
	// 窗口超过 A 所用的恢复间隔：A 若把等待间隔转入后台继续，其到期后的拨号/重建会落在窗口内
	window := max(idleWindow, aInterval+100*time.Millisecond)
	hang.Store(false)
	c.rec.interval = 10 * time.Millisecond
	idle := countWrapper(f, dials)
	time.Sleep(window)
	assert.Equal(t, idle, countWrapper(f, dials), "A 失败后不得有后台重连、健康轮询、自动重放或延迟重建")
	assert.Zero(t, c.generation(), "空闲期间不得安装重建")
	requireNoRebuild(t, c)
	assert.Len(t, log.all(), 1, "空闲期间不产生事件")

	// 请求 B：新 ctx、完整预算；只有包装层真实重建出的新连接上的查询才成功
	c.rec.budget = ReadRecoveryBudget
	fp.arm()
	callsBefore := op.calls(f)
	ctxB, cancelB := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelB()
	require.NoError(t, op.do(ctxB, c), "B 经真实重建后成功且数据正确")
	after := countWrapper(f, dials)
	assert.Equal(t, idle.dials+1, after.dials, "B 真实重建一次")
	assert.Equal(t, idle.connects+1, after.connects)
	assert.Equal(t, callsBefore+2, op.calls(f), "B 先在旧连接遇 503，再在新连接成功")
	assert.Zero(t, after.loads, "恢复不得发起 Load")
	assert.EqualValues(t, 1, c.generation())
	assert.NotSame(t, old, c.GetClient(), "底层客户端已替换")
	requireNoRebuild(t, c)
	waitOpen(t, tl, 1) // 旧连接已关闭，只剩新连接

	evs = log.all()
	require.Len(t, evs, 2)
	assert.Equal(t, op.name, evs[1].Op)
	assert.NoError(t, evs[1].Err)
	assert.Equal(t, 2, evs[1].Attempts)
	assert.True(t, errors.Is(evs[1].FirstErr, merr.ErrChannelNotAvailable), "B 首次仍遇白名单错误")
}

// ---- 共享重建中的取消 ----

// 共享重建的发起者被取消：取消者及时返回；确已在等待该重建的同库请求接替重建并成功，数据正确、不泄漏连接
func TestReadRecoverySharedRebuildInitiatorCanceled(t *testing.T) {
	for _, op := range readOps {
		t.Run(op.name, func(t *testing.T) {
			var fp freshPeers
			handshaking := make(chan struct{}, 1)
			f := &recFake{connect: func(ctx context.Context, n int32) error {
				fp.connected(ctx)
				if n == 2 { // A 发起的重建握手：挂起到 A 被取消
					handshaking <- struct{}{}
					<-ctx.Done()
					return status.FromContextError(ctx.Err()).Err()
				}
				return nil
			}}
			op.set(f, fp.read)
			var log eventLog
			c, tl := newRecClient(t, f, WithReadRecoveryObserver(log.add))
			fp.arm()
			dialers, sharing := traceRebuild(c)

			ctxA, cancelA := context.WithCancel(withWho("A"))
			defer cancelA()
			aDone := make(chan error, 1)
			go func() { aDone <- op.do(ctxA, c) }()
			recvWithin(t, handshaking, 2*time.Second, "A 发起重建并进入握手")

			ctxW, cancelW := context.WithTimeout(withWho("W"), 5*time.Second)
			defer cancelW()
			wDone := make(chan error, 1)
			go func() { wDone <- op.do(ctxW, c) }()
			require.Equal(t, "W", recvWithin(t, sharing, 2*time.Second, "W 开始等待 A 发起的重建"))

			cancelA()
			errA := recvWithin(t, aDone, time.Second, "取消者 A 及时返回")
			var rerr *ReadRecoveryError
			require.True(t, errors.As(errA, &rerr), "%v", errA)
			assert.True(t, errors.Is(errA, context.Canceled))
			assert.Equal(t, 1, rerr.Attempts)

			require.NoError(t, recvWithin(t, wDone, 5*time.Second, "W 接替重建后返回"), "W 接替重建后成功且数据正确")
			assert.Equal(t, []string{"A", "W"}, dialers(), "A 的重建被放弃后由 W 接替")
			assert.Empty(t, sharing, "W 只等待过一次")
			assert.EqualValues(t, 3, f.nConnect.Load()) // 初始、A 被放弃的握手、W 的重建
			assert.EqualValues(t, 3, op.calls(f))       // A、W 各一次首次查询，W 一次重试
			assert.EqualValues(t, 1, c.generation())
			requireNoRebuild(t, c)
			waitOpen(t, tl, 1)

			evs := log.all()
			require.Len(t, evs, 2)
			for _, ev := range evs {
				if ev.Err == nil {
					assert.Equal(t, 2, ev.Attempts, "W 恢复成功")
				} else {
					assert.Same(t, errA, ev.Err, "A 的事件与其返回值一致")
				}
				assert.True(t, errors.Is(ev.FirstErr, merr.ErrChannelNotAvailable))
			}
		})
	}
}

// 等待共享重建的请求被取消：等待者及时返回，发起者的重建不受影响并成功
func TestReadRecoverySharedRebuildWaiterCanceled(t *testing.T) {
	for _, op := range readOps {
		t.Run(op.name, func(t *testing.T) {
			var fp freshPeers
			handshaking := make(chan struct{}, 1)
			release := make(chan struct{})
			f := &recFake{connect: func(ctx context.Context, n int32) error {
				fp.connected(ctx)
				if n == 2 { // A 发起的重建握手：挂起到测试放行
					handshaking <- struct{}{}
					select {
					case <-release:
					case <-ctx.Done():
						return status.FromContextError(ctx.Err()).Err()
					}
				}
				return nil
			}}
			op.set(f, fp.read)
			var log eventLog
			c, tl := newRecClient(t, f, WithReadRecoveryObserver(log.add))
			fp.arm()
			dialers, sharing := traceRebuild(c)

			ctxA, cancelA := context.WithTimeout(withWho("A"), 5*time.Second)
			defer cancelA()
			aDone := make(chan error, 1)
			go func() { aDone <- op.do(ctxA, c) }()
			recvWithin(t, handshaking, 2*time.Second, "A 发起重建并进入握手")

			ctxW, cancelW := context.WithCancel(withWho("W"))
			defer cancelW()
			wDone := make(chan error, 1)
			go func() { wDone <- op.do(ctxW, c) }()
			require.Equal(t, "W", recvWithin(t, sharing, 2*time.Second, "W 开始等待 A 发起的重建"))

			cancelW()
			errW := recvWithin(t, wDone, time.Second, "被取消的等待者 W 及时返回")
			var rerr *ReadRecoveryError
			require.True(t, errors.As(errW, &rerr), "%v", errW)
			assert.True(t, errors.Is(errW, context.Canceled))
			assert.Equal(t, 1, rerr.Attempts)
			select {
			case err := <-aDone:
				t.Fatalf("等待者取消不得影响发起者: %v", err)
			default:
			}
			c.rec.mu.Lock()
			building := c.rec.building
			c.rec.mu.Unlock()
			require.NotNil(t, building, "发起者的重建仍在进行")

			close(release)
			require.NoError(t, recvWithin(t, aDone, 5*time.Second, "发起者 A 完成重建后返回"), "A 成功且数据正确")
			assert.Equal(t, []string{"A"}, dialers(), "W 被取消后未自行拨号，A 的重建未被打断")
			assert.Empty(t, sharing)
			assert.EqualValues(t, 2, f.nConnect.Load())
			assert.EqualValues(t, 3, op.calls(f)) // A、W 各一次首次查询，A 一次重试
			assert.EqualValues(t, 1, c.generation())
			requireNoRebuild(t, c)
			waitOpen(t, tl, 1)

			evs := log.all()
			require.Len(t, evs, 2)
			for _, ev := range evs {
				if ev.Err == nil {
					assert.Equal(t, 2, ev.Attempts, "A 恢复成功")
				} else {
					assert.Same(t, errW, ev.Err, "W 的事件与其返回值一致")
				}
			}
		})
	}
}

// ---- observer 出口覆盖 ----

// requireOneEvent 断言恰好通知一次且事件与返回值一致；未进入恢复、原样返回的错误同时就是 FirstErr。
// 错误一律按身份（==）比较而非深比较：内容相同的不同错误值不能通过（当前各用例的错误动态类型均可比较）
func requireOneEvent(t *testing.T, log *eventLog, op string, err error) ReadRecoveryEvent {
	t.Helper()
	evs := log.all()
	require.Len(t, evs, 1, "每次调用恰好通知一次")
	ev := evs[0]
	assert.Equal(t, op, ev.Op)
	assert.Equal(t, "c", ev.Collection)
	require.Error(t, ev.FirstErr, "事件中 FirstErr 恒非 nil")
	var rerr *ReadRecoveryError
	switch {
	case err == nil:
		assert.NoError(t, ev.Err)
	case errors.As(err, &rerr):
		assert.Same(t, err, ev.Err)
		assert.True(t, ev.Err == err, "事件的 Err 与返回的错误为同一值")
		assert.True(t, ev.FirstErr == rerr.First, "事件的 FirstErr 与 ReadRecoveryError.First 为同一值")
		assert.Equal(t, rerr.Attempts, ev.Attempts)
		assert.Equal(t, rerr.Elapsed, ev.Elapsed)
	default:
		assert.True(t, ev.Err == err, "原样返回的错误即事件的 Err（同一值）: %v / %v", ev.Err, err)
		assert.True(t, ev.FirstErr == err, "未进入恢复时 Err 与 FirstErr 为同一值: %v / %v", ev.FirstErr, err)
	}
	return ev
}

// requireRaw 断言错误未被包装为 ReadRecoveryError
func requireRaw(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var rerr *ReadRecoveryError
	require.False(t, errors.As(err, &rerr), "应原样返回: %v", err)
}

// 启用恢复时，Search/Query 首次直接成功不通知；其余每个最终出口恰好通知一次，ev.Err 与返回值一致，首次错误保留
func TestReadRecoveryObserverEveryExit(t *testing.T) {
	permission := status.Error(codes.PermissionDenied, "denied")
	for _, op := range readOps {
		t.Run(op.name+"/首次直接成功不通知", func(t *testing.T) {
			var log eventLog
			c, _ := newRecClient(t, &recFake{}, WithReadRecoveryObserver(log.add))
			require.NoError(t, op.do(context.Background(), c))
			assert.Empty(t, log.all())
		})

		t.Run(op.name+"/首次不可恢复原样返回", func(t *testing.T) {
			var log eventLog
			f := &recFake{}
			op.set(f, always(permission))
			c, _ := newRecClient(t, f, WithReadRecoveryObserver(log.add))
			err := op.do(context.Background(), c)
			requireRaw(t, err)
			assert.Equal(t, codes.PermissionDenied, status.Code(err))
			ev := requireOneEvent(t, &log, op.name, err)
			assert.Equal(t, 1, ev.Attempts)
			assert.EqualValues(t, 1, op.calls(f))
			assert.EqualValues(t, 1, f.nConnect.Load(), "不重建")
		})

		t.Run(op.name+"/首次查询中调用方deadline原样返回", func(t *testing.T) {
			var log eventLog
			f := &recFake{}
			op.set(f, blockFirstSearch(nil))
			c, _ := newRecClient(t, f, WithReadRecoveryObserver(log.add))
			ctx, cancel := shortCtx(150 * time.Millisecond)
			defer cancel()
			err := op.do(ctx, c)
			requireRaw(t, err)
			assert.True(t, isCtxErr(err), "%v", err)
			ev := requireOneEvent(t, &log, op.name, err)
			assert.Equal(t, 1, ev.Attempts)
		})

		t.Run(op.name+"/首次查询中调用方cancel原样返回", func(t *testing.T) {
			var log eventLog
			entered := make(chan struct{}, 1)
			f := &recFake{}
			op.set(f, blockFirstSearch(entered))
			c, _ := newRecClient(t, f, WithReadRecoveryObserver(log.add))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				select {
				case <-entered:
					cancel()
				case <-ctx.Done():
				}
			}()
			err := op.do(ctx, c)
			requireRaw(t, err)
			assert.True(t, isCtxErr(err), "%v", err)
			ev := requireOneEvent(t, &log, op.name, err)
			assert.Equal(t, 1, ev.Attempts)
		})

		t.Run(op.name+"/首次门禁等待中调用方deadline原样返回", func(t *testing.T) {
			var log eventLog
			f := &recFake{}
			c, _ := newRecClient(t, f, WithReadRecoveryObserver(log.add))
			require.NoError(t, c.gate.acquire(context.Background(), true)) // 模拟长写占用门禁
			defer c.gate.release(true)
			ctx, cancel := shortCtx(100 * time.Millisecond)
			defer cancel()
			err := op.do(ctx, c)
			requireRaw(t, err)
			assert.True(t, errors.Is(err, context.DeadlineExceeded))
			ev := requireOneEvent(t, &log, op.name, err)
			assert.Zero(t, ev.Attempts, "未越过门禁，未发出查询")
			assert.Zero(t, op.calls(f))
		})

		t.Run(op.name+"/首次门禁等待被预算截断", func(t *testing.T) {
			var log eventLog
			f := &recFake{}
			c, _ := newRecClient(t, f, WithReadRecoveryObserver(log.add))
			c.rec.budget = 100 * time.Millisecond
			require.NoError(t, c.gate.acquire(context.Background(), true))
			defer c.gate.release(true)
			err := op.do(context.Background(), c)
			assert.True(t, errors.Is(err, ErrReadRecoveryExhausted), "%v", err)
			ev := requireOneEvent(t, &log, op.name, err)
			assert.True(t, isCtxErr(ev.FirstErr), "首次错误为门禁等待被截断")
			assert.Zero(t, ev.Attempts, "未越过门禁，未发出查询")
			assert.Zero(t, op.calls(f))
		})

		// Close 的取消（经恢复 ctx）与门禁拒绝谁先生效取决于时机：原样返回 client is closed，
		// 或 Last 为它的 ReadRecoveryError（First 为门禁等待被取消的 ctx 错误）；两者都恰好通知一次
		t.Run(op.name+"/首次门禁等待中Close", func(t *testing.T) {
			var log eventLog
			f := &recFake{}
			c, _ := newRecClient(t, f, WithReadRecoveryObserver(log.add))
			require.NoError(t, c.gate.acquire(context.Background(), true)) // 模拟长写占用门禁
			// Close 要等写门禁释放才返回；测试提前失败时也须释放，否则 Close 与 Cleanup 会一直等待
			releaseWrite := sync.OnceFunc(func() { c.gate.release(true) })
			defer releaseWrite()

			done := make(chan error, 1)
			go func() { done <- op.do(context.Background(), c) }()
			require.Eventually(t, func() bool {
				c.gate.mu.Lock()
				defer c.gate.mu.Unlock()
				return len(c.gate.queue) == 1
			}, 2*time.Second, time.Millisecond, "调用已在门禁队列中等待")

			closeDone := make(chan error, 1)
			go func() { closeDone <- c.Close() }()
			err := recvWithin(t, done, 2*time.Second, "门禁等待中的调用被 Close 终止")
			require.Error(t, err)
			assert.True(t, errors.Is(err, errClientClosed), "%v", err)
			var rerr *ReadRecoveryError
			if errors.As(err, &rerr) {
				assert.True(t, isCtxErr(rerr.First), "首次错误为门禁等待被 Close 取消: %v", rerr.First)
			} else {
				assert.True(t, err == errClientClosed, "原样返回门禁拒绝: %v", err)
			}
			ev := requireOneEvent(t, &log, op.name, err)
			assert.Zero(t, ev.Attempts, "未越过门禁，未发出查询")
			assert.Zero(t, op.calls(f), "不发查询 RPC")
			assert.EqualValues(t, 1, f.nConnect.Load(), "不重建")

			releaseWrite()
			require.NoError(t, recvWithin(t, closeDone, 2*time.Second, "写门禁释放后 Close 返回"))
			assert.True(t, c.gate.isClosing(), "客户端已进入关闭终态")
			assert.Error(t, c.rec.life.Err(), "恢复生命周期已终止")
			assert.Len(t, log.all(), 1, "Close 完成后不再产生事件")
		})

		t.Run(op.name+"/首次查询被Close截断", func(t *testing.T) {
			var log eventLog
			entered := make(chan struct{}, 1)
			f := &recFake{}
			op.set(f, blockFirstSearch(entered))
			c, _ := newRecClient(t, f, WithReadRecoveryObserver(log.add))
			closeDone := make(chan error, 1)
			go func() {
				<-entered
				closeDone <- c.Close()
			}()
			err := op.do(context.Background(), c)
			require.NoError(t, recvWithin(t, closeDone, 2*time.Second, "Close 返回"))
			var rerr *ReadRecoveryError
			require.True(t, errors.As(err, &rerr), "%v", err)
			assert.True(t, errors.Is(err, errClientClosed))
			ev := requireOneEvent(t, &log, op.name, err)
			assert.Equal(t, 1, ev.Attempts)
		})

		t.Run(op.name+"/Close后调用原样返回", func(t *testing.T) {
			var log eventLog
			f := &recFake{}
			c, _ := newRecClient(t, f, WithReadRecoveryObserver(log.add))
			require.NoError(t, c.Close())
			err := op.do(context.Background(), c)
			requireRaw(t, err)
			assert.True(t, errors.Is(err, errClientClosed))
			ev := requireOneEvent(t, &log, op.name, err)
			assert.Zero(t, ev.Attempts)
			assert.Zero(t, op.calls(f))
		})

		t.Run(op.name+"/恢复成功", func(t *testing.T) {
			var log eventLog
			f := &recFake{}
			op.set(f, failFirst(chanNotServiceable()))
			c, _ := newRecClient(t, f, WithReadRecoveryObserver(log.add))
			require.NoError(t, op.do(context.Background(), c))
			ev := requireOneEvent(t, &log, op.name, nil)
			assert.True(t, errors.Is(ev.FirstErr, merr.ErrChannelNotAvailable), "首次失败保留")
			assert.Equal(t, 2, ev.Attempts)
			assert.EqualValues(t, 2, f.nConnect.Load())
		})

		t.Run(op.name+"/恢复后不可恢复失败", func(t *testing.T) {
			var log eventLog
			f := &recFake{}
			op.set(f, func(_ context.Context, n int32) error {
				if n == 1 {
					return chanNotServiceable()
				}
				return permission
			})
			c, _ := newRecClient(t, f, WithReadRecoveryObserver(log.add))
			err := op.do(context.Background(), c)
			var rerr *ReadRecoveryError
			require.True(t, errors.As(err, &rerr), "%v", err)
			assert.Equal(t, codes.PermissionDenied, status.Code(err))
			ev := requireOneEvent(t, &log, op.name, err)
			assert.True(t, errors.Is(ev.FirstErr, merr.ErrChannelNotAvailable), "首次失败保留")
			assert.Equal(t, 2, ev.Attempts)
		})

		t.Run(op.name+"/恢复预算耗尽", func(t *testing.T) {
			var log eventLog
			f := &recFake{}
			op.set(f, always(chanNotServiceable()))
			c, _ := newRecClient(t, f, WithReadRecoveryObserver(log.add))
			c.rec.budget = 200 * time.Millisecond
			err := op.do(context.Background(), c)
			assert.True(t, errors.Is(err, ErrReadRecoveryExhausted), "%v", err)
			ev := requireOneEvent(t, &log, op.name, err)
			assert.True(t, errors.Is(ev.FirstErr, merr.ErrChannelNotAvailable), "首次失败保留")
			assert.GreaterOrEqual(t, ev.Attempts, 2)
			assert.LessOrEqual(t, int(op.calls(f)), ev.Attempts, "服务端收到的查询不多于计入的执行次数")
		})
	}

	t.Run("Search参数本地构建失败", func(t *testing.T) {
		var log eventLog
		f := &recFake{}
		c, _ := newRecClient(t, f, WithReadRecoveryObserver(log.add))
		_, err := c.Search(context.Background(), "c", nil, nil, []entity.Vector{entity.FloatVector([]float32{0, 0})}, "vec", entity.L2, 1, "", map[string]string{"nprobe": "NaN"})
		requireRaw(t, err)
		ev := requireOneEvent(t, &log, "Search", err)
		assert.Equal(t, 1, ev.Attempts, "已执行查询函数，计入一次")
		assert.Zero(t, f.nSearch.Load(), "但未发出 RPC")
	})
}

// 显式关闭恢复保持既有契约：设置 observer 也从不回调，错误原样返回、不重建
func TestReadRecoveryDisabledNeverObserves(t *testing.T) {
	for _, op := range readOps {
		t.Run(op.name, func(t *testing.T) {
			var log eventLog
			f := &recFake{}
			op.set(f, func(_ context.Context, n int32) error {
				switch n {
				case 1:
					return chanNotServiceable()
				case 2:
					return status.Error(codes.PermissionDenied, "denied")
				}
				return nil
			})
			c, _ := newRecClient(t, f, WithReadRecovery(false), WithReadRecoveryObserver(log.add))
			require.Nil(t, c.rec)

			err := op.do(context.Background(), c)
			requireRaw(t, err)
			assert.True(t, errors.Is(err, merr.ErrChannelNotAvailable))

			err = op.do(context.Background(), c)
			requireRaw(t, err)
			assert.Equal(t, codes.PermissionDenied, status.Code(err))

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err = op.do(ctx, c)
			assert.True(t, errors.Is(err, context.Canceled))

			require.NoError(t, op.do(context.Background(), c))
			assert.Empty(t, log.all(), "未启用恢复时从不回调")
			assert.EqualValues(t, 3, op.calls(f))
			assert.EqualValues(t, 1, f.nConnect.Load(), "未启用时不得重建")
		})
	}
}
