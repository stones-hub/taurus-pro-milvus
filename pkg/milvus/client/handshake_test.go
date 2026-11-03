package client

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// 本文件使用本机 fake gRPC 服务器，不依赖 Milvus。

type trackedConn struct {
	net.Conn
	once   sync.Once
	closed *atomic.Int32
}

func (c *trackedConn) Close() error {
	c.once.Do(func() { c.closed.Add(1) })
	return c.Conn.Close()
}

// trackedListener 统计服务端侧已接受/已关闭的连接数；客户端关闭连接后服务端会关闭对应 net.Conn
type trackedListener struct {
	net.Listener
	accepted atomic.Int32
	closed   atomic.Int32
}

func (l *trackedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.accepted.Add(1)
	return &trackedConn{Conn: c, closed: &l.closed}, nil
}

func (l *trackedListener) open() int32 { return l.accepted.Load() - l.closed.Load() }

type fakeMilvus struct {
	milvuspb.UnimplementedMilvusServiceServer
	connect func(ctx context.Context) (*milvuspb.ConnectResponse, error)
	auth    atomic.Value // 最近一次 Connect 的 authorization 头
}

func (f *fakeMilvus) Connect(ctx context.Context, _ *milvuspb.ConnectRequest) (*milvuspb.ConnectResponse, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("authorization"); len(v) > 0 {
			f.auth.Store(v[0])
		}
	}
	return f.connect(ctx)
}

func startFakeMilvus(t *testing.T, f *fakeMilvus) (string, *trackedListener) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tl := &trackedListener{Listener: lis}
	srv := grpc.NewServer()
	milvuspb.RegisterMilvusServiceServer(srv, f)
	go func() { _ = srv.Serve(tl) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), tl
}

func okConnect(context.Context) (*milvuspb.ConnectResponse, error) {
	return &milvuspb.ConnectResponse{Status: &commonpb.Status{}, Identifier: 1}, nil
}

func waitOpen(t *testing.T, tl *trackedListener, want int32) {
	t.Helper()
	require.Eventually(t, func() bool { return tl.open() == want }, 3*time.Second, 10*time.Millisecond,
		"open connections: got %d want %d", tl.open(), want)
}

func TestNewReleasesConnectionWhenHandshakeFails(t *testing.T) {
	cases := map[string]func(context.Context) (*milvuspb.ConnectResponse, error){
		"rpc错误": func(context.Context) (*milvuspb.ConnectResponse, error) {
			return nil, status.Error(codes.PermissionDenied, "denied")
		},
		"业务错误状态": func(context.Context) (*milvuspb.ConnectResponse, error) {
			return &milvuspb.ConnectResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Code: 65535, Reason: "database not found"}}, nil
		},
	}
	for name, connect := range cases {
		t.Run(name, func(t *testing.T) {
			addr, tl := startFakeMilvus(t, &fakeMilvus{connect: connect})

			for i := 0; i < 5; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				cli, err := New(ctx, addr, "root", "pw")
				cancel()
				require.Error(t, err)
				assert.Nil(t, cli)
			}
			assert.EqualValues(t, 5, tl.accepted.Load())
			waitOpen(t, tl, 0)
		})
	}
}

func TestNewReleasesConnectionWhenHandshakeTimesOut(t *testing.T) {
	addr, tl := startFakeMilvus(t, &fakeMilvus{connect: func(ctx context.Context) (*milvuspb.ConnectResponse, error) {
		<-ctx.Done() // 永不响应，直到客户端断开
		return nil, ctx.Err()
	}})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	cli, err := New(ctx, addr, "", "")
	require.Error(t, err)
	assert.Nil(t, cli)
	waitOpen(t, tl, 0)
}

func TestNewKeepsConnectionAfterSuccessfulHandshake(t *testing.T) {
	f := &fakeMilvus{connect: okConnect}
	addr, tl := startFakeMilvus(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := New(ctx, addr, "root", "pw")
	require.NoError(t, err)
	require.NotNil(t, cli)

	// 握手成功后不得误关连接；认证头经 SDK 拦截器链正常到达服务端
	time.Sleep(200 * time.Millisecond)
	assert.EqualValues(t, 1, tl.open())
	assert.NotNil(t, cli.GetClient())
	assert.NotEmpty(t, f.auth.Load(), "认证头应随握手请求发送")

	// 后续 RPC 仍走同一连接并正常工作（fake 未实现的方法返回 Unimplemented，说明连接可用）
	_, err = cli.HasCollection(ctx, "x")
	require.Error(t, err)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
	assert.EqualValues(t, 1, tl.open())

	require.NoError(t, cli.Close())
	waitOpen(t, tl, 0)
}

func TestNewUnimplementedConnectIsTolerated(t *testing.T) {
	// 旧版服务端未实现 Connect，SDK 视为成功，连接必须保留
	addr, tl := startFakeMilvus(t, &fakeMilvus{connect: func(context.Context) (*milvuspb.ConnectResponse, error) {
		return nil, status.Error(codes.Unimplemented, "no connect")
	}})
	cli, err := New(context.Background(), addr, "", "")
	require.NoError(t, err)
	defer cli.Close()
	time.Sleep(100 * time.Millisecond)
	assert.EqualValues(t, 1, tl.open())
}

func TestConnCaptureDoesNotRetainAfterRelease(t *testing.T) {
	c := &connCapture{active: true}
	cc := &grpc.ClientConn{}
	_ = c.interceptor(context.Background(), "m", nil, nil, cc, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return nil })
	assert.Same(t, cc, c.cc)
	c.release(false)
	assert.Nil(t, c.cc)

	// 释放后的调用不再捕获
	_ = c.interceptor(context.Background(), "m", nil, nil, cc, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return nil })
	assert.Nil(t, c.cc)
}
