package milvus

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/stones-hub/taurus-pro-milvus/pkg/milvus/client"
)

// 本文件使用本机 fake gRPC 服务器与池的真实默认工厂（client.NewWithOptions），不依赖 Milvus。

// recoverFake 首次 Search 返回真实重启后的 merr 503，之后成功
type recoverFake struct {
	milvuspb.UnimplementedMilvusServiceServer
	nConnect, nSearch atomic.Int32
}

func (f *recoverFake) Connect(context.Context, *milvuspb.ConnectRequest) (*milvuspb.ConnectResponse, error) {
	n := f.nConnect.Add(1)
	return &milvuspb.ConnectResponse{Status: &commonpb.Status{}, Identifier: int64(n)}, nil
}

func (f *recoverFake) DescribeCollection(_ context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
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

func (f *recoverFake) GetLoadState(context.Context, *milvuspb.GetLoadStateRequest) (*milvuspb.GetLoadStateResponse, error) {
	return &milvuspb.GetLoadStateResponse{Status: &commonpb.Status{}, State: commonpb.LoadState_LoadStateLoaded}, nil
}

func (f *recoverFake) Search(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	if f.nSearch.Add(1) == 1 {
		err := merr.WrapErrChannelNotAvailable("by-dev-rootcoord-dml_0_1v0", "channel distribution is not serviceable")
		return &milvuspb.SearchResults{Status: merr.Status(err)}, nil
	}
	return &milvuspb.SearchResults{Status: &commonpb.Status{}, Results: &schemapb.SearchResultData{
		NumQueries: 1, TopK: 1, Topks: []int64{1}, Scores: []float32{0},
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{42}}}},
	}}, nil
}

// 池默认创建的客户端（未传 WithReadRecovery）遇可恢复错误时按默认间隔重建并重试成功；
// 包外无法缩短间隔，本测试按真实 ReadRecoveryInterval（5 秒）等待一次
func TestPoolDefaultClientRecoversRead(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f := &recoverFake{}
	srv := grpc.NewServer()
	milvuspb.RegisterMilvusServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	p := NewPool()
	t.Cleanup(func() { _ = p.Close() })
	cli, err := p.MustGet("default", client.WithAddress(lis.Addr().String()))
	require.NoError(t, err)
	old := cli.GetClient()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	results, err := cli.Search(ctx, "c", nil, nil, []entity.Vector{entity.FloatVector([]float32{0, 0})}, "vec", entity.L2, 1, "", nil)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, 1, results[0].ResultCount)
	id, err := results[0].IDs.GetAsInt64(0)
	require.NoError(t, err)
	assert.EqualValues(t, 42, id)

	assert.EqualValues(t, 2, f.nSearch.Load())
	assert.EqualValues(t, 2, f.nConnect.Load(), "应重建一次")
	assert.NotSame(t, old, cli.GetClient(), "底层客户端已替换")
	assert.GreaterOrEqual(t, elapsed, client.ReadRecoveryInterval, "按默认恢复间隔等待")
	assert.Less(t, elapsed, client.ReadRecoveryBudget)

	// 池中的客户端对象不变，恢复后继续可用
	got, err := p.Get("default")
	require.NoError(t, err)
	assert.Same(t, cli, got)
}
