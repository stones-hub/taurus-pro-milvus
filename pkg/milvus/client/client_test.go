package client

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	testAddress        = "192.168.103.113:19530"
	testCollectionName = "test_collection"
	testPartitionName  = "test_partition"
	dimension          = 128
)

// ClientTestSuite 定义测试套件
type ClientTestSuite struct {
	suite.Suite
	cli    Client
	ctx    context.Context
	schema *entity.Schema
}

// SetupSuite 在所有测试开始前运行
func (s *ClientTestSuite) SetupSuite() {
	var err error
	s.ctx = context.Background()

	// 创建客户端
	s.cli, err = New(
		WithAddress(testAddress),
		WithAuth("root", ""),
		WithDatabase("default"),
		WithConnectTimeout(5*time.Second),
	)
	if err != nil {
		s.T().Skipf("无法连接到Milvus服务器: %v", err)
		return
	}

	// 准备测试Schema
	s.schema = &entity.Schema{
		CollectionName: testCollectionName,
		Description:    "Test collection for unit tests",
		Fields: []*entity.Field{
			{Name: "id", DataType: entity.FieldTypeInt64, PrimaryKey: true, AutoID: true},
			{Name: "vector", DataType: entity.FieldTypeFloatVector, TypeParams: map[string]string{"dim": "128"}},
			{Name: "name", DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "100"}},
			{Name: "age", DataType: entity.FieldTypeInt64},
		},
	}
}

// TearDownSuite 在所有测试结束后运行
func (s *ClientTestSuite) TearDownSuite() {
	if s.cli != nil {
		// 清理测试集合
		exists, err := s.cli.HasCollection(s.ctx, testCollectionName)
		if err == nil && exists {
			s.cli.DropCollection(s.ctx, testCollectionName)
		}
		s.cli.Close()
	}
}

// TestCollectionOperations 测试集合相关操作
func (s *ClientTestSuite) TestCollectionOperations() {
	// 确保测试前集合不存在
	exists, err := s.cli.HasCollection(s.ctx, testCollectionName)
	s.NoError(err, "检查集合存在应该成功")
	if exists {
		err = s.cli.DropCollection(s.ctx, testCollectionName)
		s.NoError(err, "删除已存在的集合应该成功")
		time.Sleep(2 * time.Second)
	}

	// 1. 创建集合
	err = s.cli.CreateCollection(s.ctx, s.schema, 2)
	s.NoError(err, "创建集合应该成功")

	// 2. 检查集合是否存在
	exists = false
	exists, err = s.cli.HasCollection(s.ctx, testCollectionName)
	s.NoError(err, "检查集合存在应该成功")
	s.True(exists, "集合应该存在")

	// 3. 获取集合统计信息
	stats, err := s.cli.GetCollectionStatistics(s.ctx, testCollectionName)
	s.NoError(err, "获取集合统计信息应该成功")
	s.Contains(stats, "row_count", "统计信息应该包含行数")

	// 4. 创建索引
	indexParams, err := entity.NewIndexIvfFlat(entity.L2, 1024)
	s.NoError(err, "创建索引参数应该成功")
	err = s.cli.CreateIndex(s.ctx, testCollectionName, "vector", indexParams, false)
	s.NoError(err, "创建索引应该成功")
	time.Sleep(2 * time.Second)

	// 5. 加载集合
	err = s.cli.LoadCollection(s.ctx, testCollectionName, false)
	s.NoError(err, "加载集合应该成功")

	// 5. 释放集合
	err = s.cli.ReleaseCollection(s.ctx, testCollectionName)
	s.NoError(err, "释放集合应该成功")

	// 6. 删除集合
	err = s.cli.DropCollection(s.ctx, testCollectionName)
	s.NoError(err, "删除集合应该成功")

	// 7. 验证集合已删除
	exists, err = s.cli.HasCollection(s.ctx, testCollectionName)
	s.NoError(err, "检查集合存在应该成功")
	s.False(exists, "集合应该已被删除")
}

// TestPartitionOperations 测试分区相关操作
func (s *ClientTestSuite) TestPartitionOperations() {
	// 确保测试前集合不存在
	exists, err := s.cli.HasCollection(s.ctx, testCollectionName)
	s.NoError(err, "检查集合存在应该成功")
	if exists {
		err = s.cli.DropCollection(s.ctx, testCollectionName)
		s.NoError(err, "删除已存在的集合应该成功")
		time.Sleep(2 * time.Second)
	}

	// 准备测试集合
	err = s.cli.CreateCollection(s.ctx, s.schema, 2)
	s.NoError(err, "创建集合应该成功")
	defer s.cli.DropCollection(s.ctx, testCollectionName)

	// 创建索引
	indexParams, err := entity.NewIndexIvfFlat(entity.L2, 1024)
	s.NoError(err, "创建索引参数应该成功")
	err = s.cli.CreateIndex(s.ctx, testCollectionName, "vector", indexParams, false)
	s.NoError(err, "创建索引应该成功")
	time.Sleep(2 * time.Second)

	// 1. 创建分区
	err = s.cli.CreatePartition(s.ctx, testCollectionName, testPartitionName)
	s.NoError(err, "创建分区应该成功")

	// 2. 检查分区是否存在
	exists = false
	exists, err = s.cli.HasPartition(s.ctx, testCollectionName, testPartitionName)
	s.NoError(err, "检查分区存在应该成功")
	s.True(exists, "分区应该存在")

	// 3. 加载分区
	err = s.cli.LoadPartitions(s.ctx, testCollectionName, []string{testPartitionName}, false)
	s.NoError(err, "加载分区应该成功")

	// 4. 释放分区
	err = s.cli.ReleasePartitions(s.ctx, testCollectionName, []string{testPartitionName})
	s.NoError(err, "释放分区应该成功")

	// 5. 删除分区
	err = s.cli.DropPartition(s.ctx, testCollectionName, testPartitionName)
	s.NoError(err, "删除分区应该成功")

	// 6. 验证分区已删除
	exists, err = s.cli.HasPartition(s.ctx, testCollectionName, testPartitionName)
	s.NoError(err, "检查分区存在应该成功")
	s.False(exists, "分区应该已被删除")
}

// TestIndexOperations 测试索引相关操作
func (s *ClientTestSuite) TestIndexOperations() {
	// 准备测试集合
	err := s.cli.CreateCollection(s.ctx, s.schema, 2)
	s.NoError(err, "创建集合应该成功")
	defer s.cli.DropCollection(s.ctx, testCollectionName)

	// 1. 创建索引
	indexParams, err := entity.NewIndexIvfFlat(entity.L2, 1024)
	s.NoError(err, "创建索引参数应该成功")

	err = s.cli.CreateIndex(s.ctx, testCollectionName, "vector", indexParams, false)
	s.NoError(err, "创建索引应该成功")

	// 2. 获取索引状态
	state, err := s.cli.GetIndexState(s.ctx, testCollectionName, "vector")
	s.NoError(err, "获取索引状态应该成功")
	s.NotEqual(entity.IndexState(0), state, "索引状态应该有效")

	// 3. 删除索引
	err = s.cli.DropIndex(s.ctx, testCollectionName, "vector")
	s.NoError(err, "删除索引应该成功")
}

// TestDataOperations 测试数据操作
func (s *ClientTestSuite) TestDataOperations() {
	// 准备测试集合
	err := s.cli.CreateCollection(s.ctx, s.schema, 2)
	s.NoError(err, "创建集合应该成功")
	defer s.cli.DropCollection(s.ctx, testCollectionName)

	// 创建并加载索引
	indexParams, err := entity.NewIndexIvfFlat(entity.L2, 1024)
	s.NoError(err, "创建索引参数应该成功")
	err = s.cli.CreateIndex(s.ctx, testCollectionName, "vector", indexParams, false)
	s.NoError(err, "创建索引应该成功")
	err = s.cli.LoadCollection(s.ctx, testCollectionName, false)
	s.NoError(err, "加载集合应该成功")

	// 1. 插入数据
	vectors := make([][]float32, 2)
	for i := range vectors {
		vectors[i] = make([]float32, dimension)
		for j := range vectors[i] {
			vectors[i][j] = float32(i*dimension + j)
		}
	}

	names := []string{"张三", "李四"}
	ages := []int64{25, 30}

	columns := []entity.Column{
		entity.NewColumnVarChar("name", names),
		entity.NewColumnInt64("age", ages),
		entity.NewColumnFloatVector("vector", dimension, vectors),
	}

	_, err = s.cli.Insert(s.ctx, testCollectionName, "", columns...)
	s.NoError(err, "插入数据应该成功")

	time.Sleep(2 * time.Second) // 等待数据生效

	// 2. 条件查询
	queryResults, err := s.cli.Query(
		s.ctx,
		testCollectionName,
		nil,
		"age >= 25",
		[]string{"name", "age"},
	)
	s.NoError(err, "查询数据应该成功")
	s.NotEmpty(queryResults, "查询结果不应为空")

	// 3. 向量搜索
	searchVectors := []entity.Vector{
		entity.FloatVector(vectors[0]),
	}
	searchParams, err := entity.NewIndexIvfFlatSearchParam(10)
	s.NoError(err, "创建搜索参数应该成功")

	searchResults, err := s.cli.Search(
		s.ctx,
		testCollectionName,
		nil,
		"",
		[]string{"name", "age"},
		searchVectors,
		"vector",
		entity.L2,
		3,
		searchParams,
	)
	s.NoError(err, "向量搜索应该成功")
	s.NotEmpty(searchResults, "搜索结果不应为空")

	// 4. 删除数据
	err = s.cli.Delete(s.ctx, testCollectionName, "", "age == 25")
	s.NoError(err, "删除数据应该成功")

	time.Sleep(2 * time.Second) // 等待删除生效

	// 5. 验证删除结果
	queryResults, err = s.cli.Query(
		s.ctx,
		testCollectionName,
		nil,
		"age == 25",
		[]string{"name", "age"},
	)
	s.NoError(err, "查询删除结果应该成功")
	s.Empty(queryResults[1].(*entity.ColumnInt64).Data(), "删除的数据应该不存在")
}

// TestOptionsConfiguration 测试配置选项
func (s *ClientTestSuite) TestOptionsConfiguration() {
	tests := []struct {
		name    string
		options []Option
		check   func(*testing.T, *Options)
	}{
		{
			name: "default options",
			options: []Option{
				WithAddress(testAddress),
			},
			check: func(t *testing.T, opts *Options) {
				assert.Equal(t, testAddress, opts.Address)
				assert.Equal(t, 30*time.Second, opts.ConnectTimeout)
				assert.Equal(t, uint(75), opts.MaxRetry)
				assert.Equal(t, 3*time.Second, opts.MaxRetryBackoff)
				assert.Equal(t, 5*time.Second, opts.KeepAliveTime)
				assert.Equal(t, 10*time.Second, opts.KeepAliveTimeout)
			},
		},
		{
			name: "auth with username/password",
			options: []Option{
				WithAddress(testAddress),
				WithAuth("user", "pass"),
			},
			check: func(t *testing.T, opts *Options) {
				assert.Equal(t, "user", opts.Username)
				assert.Equal(t, "pass", opts.Password)
				assert.Empty(t, opts.APIKey)
			},
		},
		{
			name: "auth with api key",
			options: []Option{
				WithAddress(testAddress),
				WithAPIKey("test-api-key"),
			},
			check: func(t *testing.T, opts *Options) {
				assert.Equal(t, "test-api-key", opts.APIKey)
				assert.Empty(t, opts.Username)
				assert.Empty(t, opts.Password)
			},
		},
		{
			name: "database and identifier",
			options: []Option{
				WithAddress(testAddress),
				WithDatabase("test_db"),
				WithIdentifier("test_client"),
			},
			check: func(t *testing.T, opts *Options) {
				assert.Equal(t, "test_db", opts.DBName)
				assert.Equal(t, "test_client", opts.Identifier)
			},
		},
		{
			name: "connection timeouts",
			options: []Option{
				WithAddress(testAddress),
				WithConnectTimeout(5 * time.Second),
			},
			check: func(t *testing.T, opts *Options) {
				assert.Equal(t, 5*time.Second, opts.ConnectTimeout)
			},
		},
		{
			name: "retry configuration",
			options: []Option{
				WithAddress(testAddress),
				WithRetry(100, 5*time.Second),
			},
			check: func(t *testing.T, opts *Options) {
				assert.Equal(t, uint(100), opts.MaxRetry)
				assert.Equal(t, 5*time.Second, opts.MaxRetryBackoff)
			},
		},
		{
			name: "keepalive configuration",
			options: []Option{
				WithAddress(testAddress),
				WithKeepAlive(15*time.Second, 30*time.Second),
			},
			check: func(t *testing.T, opts *Options) {
				assert.Equal(t, 15*time.Second, opts.KeepAliveTime)
				assert.Equal(t, 30*time.Second, opts.KeepAliveTimeout)
			},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			opts := DefaultOptions()
			for _, opt := range tt.options {
				opt(opts)
			}
			tt.check(s.T(), opts)
		})
	}
}

// TestConnectionTimeout 测试连接超时
func (s *ClientTestSuite) TestConnectionTimeout() {
	_, err := New(
		WithAddress("non-existent-host:19530"),
		WithConnectTimeout(1*time.Second),
	)
	s.Error(err, "连接不存在的主机应该超时")
	s.Contains(err.Error(), "deadline exceeded")
}

// TestClientClose 测试客户端关闭
func (s *ClientTestSuite) TestClientClose() {
	cli, err := New(
		WithAddress(testAddress),
		WithAuth("root", ""),
		WithConnectTimeout(1*time.Second),
	)
	if err != nil {
		s.T().Skipf("跳过测试，连接错误: %v", err)
		return
	}

	// 测试正常关闭
	err = cli.Close()
	s.NoError(err, "关闭客户端应该成功")

	// 测试重复关闭
	err = cli.Close()
	s.NoError(err, "重复关闭客户端应该成功")

	// 测试关闭后的操作
	_, err = cli.HasCollection(context.Background(), testCollectionName)
	s.Error(err, "使用已关闭的客户端应该返回错误")
	s.Contains(err.Error(), "client is closed")
}

// TestClientSuite 运行测试套件
func TestClientSuite(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}
