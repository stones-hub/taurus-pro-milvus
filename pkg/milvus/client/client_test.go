package client

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// 集成测试配置：MILVUS_INTEGRATION=1 显式开启，普通 go test 不连接任何服务器。
// 开启后必须提供 MILVUS_TEST_ADDRESS；MILVUS_TEST_USERNAME / MILVUS_TEST_PASSWORD 可选（未启用认证时留空）。
var testAddress, testUsername, testPassword string

// requireIntegration 未开启集成测试时跳过；开启但配置缺失则失败
func requireIntegration(t testing.TB) {
	t.Helper()
	if os.Getenv("MILVUS_INTEGRATION") != "1" {
		t.Skip("集成测试未启用，设置 MILVUS_INTEGRATION=1 及 MILVUS_TEST_ADDRESS 开启")
	}
	testAddress = os.Getenv("MILVUS_TEST_ADDRESS")
	if testAddress == "" {
		t.Fatal("MILVUS_INTEGRATION=1 但未设置 MILVUS_TEST_ADDRESS")
	}
	testUsername = os.Getenv("MILVUS_TEST_USERNAME")
	testPassword = os.Getenv("MILVUS_TEST_PASSWORD")
}

// 测试辅助函数：集成未启用时跳过，连接失败则失败
func createTestClient(t testing.TB) Client {
	requireIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := New(ctx, testAddress, testUsername, testPassword)
	if err != nil {
		t.Fatalf("无法连接到Milvus服务器 %s: %v", testAddress, err)
	}
	require.NotNil(t, client)
	// 先注册，LIFO 下最后执行，保证其他资源清理时连接仍可用
	t.Cleanup(func() { _ = client.Close() })

	return client
}

// registerCleanup 注册资源清理：使用独立的有界 context，不受测试自身 ctx 过期影响，清理失败会使测试失败
func registerCleanup(t testing.TB, what string, fn func(ctx context.Context) error) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := fn(ctx); err != nil {
			t.Errorf("清理%s失败: %v", what, err)
		}
	})
}

// registerDropCollection 注册集合清理，应在 CreateCollection 之前调用（删除不存在的集合不报错）
func registerDropCollection(t testing.TB, cli Client, name string) {
	t.Helper()
	registerCleanup(t, "集合 "+name, func(ctx context.Context) error { return cli.DropCollection(ctx, name) })
}

// flushCollection 将 growing segment 封存，使后续索引/搜索针对 sealed segment
func flushCollection(ctx context.Context, t testing.TB, cli Client, name string) {
	t.Helper()
	task, err := cli.GetClient().Flush(ctx, milvusclient.NewFlushOption(name))
	require.NoError(t, err)
	require.NoError(t, task.Await(ctx))
}

// requireDomainError 要求错误是服务端对请求本身的拒绝，且信息包含 wants 之一；
// 超时、取消、断连不能算作期望的失败
func requireDomainError(t testing.TB, err error, wants ...string) {
	t.Helper()
	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, context.Canceled)
	switch status.Code(err) {
	case codes.DeadlineExceeded, codes.Canceled, codes.Unavailable:
		t.Fatalf("期望领域错误，实际是传输层错误: %v", err)
	}
	msg := strings.ToLower(err.Error())
	for _, w := range wants {
		if strings.Contains(msg, strings.ToLower(w)) {
			return
		}
	}
	t.Fatalf("错误信息应包含 %v 之一，实际: %v", wants, err)
}

func generateRandomCollectionName() string {
	return fmt.Sprintf("test_collection_%d_%d", time.Now().UnixNano(), rand.Intn(10000))
}

func generateRandomDBName() string {
	return fmt.Sprintf("test_db_%d_%d", time.Now().UnixNano(), rand.Intn(10000))
}

func generateRandomAliasName() string {
	return fmt.Sprintf("test_alias_%d_%d", time.Now().UnixNano(), rand.Intn(10000))
}

func createTestSchema(collectionName string) *entity.Schema {
	return &entity.Schema{
		CollectionName: collectionName,
		Description:    "Test collection for unit testing",
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
					"max_length": "100",
				},
			},
		},
	}
}

func generateTestVectors(count int, dim int) [][]float32 {
	vectors := make([][]float32, count)
	for i := 0; i < count; i++ {
		vector := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vector[j] = rand.Float32()
		}
		vectors[i] = vector
	}
	return vectors
}

// TestNew 测试客户端创建
func TestNew(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("创建客户端成功", func(t *testing.T) {
		requireIntegration(t)
		client, err := New(ctx, testAddress, testUsername, testPassword)
		require.NoError(t, err)
		require.NotNil(t, client)
		defer client.Close()
	})

	t.Run("创建客户端失败-无效地址", func(t *testing.T) {
		// 不访问服务器：本机未监听的端口，在 ctx 超时内失败
		failCtx, failCancel := context.WithTimeout(ctx, time.Second)
		defer failCancel()
		client, err := New(failCtx, "127.0.0.1:1", "", "")
		assert.Error(t, err)
		assert.Nil(t, client)
	})
}

// TestNewWithOptions 测试使用选项创建客户端
func TestNewWithOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("使用默认选项创建客户端", func(t *testing.T) {
		requireIntegration(t)
		client, err := NewWithOptions(ctx,
			WithAddress(testAddress),
			WithAuth(testUsername, testPassword),
		)
		require.NoError(t, err)
		require.NotNil(t, client)
		defer client.Close()
	})

	t.Run("使用自定义选项创建客户端", func(t *testing.T) {
		admin := createTestClient(t)

		// 自行创建唯一数据库并在结束后清理
		dbName := generateRandomDBName()
		registerCleanup(t, "数据库 "+dbName, func(ctx context.Context) error { return admin.DropDatabase(ctx, dbName) })
		require.NoError(t, admin.CreateDatabase(ctx, dbName))

		client, err := NewWithOptions(ctx,
			WithAddress(testAddress),
			WithAuth(testUsername, testPassword),
			WithDatabase(dbName),
			WithRetry(5, 2*time.Second),
		)
		require.NoError(t, err)
		require.NotNil(t, client)
		defer client.Close()
	})
}

// TestGetClient 测试获取原始客户端
func TestGetClient(t *testing.T) {
	client := createTestClient(t)

	t.Run("获取原始客户端", func(t *testing.T) {
		rawClient := client.GetClient()
		assert.NotNil(t, rawClient)
	})

	t.Run("关闭后获取客户端返回nil", func(t *testing.T) {
		client.Close()
		rawClient := client.GetClient()
		assert.Nil(t, rawClient)
	})
}

// TestDatabaseOperations 测试数据库操作
func TestDatabaseOperations(t *testing.T) {
	client := createTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbName := generateRandomDBName()
	dbDropped := false
	registerCleanup(t, "数据库 "+dbName, func(ctx context.Context) error {
		if dbDropped {
			return nil
		}
		// 先切回 default，避免删除当前使用的库
		if err := client.UseDatabase(ctx, "default"); err != nil {
			return err
		}
		return client.DropDatabase(ctx, dbName)
	})

	t.Run("创建数据库", func(t *testing.T) {
		err := client.CreateDatabase(ctx, dbName)
		require.NoError(t, err)
		t.Logf("✅ 成功创建数据库: %s", dbName)
	})

	t.Run("切换数据库", func(t *testing.T) {
		err := client.UseDatabase(ctx, dbName)
		assert.NoError(t, err)
	})

	t.Run("删除数据库", func(t *testing.T) {
		err := client.DropDatabase(ctx, dbName)
		require.NoError(t, err)
		dbDropped = true
	})
}

// TestCollectionOperations 测试集合操作
func TestCollectionOperations(t *testing.T) {
	client := createTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	schema := createTestSchema(collectionName)
	registerDropCollection(t, client, collectionName) // 兜底：子测试中途失败也要清理

	t.Run("创建集合", func(t *testing.T) {
		err := client.CreateCollection(ctx, schema, 1)
		assert.NoError(t, err)
		t.Logf("✅ 成功创建集合: %s", collectionName)
	})

	t.Run("检查集合是否存在", func(t *testing.T) {
		exists, err := client.HasCollection(ctx, collectionName)
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("获取集合统计信息", func(t *testing.T) {
		stats, err := client.GetCollectionStatistics(ctx, collectionName)
		assert.NoError(t, err)
		assert.NotNil(t, stats)
	})

	t.Run("描述集合", func(t *testing.T) {
		collection, err := client.DescribeCollection(ctx, collectionName)
		assert.NoError(t, err)
		assert.NotNil(t, collection)
		assert.Equal(t, collectionName, collection.Name)
	})

	t.Run("加载集合", func(t *testing.T) {
		// 先创建索引才能加载集合
		idx := index.NewIvfFlatIndex(entity.L2, 1024)
		err := client.CreateIndex(ctx, collectionName, "vector", idx)
		require.NoError(t, err)

		err = client.LoadCollection(ctx, collectionName)
		assert.NoError(t, err)
	})

	t.Run("释放集合", func(t *testing.T) {
		err := client.ReleaseCollection(ctx, collectionName)
		assert.NoError(t, err)
	})

	t.Run("删除集合", func(t *testing.T) {
		err := client.DropCollection(ctx, collectionName)
		assert.NoError(t, err)
	})

	t.Run("检查已删除的集合", func(t *testing.T) {
		exists, err := client.HasCollection(ctx, collectionName)
		assert.NoError(t, err)
		assert.False(t, exists)
	})
}

// TestAliasOperations 测试别名操作
func TestAliasOperations(t *testing.T) {
	client := createTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 使用唯一的集合和别名，避免与已有资源冲突
	collectionName := generateRandomCollectionName()
	otherCollection := generateRandomCollectionName() + "_other"
	alias := generateRandomAliasName()

	registerDropCollection(t, client, collectionName)
	registerDropCollection(t, client, otherCollection)
	require.NoError(t, client.CreateCollection(ctx, createTestSchema(collectionName), 1))
	require.NoError(t, client.CreateCollection(ctx, createTestSchema(otherCollection), 1))

	aliasDropped := false
	// 别名需在集合之前删除：cleanup 为 LIFO，故最后注册
	registerCleanup(t, "别名 "+alias, func(ctx context.Context) error {
		if aliasDropped {
			return nil
		}
		return client.DropAlias(ctx, alias)
	})

	t.Run("创建别名", func(t *testing.T) {
		require.NoError(t, client.CreateAlias(ctx, collectionName, alias))
		desc, err := client.DescribeCollection(ctx, alias)
		require.NoError(t, err)
		assert.Equal(t, collectionName, desc.Name)
	})

	t.Run("修改别名", func(t *testing.T) {
		// AlterAlias 将已有别名重新指向另一个集合
		require.NoError(t, client.AlterAlias(ctx, otherCollection, alias))
		desc, err := client.DescribeCollection(ctx, alias)
		require.NoError(t, err)
		assert.Equal(t, otherCollection, desc.Name)
	})

	t.Run("删除别名", func(t *testing.T) {
		err := client.DropAlias(ctx, alias)
		require.NoError(t, err)
		aliasDropped = true
	})
}

// TestPartitionOperations 测试分区操作
func TestPartitionOperations(t *testing.T) {
	client := createTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	partitionName := "test_partition"
	schema := createTestSchema(collectionName)

	// 先创建集合
	err := client.CreateCollection(ctx, schema, 1)
	require.NoError(t, err)
	registerDropCollection(t, client, collectionName)

	t.Run("创建分区", func(t *testing.T) {
		err := client.CreatePartition(ctx, collectionName, partitionName)
		assert.NoError(t, err)
	})

	t.Run("检查分区是否存在", func(t *testing.T) {
		exists, err := client.HasPartition(ctx, collectionName, partitionName)
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("加载分区", func(t *testing.T) {
		// 先创建索引才能加载分区
		idx := index.NewIvfFlatIndex(entity.L2, 1024)
		err := client.CreateIndex(ctx, collectionName, "vector", idx)
		require.NoError(t, err)

		err = client.LoadPartitions(ctx, collectionName, []string{partitionName})
		assert.NoError(t, err)
	})

	t.Run("释放分区", func(t *testing.T) {
		err := client.ReleasePartitions(ctx, collectionName, []string{partitionName})
		assert.NoError(t, err)
	})

	t.Run("删除分区", func(t *testing.T) {
		err := client.DropPartition(ctx, collectionName, partitionName)
		assert.NoError(t, err)
	})

	t.Run("检查已删除的分区", func(t *testing.T) {
		exists, err := client.HasPartition(ctx, collectionName, partitionName)
		assert.NoError(t, err)
		assert.False(t, exists)
	})
}

// TestIndexOperations 测试索引操作
func TestIndexOperations(t *testing.T) {
	client := createTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	schema := createTestSchema(collectionName)

	// 先创建集合
	err := client.CreateCollection(ctx, schema, 1)
	require.NoError(t, err)
	registerDropCollection(t, client, collectionName)

	// 创建索引
	idx := index.NewIvfFlatIndex(entity.L2, 1024)

	t.Run("创建索引", func(t *testing.T) {
		err := client.CreateIndex(ctx, collectionName, "vector", idx)
		assert.NoError(t, err)
	})

	t.Run("删除索引", func(t *testing.T) {
		err := client.DropIndex(ctx, collectionName, "vector")
		assert.NoError(t, err)
	})
}

// TestDataOperations 测试数据操作
func TestDataOperations(t *testing.T) {
	client := createTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	schema := createTestSchema(collectionName)

	// 先创建集合
	err := client.CreateCollection(ctx, schema, 1)
	require.NoError(t, err)
	registerDropCollection(t, client, collectionName)

	// 先创建索引再加载集合
	idx := index.NewIvfFlatIndex(entity.L2, 1024)
	err = client.CreateIndex(ctx, collectionName, "vector", idx)
	require.NoError(t, err)

	err = client.LoadCollection(ctx, collectionName)
	require.NoError(t, err)

	t.Run("插入数据", func(t *testing.T) {
		// 准备测试数据
		vectorData := generateTestVectors(10, 128)
		textData := []string{"text1", "text2", "text3", "text4", "text5", "text6", "text7", "text8", "text9", "text10"}

		// 创建列数据
		vectorColumn := column.NewColumnFloatVector("vector", 128, vectorData)
		textColumn := column.NewColumnVarChar("text", textData)

		// 插入数据
		ids, err := client.Insert(ctx, collectionName, "", vectorColumn, textColumn)
		assert.NoError(t, err)
		assert.NotNil(t, ids)
		assert.Equal(t, 10, ids.Len())
	})

	t.Run("搜索数据", func(t *testing.T) {
		// 准备搜索向量
		searchVectorsData := generateTestVectors(1, 128)
		searchVectors := make([]entity.Vector, 1)
		searchVectors[0] = entity.FloatVector(searchVectorsData[0])

		// 执行搜索
		results, err := client.Search(ctx, collectionName, nil, []string{"text"}, searchVectors, "vector", entity.L2, 5, "", nil)
		assert.NoError(t, err)
		assert.NotNil(t, results)
		assert.Len(t, results, 1)
	})

	t.Run("带过滤条件的向量搜索", func(t *testing.T) {
		// 准备搜索向量
		searchVectorsData := generateTestVectors(1, 128)
		searchVectors := make([]entity.Vector, 1)
		searchVectors[0] = entity.FloatVector(searchVectorsData[0])

		// 带过滤条件的搜索
		results, err := client.Search(ctx, collectionName, nil, []string{"text"}, searchVectors, "vector", entity.L2, 5, "id > 0", nil)
		assert.NoError(t, err)
		assert.NotNil(t, results)
	})

	t.Run("查询数据", func(t *testing.T) {
		// 执行查询 - 使用有效的表达式
		columns, err := client.Query(ctx, collectionName, nil, "id > 0", []string{"text"})
		assert.NoError(t, err)
		assert.NotNil(t, columns)
		// 查询可能返回多个字段，包括id和text
		assert.GreaterOrEqual(t, len(columns), 1)
	})

	t.Run("更新向量数据", func(t *testing.T) {
		// 先插入一些数据
		vectorData := generateTestVectors(5, 128)
		textData := []string{"update1", "update2", "update3", "update4", "update5"}

		vectorColumn := column.NewColumnFloatVector("vector", 128, vectorData)
		textColumn := column.NewColumnVarChar("text", textData)

		ids, err := client.Insert(ctx, collectionName, "", vectorColumn, textColumn)
		assert.NoError(t, err)
		assert.Equal(t, 5, ids.Len())

		// 等待数据刷新
		time.Sleep(2 * time.Second)

		// 通过查询验证数据已插入
		columns, err := client.Query(ctx, collectionName, nil, "id > 0", []string{"text"})
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(columns), 1)

		// 删除特定数据（模拟更新）
		err = client.Delete(ctx, collectionName, "", "id > 0")
		assert.NoError(t, err)

		// 插入新数据（模拟更新后的数据）
		newVectorData := generateTestVectors(3, 128)
		newTextData := []string{"updated1", "updated2", "updated3"}

		newVectorColumn := column.NewColumnFloatVector("vector", 128, newVectorData)
		newTextColumn := column.NewColumnVarChar("text", newTextData)

		newIds, err := client.Insert(ctx, collectionName, "", newVectorColumn, newTextColumn)
		assert.NoError(t, err)
		assert.Equal(t, 3, newIds.Len())
	})

	t.Run("删除数据", func(t *testing.T) {
		// 删除所有数据
		err := client.Delete(ctx, collectionName, "", "id > 0")
		assert.NoError(t, err)
	})
}

// TestSearchVectorFieldMetricAndParams 验证双向量字段集合上 Search 的字段、metric 和参数契约
func TestSearchVectorFieldMetricAndParams(t *testing.T) {
	client := createTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const dim = 8
	collectionName := generateRandomCollectionName()
	schema := &entity.Schema{
		CollectionName: collectionName,
		Fields: []*entity.Field{
			{Name: "id", DataType: entity.FieldTypeInt64, PrimaryKey: true},
			{Name: "vec_a", DataType: entity.FieldTypeFloatVector, TypeParams: map[string]string{"dim": "8"}},
			{Name: "vec_b", DataType: entity.FieldTypeFloatVector, TypeParams: map[string]string{"dim": "8"}},
		},
	}
	registerDropCollection(t, client, collectionName)
	require.NoError(t, client.CreateCollection(ctx, schema, 1))

	// 第 i 行：vec_a 在第 i 维为 1，vec_b 在第 (i+1)%dim 维为 1（one-hot，两个字段的最近邻不同）
	const rows = dim
	ids := make([]int64, rows)
	aData := make([][]float32, rows)
	bData := make([][]float32, rows)
	for i := 0; i < rows; i++ {
		ids[i] = int64(i)
		aData[i] = make([]float32, dim)
		bData[i] = make([]float32, dim)
		aData[i][i] = 1
		bData[i][(i+1)%dim] = 1
	}
	_, err := client.Insert(ctx, collectionName, "",
		column.NewColumnInt64("id", ids),
		column.NewColumnFloatVector("vec_a", dim, aData),
		column.NewColumnFloatVector("vec_b", dim, bData),
	)
	require.NoError(t, err)

	flushCollection(ctx, t, client, collectionName)
	require.NoError(t, client.CreateIndex(ctx, collectionName, "vec_a", index.NewIvfFlatIndex(entity.L2, 4)))
	require.NoError(t, client.CreateIndex(ctx, collectionName, "vec_b", index.NewIvfFlatIndex(entity.L2, 4)))
	require.NoError(t, client.LoadCollection(ctx, collectionName))

	// 查询向量 e_3：vec_a 的最近邻是第 3 行，vec_b 的最近邻是第 2 行（(2+1)%8 == 3）
	query := make([]float32, dim)
	query[3] = 1
	vectors := []entity.Vector{entity.FloatVector(query)}
	params := map[string]string{"nprobe": "4"}

	topID := func(t *testing.T, field string) int64 {
		results, err := client.Search(ctx, collectionName, nil, nil, vectors, field, entity.L2, 1, "", params)
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, 1, results[0].ResultCount)
		return results[0].IDs.(*column.ColumnInt64).Data()[0]
	}

	t.Run("指定vec_a", func(t *testing.T) { assert.EqualValues(t, 3, topID(t, "vec_a")) })
	t.Run("指定vec_b", func(t *testing.T) { assert.EqualValues(t, 2, topID(t, "vec_b")) })

	t.Run("未指定字段时多向量字段集合由服务端报错", func(t *testing.T) {
		_, err := client.Search(ctx, collectionName, nil, nil, vectors, "", entity.L2, 1, "", params)
		requireDomainError(t, err, "anns_field")
	})

	t.Run("metric与索引不匹配由服务端报错", func(t *testing.T) {
		_, err := client.Search(ctx, collectionName, nil, nil, vectors, "vec_a", entity.IP, 1, "", params)
		requireDomainError(t, err, "metric")
	})

	t.Run("不存在的字段由服务端报错", func(t *testing.T) {
		_, err := client.Search(ctx, collectionName, nil, nil, vectors, "no_such_field", entity.L2, 1, "", params)
		requireDomainError(t, err, "no_such_field", "not found")
	})
}

// TestSearchMetricMatchesIndex 为每种 metric 构造匹配的索引并 flush 后搜索，
// 验证 metric 实际生效（自身距离/相似度），且与索引不匹配的 metric 被服务端拒绝
func TestSearchMetricMatchesIndex(t *testing.T) {
	client := createTestClient(t)

	const dim = 16
	metrics := []entity.MetricType{entity.L2, entity.IP, entity.COSINE}

	// 单位向量：自身检索时 L2 距离为 0，IP/COSINE 相似度为 1
	rng := rand.New(rand.NewSource(42))
	data := make([][]float32, 20)
	for i := range data {
		v := make([]float32, dim)
		var norm float64
		for j := range v {
			v[j] = rng.Float32() + 0.01
			norm += float64(v[j] * v[j])
		}
		for j := range v {
			v[j] /= float32(math.Sqrt(norm))
		}
		data[i] = v
	}
	query := []entity.Vector{entity.FloatVector(data[0])}

	for _, metric := range metrics {
		metric := metric
		t.Run(string(metric), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			collectionName := generateRandomCollectionName()
			schema := &entity.Schema{
				CollectionName: collectionName,
				Fields: []*entity.Field{
					{Name: "id", DataType: entity.FieldTypeInt64, PrimaryKey: true, AutoID: true},
					{Name: "vector", DataType: entity.FieldTypeFloatVector, TypeParams: map[string]string{"dim": "16"}},
				},
			}
			registerDropCollection(t, client, collectionName)
			require.NoError(t, client.CreateCollection(ctx, schema, 1))
			_, err := client.Insert(ctx, collectionName, "", column.NewColumnFloatVector("vector", dim, data))
			require.NoError(t, err)
			flushCollection(ctx, t, client, collectionName)
			require.NoError(t, client.CreateIndex(ctx, collectionName, "vector", index.NewIvfFlatIndex(metric, 4)))
			require.NoError(t, client.LoadCollection(ctx, collectionName))

			results, err := client.Search(ctx, collectionName, nil, nil, query, "vector", metric, 3, "", map[string]string{"nprobe": "4"})
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, 3, results[0].ResultCount)
			want := float32(1)
			if metric == entity.L2 {
				want = 0
			}
			assert.InDelta(t, want, results[0].Scores[0], 1e-3, "metric %s 的自身检索得分", metric)

			for _, other := range metrics {
				if other == metric {
					continue
				}
				_, err := client.Search(ctx, collectionName, nil, nil, query, "vector", other, 3, "", map[string]string{"nprobe": "4"})
				requireDomainError(t, err, "metric")
			}
		})
	}
}

// TestCompactOperation 测试压缩操作
func TestCompactOperation(t *testing.T) {
	client := createTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	schema := createTestSchema(collectionName)

	// 先创建集合
	err := client.CreateCollection(ctx, schema, 1)
	require.NoError(t, err)
	registerDropCollection(t, client, collectionName)

	t.Run("压缩集合", func(t *testing.T) {
		compactionID, err := client.Compact(ctx, collectionName)
		assert.NoError(t, err)
		// 压缩ID可能为-1，这是正常的
		assert.GreaterOrEqual(t, compactionID, int64(-1))
	})
}

// TestClientClosed 测试客户端关闭后的操作
func TestClientClosed(t *testing.T) {
	client := createTestClient(t)
	client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	schema := createTestSchema(collectionName)

	t.Run("关闭后创建集合应失败", func(t *testing.T) {
		err := client.CreateCollection(ctx, schema, 1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "client is closed")
	})

	t.Run("关闭后检查集合应失败", func(t *testing.T) {
		_, err := client.HasCollection(ctx, collectionName)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "client is closed")
	})

	t.Run("关闭后插入数据应失败", func(t *testing.T) {
		vectorData := generateTestVectors(1, 128)
		vectorColumn := column.NewColumnFloatVector("vector", 128, vectorData)
		_, err := client.Insert(ctx, collectionName, "", vectorColumn)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "client is closed")
	})
}

// TestConcurrentOperations 测试并发操作
func TestConcurrentOperations(t *testing.T) {
	client := createTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	schema := createTestSchema(collectionName)

	// 创建集合
	err := client.CreateCollection(ctx, schema, 1)
	require.NoError(t, err)
	registerDropCollection(t, client, collectionName)

	t.Run("并发检查集合", func(t *testing.T) {
		const numGoroutines = 10
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer func() { done <- true }()
				exists, err := client.HasCollection(ctx, collectionName)
				assert.NoError(t, err)
				assert.True(t, exists)
			}()
		}

		// 等待所有goroutine完成
		for i := 0; i < numGoroutines; i++ {
			<-done
		}
	})
}

// TestErrorHandling 测试错误处理
func TestErrorHandling(t *testing.T) {
	client := createTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("操作不存在的集合", func(t *testing.T) {
		nonExistentCollection := "non_existent_collection"

		// 检查不存在的集合
		exists, err := client.HasCollection(ctx, nonExistentCollection)
		assert.NoError(t, err)
		assert.False(t, exists)

		// 删除不存在的集合 - 可能不会返回错误
		err = client.DropCollection(ctx, nonExistentCollection)
		// 不强制要求错误，因为某些版本可能不返回错误

		// 获取不存在集合的统计信息
		_, err = client.GetCollectionStatistics(ctx, nonExistentCollection)
		// 不强制要求错误，因为某些版本可能不返回错误
	})

	t.Run("操作不存在的分区", func(t *testing.T) {
		collectionName := generateRandomCollectionName()
		schema := createTestSchema(collectionName)

		// 创建集合
		err := client.CreateCollection(ctx, schema, 1)
		require.NoError(t, err)
		registerDropCollection(t, client, collectionName)

		// 检查不存在的分区
		exists, err := client.HasPartition(ctx, collectionName, "non_existent_partition")
		assert.NoError(t, err)
		assert.False(t, exists)

		// 删除不存在的分区 - 可能不会返回错误
		err = client.DropPartition(ctx, collectionName, "non_existent_partition")
		// 不强制要求错误，因为某些版本可能不返回错误
	})
}

// TestDataPersistence 测试数据持久化（可选保留数据）
func TestDataPersistence(t *testing.T) {
	client := createTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	schema := createTestSchema(collectionName)

	// 创建集合；默认必须清理，仅 KEEP_TEST_DATA=true 显式保留
	if os.Getenv("KEEP_TEST_DATA") == "true" {
		t.Logf("🔒 将保留测试数据 - 集合: %s，请手动删除", collectionName)
	} else {
		registerDropCollection(t, client, collectionName)
	}
	err := client.CreateCollection(ctx, schema, 1)
	require.NoError(t, err)
	t.Logf("🔍 创建了测试集合: %s", collectionName)

	// 创建索引
	idx := index.NewIvfFlatIndex(entity.L2, 1024)
	err = client.CreateIndex(ctx, collectionName, "vector", idx)
	require.NoError(t, err)

	// 加载集合
	err = client.LoadCollection(ctx, collectionName)
	require.NoError(t, err)

	// 插入测试数据
	vectorData := generateTestVectors(5, 128)
	textData := []string{"persist1", "persist2", "persist3", "persist4", "persist5"}

	vectorColumn := column.NewColumnFloatVector("vector", 128, vectorData)
	textColumn := column.NewColumnVarChar("text", textData)

	ids, err := client.Insert(ctx, collectionName, "", vectorColumn, textColumn)
	require.NoError(t, err)
	t.Logf("🔍 插入了 %d 条向量数据", ids.Len())

	// 验证数据存在
	exists, err := client.HasCollection(ctx, collectionName)
	require.NoError(t, err)
	assert.True(t, exists)
	t.Logf("✅ 集合存在验证通过")

	// 查询数据验证
	columns, err := client.Query(ctx, collectionName, nil, "id > 0", []string{"text"})
	require.NoError(t, err)
	t.Logf("✅ 查询到 %d 个字段的数据", len(columns))

	// 搜索数据验证
	searchVectorsData := generateTestVectors(1, 128)
	searchVectors := make([]entity.Vector, 1)
	searchVectors[0] = entity.FloatVector(searchVectorsData[0])

	results, err := client.Search(ctx, collectionName, nil, []string{"text"}, searchVectors, "vector", entity.L2, 3, "", nil)
	require.NoError(t, err)
	t.Logf("✅ 搜索返回 %d 个结果", len(results))
}

// BenchmarkOperations 性能测试
func BenchmarkOperations(b *testing.B) {
	client := createTestClient(b)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	schema := createTestSchema(collectionName)

	// 创建集合
	err := client.CreateCollection(ctx, schema, 1)
	require.NoError(b, err)
	registerDropCollection(b, client, collectionName)

	// 创建索引后才能加载集合
	require.NoError(b, client.CreateIndex(ctx, collectionName, "vector", index.NewIvfFlatIndex(entity.L2, 1024)))
	err = client.LoadCollection(ctx, collectionName)
	require.NoError(b, err)

	b.Run("HasCollection", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := client.HasCollection(ctx, collectionName)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GetCollectionStatistics", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := client.GetCollectionStatistics(ctx, collectionName)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
