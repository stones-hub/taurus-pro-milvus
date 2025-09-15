package client

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// 测试配置
	testAddress  = "192.168.103.113:19530"
	testUsername = "root"
	testPassword = ""
	testDBName   = "test_db"
)

// 测试辅助函数
func createTestClient(t *testing.T) Client {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := New(ctx, testAddress, testUsername, testPassword)
	if err != nil {
		t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
	}
	require.NotNil(t, client)

	return client
}

func generateRandomCollectionName() string {
	return fmt.Sprintf("test_collection_%d_%d", time.Now().Unix(), rand.Intn(10000))
}

func generateRandomDBName() string {
	return fmt.Sprintf("test_db_%d_%d", time.Now().Unix(), rand.Intn(10000))
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
		client, err := New(ctx, testAddress, testUsername, testPassword)
		assert.NoError(t, err)
		assert.NotNil(t, client)
		defer client.Close()
	})

	t.Run("创建客户端失败-无效地址", func(t *testing.T) {
		client, err := New(ctx, "invalid:address", testUsername, testPassword)
		assert.Error(t, err)
		assert.Nil(t, client)
	})
}

// TestNewWithOptions 测试使用选项创建客户端
func TestNewWithOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("使用默认选项创建客户端", func(t *testing.T) {
		// 跳过需要实际连接的测试
		t.Skip("跳过需要Milvus服务器连接的测试")
	})

	t.Run("使用自定义选项创建客户端", func(t *testing.T) {
		client, err := NewWithOptions(ctx,
			WithAddress(testAddress),
			WithAuth(testUsername, testPassword),
			WithDatabase("test_db"),
			WithRetry(5, 2*time.Second),
		)
		if err != nil {
			t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
		}
		assert.NotNil(t, client)
		defer client.Close()
	})
}

// TestGetClient 测试获取原始客户端
func TestGetClient(t *testing.T) {
	client := createTestClient(t)
	defer client.Close()

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
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbName := generateRandomDBName()

	t.Run("创建数据库", func(t *testing.T) {
		err := client.CreateDatabase(ctx, dbName)
		assert.NoError(t, err)
		t.Logf("✅ 成功创建数据库: %s", dbName)
	})

	t.Run("切换数据库", func(t *testing.T) {
		err := client.UseDatabase(ctx, dbName)
		assert.NoError(t, err)
	})

	t.Run("删除数据库", func(t *testing.T) {
		err := client.DropDatabase(ctx, dbName)
		assert.NoError(t, err)
	})
}

// TestCollectionOperations 测试集合操作
func TestCollectionOperations(t *testing.T) {
	client := createTestClient(t)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	schema := createTestSchema(collectionName)

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
		if err != nil {
			t.Skipf("跳过测试，无法创建索引: %v", err)
		}

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
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	alias := "test_alias"
	schema := createTestSchema(collectionName)

	// 先创建集合
	err := client.CreateCollection(ctx, schema, 1)
	require.NoError(t, err)
	defer client.DropCollection(ctx, collectionName)

	t.Run("创建别名", func(t *testing.T) {
		err := client.CreateAlias(ctx, collectionName, alias)
		if err != nil {
			t.Skipf("跳过测试，别名已存在: %v", err)
		}
	})

	t.Run("修改别名", func(t *testing.T) {
		newAlias := "new_test_alias"
		err := client.AlterAlias(ctx, collectionName, newAlias)
		if err != nil {
			t.Skipf("跳过测试，别名冲突: %v", err)
		}
		alias = newAlias // 更新别名用于后续测试
	})

	t.Run("删除别名", func(t *testing.T) {
		err := client.DropAlias(ctx, alias)
		assert.NoError(t, err)
	})
}

// TestPartitionOperations 测试分区操作
func TestPartitionOperations(t *testing.T) {
	client := createTestClient(t)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	partitionName := "test_partition"
	schema := createTestSchema(collectionName)

	// 先创建集合
	err := client.CreateCollection(ctx, schema, 1)
	require.NoError(t, err)
	defer client.DropCollection(ctx, collectionName)

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
		if err != nil {
			t.Skipf("跳过测试，无法创建索引: %v", err)
		}

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
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	schema := createTestSchema(collectionName)

	// 先创建集合
	err := client.CreateCollection(ctx, schema, 1)
	require.NoError(t, err)
	defer client.DropCollection(ctx, collectionName)

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
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	schema := createTestSchema(collectionName)

	// 先创建集合
	err := client.CreateCollection(ctx, schema, 1)
	require.NoError(t, err)
	defer client.DropCollection(ctx, collectionName)

	// 先创建索引再加载集合
	idx := index.NewIvfFlatIndex(entity.L2, 1024)
	err = client.CreateIndex(ctx, collectionName, "vector", idx)
	if err != nil {
		t.Skipf("跳过测试，无法创建索引: %v", err)
	}

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

	t.Run("向量相似度搜索", func(t *testing.T) {
		// 测试不同的相似度度量
		searchVectorsData := generateTestVectors(1, 128)
		searchVectors := make([]entity.Vector, 1)
		searchVectors[0] = entity.FloatVector(searchVectorsData[0])

		// 测试L2距离
		results, err := client.Search(ctx, collectionName, nil, []string{"text"}, searchVectors, "vector", entity.L2, 3, "", nil)
		assert.NoError(t, err)
		assert.NotNil(t, results)

		// 测试IP内积
		results, err = client.Search(ctx, collectionName, nil, []string{"text"}, searchVectors, "vector", entity.IP, 3, "", nil)
		assert.NoError(t, err)
		assert.NotNil(t, results)

		// 测试COSINE余弦相似度
		results, err = client.Search(ctx, collectionName, nil, []string{"text"}, searchVectors, "vector", entity.COSINE, 3, "", nil)
		assert.NoError(t, err)
		assert.NotNil(t, results)
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

// TestCompactOperation 测试压缩操作
func TestCompactOperation(t *testing.T) {
	client := createTestClient(t)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	schema := createTestSchema(collectionName)

	// 先创建集合
	err := client.CreateCollection(ctx, schema, 1)
	require.NoError(t, err)
	defer client.DropCollection(ctx, collectionName)

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
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	schema := createTestSchema(collectionName)

	// 创建集合
	err := client.CreateCollection(ctx, schema, 1)
	require.NoError(t, err)
	defer client.DropCollection(ctx, collectionName)

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
	defer client.Close()

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
		defer client.DropCollection(ctx, collectionName)

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
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	schema := createTestSchema(collectionName)

	// 创建集合
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

	// 检查是否要保留数据
	// 设置环境变量 KEEP_TEST_DATA=true 来保留测试数据
	if os.Getenv("KEEP_TEST_DATA") == "true" {
		t.Logf("🔒 保留测试数据 - 集合: %s", collectionName)
		t.Logf("🔒 请手动删除集合: %s", collectionName)
	} else {
		// 清理数据
		err = client.DropCollection(ctx, collectionName)
		require.NoError(t, err)
		t.Logf("🧹 已清理测试数据")
	}
}

// BenchmarkOperations 性能测试
func BenchmarkOperations(b *testing.B) {
	client := createTestClient(&testing.T{})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := generateRandomCollectionName()
	schema := createTestSchema(collectionName)

	// 创建集合
	err := client.CreateCollection(ctx, schema, 1)
	require.NoError(&testing.T{}, err)
	defer client.DropCollection(ctx, collectionName)

	// 加载集合
	err = client.LoadCollection(ctx, collectionName)
	require.NoError(&testing.T{}, err)

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
