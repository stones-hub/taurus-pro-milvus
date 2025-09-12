package client

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/stretchr/testify/assert"
)

const (
	testAddress        = "192.168.103.113:19530"
	testCollectionName = "test_collection"
	testPartitionName  = "test_partition"
)

func setupTestClient(t *testing.T) Client {
	cli, err := New(
		WithAddress(testAddress),
		WithTimeout(5*time.Second, 30*time.Second),
	)
	assert.NoError(t, err)
	assert.NotNil(t, cli)
	return cli
}

func setupTestCollection(t *testing.T, cli Client) *entity.Schema {
	// 创建测试集合的 Schema
	schema := &entity.Schema{
		CollectionName: testCollectionName,
		Description:    "Test collection for unit tests",
		Fields: []*entity.Field{
			{
				Name:       "id",
				DataType:   entity.FieldTypeInt64,
				PrimaryKey: true,
				AutoID:     true,
			},
			{
				Name:     "vector",
				DataType: entity.FieldTypeFloatVector,
				TypeParams: map[string]string{
					"dim": "128",
				},
			},
			{
				Name:     "name",
				DataType: entity.FieldTypeVarChar,
				TypeParams: map[string]string{
					"max_length": "100",
				},
			},
			{
				Name:     "age",
				DataType: entity.FieldTypeInt64,
			},
		},
	}

	// 确保测试前集合不存在
	exists, err := cli.HasCollection(context.Background(), testCollectionName)
	assert.NoError(t, err)
	if exists {
		err = cli.DropCollection(context.Background(), testCollectionName)
		assert.NoError(t, err)
		time.Sleep(2 * time.Second)
	}

	return schema
}

func createAndLoadCollection(t *testing.T, cli Client, schema *entity.Schema) {
	// 创建集合
	err := cli.CreateCollection(context.Background(), schema, 2)
	assert.NoError(t, err)

	// 创建索引
	indexParams, err := entity.NewIndexIvfFlat(entity.L2, 1024)
	assert.NoError(t, err)
	err = cli.CreateIndex(context.Background(), testCollectionName, "vector", indexParams, false)
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)

	// 加载集合
	err = cli.LoadCollection(context.Background(), testCollectionName, false)
	assert.NoError(t, err)
}

func TestNew(t *testing.T) {
	cli := setupTestClient(t)
	defer cli.Close()
}

func TestCreateAndDropCollection(t *testing.T) {
	cli := setupTestClient(t)
	defer cli.Close()

	schema := setupTestCollection(t, cli)

	// 测试创建集合
	err := cli.CreateCollection(context.Background(), schema, 2)
	assert.NoError(t, err)

	// 验证集合是否存在
	exists, err := cli.HasCollection(context.Background(), testCollectionName)
	assert.NoError(t, err)
	assert.True(t, exists)

	// 测试删除集合
	err = cli.DropCollection(context.Background(), testCollectionName)
	assert.NoError(t, err)

	// 验证集合是否已删除
	exists, err = cli.HasCollection(context.Background(), testCollectionName)
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestLoadAndReleaseCollection(t *testing.T) {
	cli := setupTestClient(t)
	defer cli.Close()

	schema := setupTestCollection(t, cli)
	createAndLoadCollection(t, cli, schema)
	defer cli.DropCollection(context.Background(), testCollectionName)

	// 测试获取集合统计信息
	stats, err := cli.GetCollectionStatistics(context.Background(), testCollectionName)
	assert.NoError(t, err)
	assert.NotNil(t, stats)

	// 测试释放集合
	err = cli.ReleaseCollection(context.Background(), testCollectionName)
	assert.NoError(t, err)
}

func TestPartitionOperations(t *testing.T) {
	cli := setupTestClient(t)
	defer cli.Close()

	schema := setupTestCollection(t, cli)
	createAndLoadCollection(t, cli, schema)
	defer cli.DropCollection(context.Background(), testCollectionName)

	// 测试创建分区
	err := cli.CreatePartition(context.Background(), testCollectionName, testPartitionName)
	assert.NoError(t, err)

	// 验证分区是否存在
	exists, err := cli.HasPartition(context.Background(), testCollectionName, testPartitionName)
	assert.NoError(t, err)
	assert.True(t, exists)

	// 测试加载分区
	err = cli.LoadPartitions(context.Background(), testCollectionName, []string{testPartitionName}, false)
	assert.NoError(t, err)

	// 测试释放分区
	err = cli.ReleasePartitions(context.Background(), testCollectionName, []string{testPartitionName})
	assert.NoError(t, err)

	// 测试删除分区
	err = cli.DropPartition(context.Background(), testCollectionName, testPartitionName)
	assert.NoError(t, err)

	// 验证分区是否已删除
	exists, err = cli.HasPartition(context.Background(), testCollectionName, testPartitionName)
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestIndexOperations(t *testing.T) {
	cli := setupTestClient(t)
	defer cli.Close()

	schema := setupTestCollection(t, cli)

	// 创建集合
	err := cli.CreateCollection(context.Background(), schema, 2)
	assert.NoError(t, err)
	defer cli.DropCollection(context.Background(), testCollectionName)

	// 创建索引
	indexParams, err := entity.NewIndexIvfFlat(entity.L2, 1024)
	assert.NoError(t, err)

	err = cli.CreateIndex(context.Background(), testCollectionName, "vector", indexParams, false)
	assert.NoError(t, err)

	// 获取索引状态
	state, err := cli.GetIndexState(context.Background(), testCollectionName, "vector")
	assert.NoError(t, err)
	assert.NotEqual(t, entity.IndexState(0), state)

	// 删除索引
	err = cli.DropIndex(context.Background(), testCollectionName, "vector")
	assert.NoError(t, err)
}

func TestDataOperations(t *testing.T) {
	cli := setupTestClient(t)
	defer cli.Close()

	schema := setupTestCollection(t, cli)
	createAndLoadCollection(t, cli, schema)
	defer cli.DropCollection(context.Background(), testCollectionName)
	defer cli.ReleaseCollection(context.Background(), testCollectionName)

	// 准备测试数据
	vectors := make([][]float32, 2)
	for i := range vectors {
		vectors[i] = make([]float32, 128)
		for j := range vectors[i] {
			vectors[i][j] = float32(i*128 + j)
		}
	}

	names := []string{"张三", "李四"}
	ages := []int64{25, 30}

	// 插入数据
	columns := []entity.Column{
		entity.NewColumnVarChar("name", names),
		entity.NewColumnInt64("age", ages),
		entity.NewColumnFloatVector("vector", 128, vectors),
	}

	_, err := cli.Insert(context.Background(), testCollectionName, "", columns...)
	assert.NoError(t, err)

	time.Sleep(2 * time.Second) // 等待数据生效

	// 测试查询
	queryResults, err := cli.Query(
		context.Background(),
		testCollectionName,
		nil,
		"age >= 25",
		[]string{"name", "age", "vector"},
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, queryResults)

	// 测试向量搜索
	searchVectors := []entity.Vector{
		entity.FloatVector(vectors[0]),
	}
	searchParams, err := entity.NewIndexIvfFlatSearchParam(10)
	assert.NoError(t, err)

	searchResults, err := cli.Search(
		context.Background(),
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
	assert.NoError(t, err)
	assert.NotEmpty(t, searchResults)

	// 测试删除
	err = cli.Delete(context.Background(), testCollectionName, "", "age == 25")
	assert.NoError(t, err)

	time.Sleep(2 * time.Second) // 等待删除生效

	// 验证删除结果
	queryResults, err = cli.Query(
		context.Background(),
		testCollectionName,
		nil,
		"age == 25",
		[]string{"name", "age"},
	)
	assert.NoError(t, err)
	assert.Empty(t, queryResults[1].(*entity.ColumnInt64).Data()) // age 字段是第二个字段
}

func TestClientClose(t *testing.T) {
	cli := setupTestClient(t)

	// 测试正常关闭
	err := cli.Close()
	assert.NoError(t, err)

	// 测试重复关闭
	err = cli.Close()
	assert.NoError(t, err)

	// 测试关闭后的操作
	_, err = cli.HasCollection(context.Background(), testCollectionName)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client is closed")
}
