package main

import (
	"context"
	"log"
	"time"

	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/stones-hub/taurus-pro-milvus/pkg/milvus/client"
	"github.com/stones-hub/taurus-pro-milvus/pkg/milvus/schema"
)

func main() {
	log.Printf("开始测试 Milvus 连接...")

	// 创建客户端
	cli, err := client.New(
		client.WithAddress("192.168.103.113:19530"),
		client.WithTimeout(5*time.Second, 30*time.Second),
	)
	if err != nil {
		log.Fatalf("❌ 创建客户端失败: %v", err)
	}
	defer cli.Close()
	log.Printf("✅ 成功连接到 Milvus 服务器")

	collectionName := "test_collection"

	// 检查集合是否存在
	ctx := context.Background()
	exists, err := cli.HasCollection(ctx, collectionName)
	if err != nil {
		log.Fatalf("❌ 检查集合是否存在失败: %v", err)
	}
	if exists {
		log.Printf("集合已存在，正在删除...")
		err = cli.DropCollection(ctx, collectionName)
		if err != nil {
			log.Fatalf("❌ 删除集合失败: %v", err)
		}
		log.Printf("✅ 集合删除成功")
		time.Sleep(5 * time.Second)
	}

	log.Printf("开始创建测试集合: %s", collectionName)

	// 创建Schema
	builder := schema.NewBuilder(collectionName).
		WithDescription("Test collection for vector similarity search")

	// 添加字段
	builder.AddField(schema.NewIDField("id", entity.FieldTypeInt64, true))
	builder.AddField(schema.NewVectorField("vector", 128, entity.FieldTypeFloatVector))
	builder.AddField(schema.NewVarCharField("name", 100))
	builder.AddField(schema.NewInt64Field("age"))

	// 构建Schema
	sch, err := builder.Build()
	if err != nil {
		log.Fatalf("❌ 构建 Schema 失败: %v", err)
	}
	log.Printf("✅ Schema 构建成功")

	// 创建集合
	err = cli.CreateCollection(ctx, sch, 2)
	if err != nil {
		log.Fatalf("❌ 创建集合失败: %v", err)
	}
	log.Printf("✅ 集合创建成功")

	// 等待集合创建完成
	log.Printf("等待集合创建完成...")
	time.Sleep(5 * time.Second)

	// 创建索引
	log.Printf("开始创建索引...")
	indexParams, err := entity.NewIndexIvfFlat(
		entity.L2,
		1024,
	)
	if err != nil {
		log.Fatalf("❌ 创建索引参数失败: %v", err)
	}
	err = cli.CreateIndex(ctx, collectionName, "vector", indexParams, false)
	if err != nil {
		log.Fatalf("❌ 创建索引失败: %v", err)
	}
	log.Printf("✅ 索引创建成功")

	// 等待索引创建完成
	log.Printf("等待索引创建完成...")
	time.Sleep(5 * time.Second)

	// 加载集合
	log.Printf("开始加载集合...")
	err = cli.LoadCollection(ctx, collectionName, false)
	if err != nil {
		log.Fatalf("❌ 加载集合失败: %v", err)
	}
	log.Printf("✅ 集合加载成功")
	defer cli.ReleaseCollection(ctx, collectionName)

	// 准备测试数据
	log.Printf("开始准备测试数据...")
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

	log.Printf("开始插入数据...")
	_, err = cli.Insert(ctx, collectionName, "", columns...)
	if err != nil {
		log.Fatalf("❌ 插入数据失败: %v", err)
	}
	log.Printf("✅ 数据插入成功")

	// 等待数据生效
	log.Printf("等待数据生效...")
	time.Sleep(5 * time.Second)

	// 搜索数据
	log.Printf("开始搜索数据...")
	searchVectors := []entity.Vector{
		entity.FloatVector(vectors[0]),
	}

	searchParams, err := entity.NewIndexIvfFlatSearchParam(10)
	if err != nil {
		log.Fatalf("❌ 创建搜索参数失败: %v", err)
	}
	results, err := cli.Search(
		ctx,
		collectionName,
		nil,
		"",
		[]string{"name", "age"},
		searchVectors,
		"vector",
		entity.L2,
		3,
		searchParams,
	)
	if err != nil {
		log.Fatalf("❌ 搜索数据失败: %v", err)
	}
	log.Printf("✅ 搜索完成")

	// 打印搜索结果
	for i, result := range results {
		log.Printf("查询 %d 的搜索结果:", i)
		if ids, ok := result.IDs.(*entity.ColumnInt64); ok {
			for j, id := range ids.Data() {
				log.Printf("  ID: %v, 距离: %v", id, result.Scores[j])
			}
		}
	}

	// 查询数据
	log.Printf("开始查询数据...")
	expr := "age >= 25"
	outputFields := []string{"name", "age"}
	queryResults, err := cli.Query(ctx, collectionName, nil, expr, outputFields)
	if err != nil {
		log.Fatalf("❌ 查询数据失败: %v", err)
	}
	log.Printf("✅ 查询完成")

	// 打印查询结果
	log.Printf("查询结果 (age >= 25):")
	for _, col := range queryResults {
		log.Printf("字段 %s: %v", col.Name(), col)
	}

	log.Printf("✅ 所有测试完成")
}
