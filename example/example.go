package main

import (
	"context"
	"fmt"
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

	// 查询所有数据
	log.Printf("开始查询所有数据...")
	allResults, err := cli.Query(ctx, collectionName, nil, "age > 0", []string{"name", "age", "vector"})
	if err != nil {
		log.Fatalf("❌ 查询数据失败: %v", err)
	}
	log.Printf("✅ 查询完成")

	// 打印所有数据
	log.Printf("所有数据:")
	for _, col := range allResults {
		switch col.Name() {
		case "name":
			if nameCol, ok := col.(*entity.ColumnVarChar); ok {
				log.Printf("  名字: %v", nameCol.Data())
			}
		case "age":
			if ageCol, ok := col.(*entity.ColumnInt64); ok {
				log.Printf("  年龄: %v", ageCol.Data())
			}
		case "vector":
			if vecCol, ok := col.(*entity.ColumnFloatVector); ok {
				log.Printf("  向量数据:")
				for i, vec := range vecCol.Data() {
					log.Printf("    向量 %d: [%v]", i, formatVector(vec))
				}
			}
		}
	}

	// 搜索数据
	log.Printf("\n开始相似度搜索...")
	searchVectors := []entity.Vector{
		entity.FloatVector(vectors[0]), // 使用第一个向量进行搜索
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
		[]string{"name", "age", "vector"},
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
		log.Printf("\n查询向量 %d 的搜索结果:", i)
		log.Printf("  查询向量: [%v]", formatVector(vectors[0]))
		if ids, ok := result.IDs.(*entity.ColumnInt64); ok {
			for j, id := range ids.Data() {
				log.Printf("  匹配结果 %d:", j+1)
				log.Printf("    ID: %v", id)
				log.Printf("    距离: %v", result.Scores[j])
				// 查询匹配向量的详细信息
				matchResults, err := cli.Query(ctx, collectionName, nil, fmt.Sprintf("id == %d", id), []string{"name", "age", "vector"})
				if err != nil {
					log.Printf("    ❌ 查询匹配向量详情失败: %v", err)
					continue
				}
				for _, col := range matchResults {
					switch col.Name() {
					case "name":
						if nameCol, ok := col.(*entity.ColumnVarChar); ok && len(nameCol.Data()) > 0 {
							log.Printf("    名字: %v", nameCol.Data()[0])
						}
					case "age":
						if ageCol, ok := col.(*entity.ColumnInt64); ok && len(ageCol.Data()) > 0 {
							log.Printf("    年龄: %v", ageCol.Data()[0])
						}
					case "vector":
						if vecCol, ok := col.(*entity.ColumnFloatVector); ok && len(vecCol.Data()) > 0 {
							log.Printf("    向量: [%v]", formatVector(vecCol.Data()[0]))
						}
					}
				}
			}
		}
	}

	// 条件查询
	log.Printf("\n开始条件查询...")
	expr := "age >= 25"
	outputFields := []string{"name", "age", "vector"}
	queryResults, err := cli.Query(ctx, collectionName, nil, expr, outputFields)
	if err != nil {
		log.Fatalf("❌ 查询数据失败: %v", err)
	}
	log.Printf("✅ 查询完成")

	// 打印查询结果
	log.Printf("\n条件查询结果 (age >= 25):")
	for _, col := range queryResults {
		switch col.Name() {
		case "name":
			if nameCol, ok := col.(*entity.ColumnVarChar); ok {
				log.Printf("  名字: %v", nameCol.Data())
			}
		case "age":
			if ageCol, ok := col.(*entity.ColumnInt64); ok {
				log.Printf("  年龄: %v", ageCol.Data())
			}
		case "vector":
			if vecCol, ok := col.(*entity.ColumnFloatVector); ok {
				log.Printf("  向量数据:")
				for i, vec := range vecCol.Data() {
					log.Printf("    向量 %d: [%v]", i, formatVector(vec))
				}
			}
		}
	}

	log.Printf("\n✅ 所有测试完成")
}

// formatVector 格式化向量数据，只显示前5个和后5个元素
func formatVector(vec []float32) string {
	if len(vec) <= 10 {
		return fmt.Sprintf("%v", vec)
	}
	prefix := vec[:5]
	suffix := vec[len(vec)-5:]
	return fmt.Sprintf("%v ... %v", prefix, suffix)
}
