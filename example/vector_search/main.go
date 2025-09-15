package main

import (
	"context"
	"fmt"
	"log"
	"time"

	milvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/stones-hub/taurus-pro-milvus/pkg/milvus/client"
	"github.com/stones-hub/taurus-pro-milvus/pkg/milvus/schema"
)

const (
	collectionName = "test_vector_collection"
	dimension      = 128
)

func main() {
	// 创建客户端
	log.Printf("1️⃣ 创建 Milvus 客户端...")
	cli, err := client.New(
		client.WithAddress("192.168.103.113:19530"),
		client.WithAuth("root", ""),
		client.WithDatabase("default"),
		client.WithConnectTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatalf("❌ 创建客户端失败: %v", err)
	}
	defer cli.Close()
	log.Printf("✅ 成功连接到 Milvus 服务器")

	ctx := context.Background()

	// 准备集合
	if err := prepareCollection(ctx, cli); err != nil {
		log.Fatalf("❌ 准备集合失败: %v", err)
	}

	// 执行向量搜索示例
	if err := vectorSearchExamples(ctx, cli); err != nil {
		log.Fatalf("❌ 向量搜索示例失败: %v", err)
	}

	log.Printf("\n✅ 所有操作完成")
}

// prepareCollection 准备测试集合
func prepareCollection(ctx context.Context, cli client.Client) error {
	// 检查并删除已存在的集合
	exists, err := cli.HasCollection(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("检查集合失败: %w", err)
	}
	if exists {
		if err := cli.DropCollection(ctx, collectionName); err != nil {
			return fmt.Errorf("删除已存在的集合失败: %w", err)
		}
		log.Printf("已删除现有集合")
		time.Sleep(2 * time.Second)
	}

	// 创建Schema
	builder := schema.NewBuilder(collectionName).
		WithDescription("Test collection for vector similarity search")

	// 添加字段
	builder.AddField(schema.NewIDField("id", entity.FieldTypeInt64, true))
	builder.AddField(schema.NewVectorField("vector", dimension, entity.FieldTypeFloatVector))
	builder.AddField(schema.NewVarCharField("name", 100))
	builder.AddField(schema.NewInt64Field("age"))
	builder.AddField(schema.NewVarCharField("category", 100)) // 添加类别字段用于过滤

	sch, err := builder.Build()
	if err != nil {
		return fmt.Errorf("构建Schema失败: %w", err)
	}

	// 创建集合
	if err := cli.CreateCollection(ctx, sch, 2); err != nil {
		return fmt.Errorf("创建集合失败: %w", err)
	}
	log.Printf("✅ 集合创建成功")

	// 创建索引
	indexParams, err := entity.NewIndexIvfFlat(entity.L2, 1024)
	if err != nil {
		return fmt.Errorf("创建索引参数失败: %w", err)
	}

	if err := cli.CreateIndex(ctx, collectionName, "vector", indexParams, false); err != nil {
		return fmt.Errorf("创建索引失败: %w", err)
	}
	log.Printf("✅ 索引创建成功")

	// 加载集合
	if err := cli.LoadCollection(ctx, collectionName, false); err != nil {
		return fmt.Errorf("加载集合失败: %w", err)
	}
	log.Printf("✅ 集合加载成功")

	// 插入测试数据
	return insertTestData(ctx, cli)
}

// insertTestData 插入测试数据
func insertTestData(ctx context.Context, cli client.Client) error {
	// 准备测试数据
	vectors := make([][]float32, 6) // 6个测试向量
	for i := range vectors {
		vectors[i] = make([]float32, dimension)
		// 创建三组相似的向量
		base := float32(i / 2) // 每两个向量相似
		for j := range vectors[i] {
			vectors[i][j] = base + float32(j)*0.01
		}
	}

	names := []string{"张三", "张三相似", "李四", "李四相似", "王五", "王五相似"}
	ages := []int64{25, 26, 30, 31, 35, 36}
	categories := []string{"A", "A", "B", "B", "C", "C"}

	// 插入数据
	columns := []entity.Column{
		entity.NewColumnVarChar("name", names),
		entity.NewColumnInt64("age", ages),
		entity.NewColumnVarChar("category", categories),
		entity.NewColumnFloatVector("vector", dimension, vectors),
	}

	log.Printf("📥 开始插入测试数据...")
	_, err := cli.Insert(ctx, collectionName, "", columns...)
	if err != nil {
		return fmt.Errorf("插入数据失败: %w", err)
	}
	log.Printf("✅ 测试数据插入成功")

	// 等待数据生效
	time.Sleep(2 * time.Second)
	return nil
}

// vectorSearchExamples 展示各种向量搜索场景
func vectorSearchExamples(ctx context.Context, cli client.Client) error {
	// 1. 基本向量相似度搜索
	log.Printf("\n1️⃣ 基本向量相似度搜索...")
	if err := basicVectorSearch(ctx, cli); err != nil {
		return fmt.Errorf("基本向量搜索失败: %w", err)
	}

	// 2. 带过滤条件的向量搜索
	log.Printf("\n2️⃣ 带过滤条件的向量搜索...")
	if err := filteredVectorSearch(ctx, cli); err != nil {
		return fmt.Errorf("带过滤条件的向量搜索失败: %w", err)
	}

	// 3. 多向量批量搜索
	log.Printf("\n3️⃣ 多向量批量搜索...")
	if err := batchVectorSearch(ctx, cli); err != nil {
		return fmt.Errorf("多向量批量搜索失败: %w", err)
	}

	return nil
}

// basicVectorSearch 基本向量相似度搜索
func basicVectorSearch(ctx context.Context, cli client.Client) error {
	// 使用第一个向量作为查询向量
	queryVec := make([]float32, dimension)
	for i := range queryVec {
		queryVec[i] = float32(i) * 0.01
	}
	queryVectors := []entity.Vector{entity.FloatVector(queryVec)}

	searchParams, err := entity.NewIndexIvfFlatSearchParam(10)
	if err != nil {
		return fmt.Errorf("创建搜索参数失败: %w", err)
	}

	results, err := cli.Search(
		ctx,
		collectionName,
		nil,
		"",                           // 无过滤条件
		[]string{"name", "category"}, // 返回这些字段
		queryVectors,
		"vector",
		entity.L2,
		5, // 返回前5个最相似的结果
		searchParams,
	)
	if err != nil {
		return fmt.Errorf("向量搜索失败: %w", err)
	}

	printSearchResults(results, "基本搜索")
	return nil
}

// filteredVectorSearch 带过滤条件的向量搜索
func filteredVectorSearch(ctx context.Context, cli client.Client) error {
	// 使用第三个向量（李四）作为查询向量
	queryVec := make([]float32, dimension)
	base := float32(1) // 对应李四的向量基数
	for i := range queryVec {
		queryVec[i] = base + float32(i)*0.01
	}
	queryVectors := []entity.Vector{entity.FloatVector(queryVec)}

	searchParams, err := entity.NewIndexIvfFlatSearchParam(10)
	if err != nil {
		return fmt.Errorf("创建搜索参数失败: %w", err)
	}

	results, err := cli.Search(
		ctx,
		collectionName,
		nil,
		"category == \"B\"", // 只搜索类别B的数据
		[]string{"name", "category", "age"},
		queryVectors,
		"vector",
		entity.L2,
		3,
		searchParams,
	)
	if err != nil {
		return fmt.Errorf("向量搜索失败: %w", err)
	}

	printSearchResults(results, "带过滤条件的搜索")
	return nil
}

// batchVectorSearch 多向量批量搜索
func batchVectorSearch(ctx context.Context, cli client.Client) error {
	// 准备多个查询向量
	queryVectors := make([]entity.Vector, 2)

	// 第一个查询向量（类似张三）
	vec1 := make([]float32, dimension)
	for i := range vec1 {
		vec1[i] = float32(i) * 0.01
	}
	queryVectors[0] = entity.FloatVector(vec1)

	// 第二个查询向量（类似李四）
	vec2 := make([]float32, dimension)
	for i := range vec2 {
		vec2[i] = float32(1) + float32(i)*0.01
	}
	queryVectors[1] = entity.FloatVector(vec2)

	searchParams, err := entity.NewIndexIvfFlatSearchParam(10)
	if err != nil {
		return fmt.Errorf("创建搜索参数失败: %w", err)
	}

	results, err := cli.Search(
		ctx,
		collectionName,
		nil,
		"age < 35", // 年龄过滤
		[]string{"name", "category", "age"},
		queryVectors,
		"vector",
		entity.L2,
		3,
		searchParams,
	)
	if err != nil {
		return fmt.Errorf("向量搜索失败: %w", err)
	}

	printSearchResults(results, "批量搜索")
	return nil
}

// printSearchResults 打印搜索结果
func printSearchResults(results []milvus.SearchResult, title string) {
	log.Printf("\n%s结果:", title)
	for i, result := range results {
		log.Printf("查询向量 %d 的匹配结果:", i+1)
		if ids, ok := result.IDs.(*entity.ColumnInt64); ok {
			for j, id := range ids.Data() {
				log.Printf("  匹配 %d:", j+1)
				log.Printf("    ID: %v", id)
				log.Printf("    距离: %v", result.Scores[j])

				// 打印其他字段
				for _, field := range result.Fields {
					switch col := field.(type) {
					case *entity.ColumnVarChar:
						if len(col.Data()) > j {
							log.Printf("    %s: %v", col.Name(), col.Data()[j])
						}
					case *entity.ColumnInt64:
						if len(col.Data()) > j {
							log.Printf("    %s: %v", col.Name(), col.Data()[j])
						}
					}
				}
			}
		}
	}
}
