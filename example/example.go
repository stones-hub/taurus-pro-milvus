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
	collectionName = "test_collection"
	partitionName  = "test_partition"
	dimension      = 128
)

func main() {
	// 1. 创建客户端
	log.Printf("1️⃣ 创建 Milvus 客户端...")
	cli, err := client.New(
		client.WithAddress("192.168.103.113:19530"),
		client.WithAuth("root", ""),    // 设置用户名和密码
		client.WithDatabase("default"), // 设置数据库名
		client.WithConnectTimeout(5*time.Second),
		client.WithRetry(3, time.Second),
		client.WithKeepAlive(5*time.Second, 10*time.Second),
		client.WithIdentifier("example-client"),
	)
	if err != nil {
		log.Fatalf("❌ 创建客户端失败: %v", err)
	}
	defer cli.Close()
	log.Printf("✅ 成功连接到 Milvus 服务器")

	ctx := context.Background()

	// 2. 集合管理
	log.Printf("\n2️⃣ 开始集合管理操作...")
	if err := collectionOperations(ctx, cli); err != nil {
		log.Fatalf("❌ 集合操作失败: %v", err)
	}

	// 3. 索引管理
	log.Printf("\n3️⃣ 开始索引管理操作...")
	if err := indexOperations(ctx, cli); err != nil {
		log.Fatalf("❌ 索引操作失败: %v", err)
	}

	// 4. 分区管理
	log.Printf("\n4️⃣ 开始分区管理操作...")
	if err := partitionOperations(ctx, cli); err != nil {
		log.Fatalf("❌ 分区操作失败: %v", err)
	}

	// 5. 数据操作
	log.Printf("\n5️⃣ 开始数据操作...")
	if err := dataOperations(ctx, cli); err != nil {
		log.Fatalf("❌ 数据操作失败: %v", err)
	}

	log.Printf("\n✅ 所有操作完成")
}

// collectionOperations 演示集合相关操作
func collectionOperations(ctx context.Context, cli client.Client) error {
	// 检查集合是否存在
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
		WithDescription("Test collection for all Milvus operations")

	// 添加字段
	builder.AddField(schema.NewIDField("id", entity.FieldTypeInt64, true))
	builder.AddField(schema.NewVectorField("vector", dimension, entity.FieldTypeFloatVector))
	builder.AddField(schema.NewVarCharField("name", 100))
	builder.AddField(schema.NewInt64Field("age"))

	sch, err := builder.Build()
	if err != nil {
		return fmt.Errorf("构建Schema失败: %w", err)
	}

	// 创建集合
	if err := cli.CreateCollection(ctx, sch, 2); err != nil {
		return fmt.Errorf("创建集合失败: %w", err)
	}
	log.Printf("✅ 集合创建成功")

	// 获取集合统计信息
	stats, err := cli.GetCollectionStatistics(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("获取集合统计信息失败: %w", err)
	}
	log.Printf("📊 集合统计信息: %v", stats)

	return nil
}

// partitionOperations 演示分区相关操作
func partitionOperations(ctx context.Context, cli client.Client) error {
	// 创建分区
	if err := cli.CreatePartition(ctx, collectionName, partitionName); err != nil {
		return fmt.Errorf("创建分区失败: %w", err)
	}
	log.Printf("✅ 分区创建成功")

	// 检查分区是否存在
	exists, err := cli.HasPartition(ctx, collectionName, partitionName)
	if err != nil {
		return fmt.Errorf("检查分区失败: %w", err)
	}
	log.Printf("📌 分区存在状态: %v", exists)

	// 加载分区
	if err := cli.LoadPartitions(ctx, collectionName, []string{partitionName}, false); err != nil {
		return fmt.Errorf("加载分区失败: %w", err)
	}
	log.Printf("✅ 分区加载成功")

	// 等待分区加载完成
	log.Printf("等待分区加载完成...")
	time.Sleep(5 * time.Second)

	// 释放分区
	defer cli.ReleasePartitions(ctx, collectionName, []string{partitionName})

	return nil
}

// indexOperations 演示索引相关操作
func indexOperations(ctx context.Context, cli client.Client) error {
	// 创建IVF_FLAT索引
	indexParams, err := entity.NewIndexIvfFlat(entity.L2, 1024)
	if err != nil {
		return fmt.Errorf("创建索引参数失败: %w", err)
	}

	// 创建索引
	if err := cli.CreateIndex(ctx, collectionName, "vector", indexParams, false); err != nil {
		return fmt.Errorf("创建索引失败: %w", err)
	}
	log.Printf("✅ 索引创建成功")

	// 获取索引状态
	state, err := cli.GetIndexState(ctx, collectionName, "vector")
	if err != nil {
		return fmt.Errorf("获取索引状态失败: %w", err)
	}
	log.Printf("📊 索引状态: %v", state)

	// 加载集合（使用索引前必须加载）
	if err := cli.LoadCollection(ctx, collectionName, false); err != nil {
		return fmt.Errorf("加载集合失败: %w", err)
	}
	log.Printf("✅ 集合加载成功")

	return nil
}

// dataOperations 演示数据操作
func dataOperations(ctx context.Context, cli client.Client) error {
	// 准备测试数据
	vectors := make([][]float32, 3)
	for i := range vectors {
		vectors[i] = make([]float32, dimension)
		for j := range vectors[i] {
			vectors[i][j] = float32(i*dimension + j)
		}
	}

	names := []string{"张三", "李四", "王五"}
	ages := []int64{25, 30, 35}

	// 1. 插入数据
	columns := []entity.Column{
		entity.NewColumnVarChar("name", names),
		entity.NewColumnInt64("age", ages),
		entity.NewColumnFloatVector("vector", dimension, vectors),
	}

	log.Printf("📥 开始插入数据...")
	_, err := cli.Insert(ctx, collectionName, partitionName, columns...)
	if err != nil {
		return fmt.Errorf("插入数据失败: %w", err)
	}
	log.Printf("✅ 数据插入成功")
	time.Sleep(2 * time.Second) // 等待数据生效

	// 2. 条件查询
	log.Printf("\n📝 开始条件查询...")
	queryResults, err := cli.Query(
		ctx,
		collectionName,
		nil, // 不指定分区，查询所有分区
		"age >= 30",
		[]string{"name", "age"},
	)
	if err != nil {
		return fmt.Errorf("条件查询失败: %w", err)
	}
	printQueryResults(queryResults)

	// 3. 向量相似度搜索
	log.Printf("\n🔍 开始向量相似度搜索...")
	searchVectors := []entity.Vector{
		entity.FloatVector(vectors[0]), // 使用第一个向量作为查询向量
	}
	searchParams, err := entity.NewIndexIvfFlatSearchParam(10)
	if err != nil {
		return fmt.Errorf("创建搜索参数失败: %w", err)
	}

	searchResults, err := cli.Search(
		ctx,
		collectionName,
		nil, // 不指定分区，搜索所有分区
		"",
		[]string{"name", "age"},
		searchVectors,
		"vector",
		entity.L2,
		3,
		searchParams,
	)
	if err != nil {
		return fmt.Errorf("向量搜索失败: %w", err)
	}
	printSearchResults(searchResults)

	// 4. 删除数据
	log.Printf("\n🗑️ 开始删除数据...")
	if err := cli.Delete(ctx, collectionName, "", "age == 25"); err != nil {
		return fmt.Errorf("删除数据失败: %w", err)
	}
	log.Printf("✅ 数据删除成功")

	// 5. 验证删除结果
	time.Sleep(2 * time.Second) // 等待删除生效
	verifyResults, err := cli.Query(
		ctx,
		collectionName,
		nil, // 不指定分区，查询所有分区
		"age == 25",
		[]string{"name", "age"},
	)
	if err != nil {
		return fmt.Errorf("验证删除失败: %w", err)
	}
	log.Printf("验证删除结果:")
	printQueryResults(verifyResults)

	return nil
}

// printQueryResults 打印查询结果
func printQueryResults(results []entity.Column) {
	log.Printf("查询结果:")
	for _, col := range results {
		switch col.Name() {
		case "name":
			if nameCol, ok := col.(*entity.ColumnVarChar); ok {
				log.Printf("  名字: %v", nameCol.Data())
			}
		case "age":
			if ageCol, ok := col.(*entity.ColumnInt64); ok {
				log.Printf("  年龄: %v", ageCol.Data())
			}
		}
	}
}

// printSearchResults 打印搜索结果
func printSearchResults(results []milvus.SearchResult) {
	for i, result := range results {
		log.Printf("\n查询向量 %d 的搜索结果:", i)
		if ids, ok := result.IDs.(*entity.ColumnInt64); ok {
			for j, id := range ids.Data() {
				log.Printf("  匹配结果 %d:", j+1)
				log.Printf("    ID: %v", id)
				log.Printf("    距离: %v", result.Scores[j])
			}
		}
	}
}
