package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/stones-hub/taurus-pro-milvus/pkg/milvus"
	"github.com/stones-hub/taurus-pro-milvus/pkg/milvus/client"
)

const (
	// Milvus服务器配置
	address  = "192.168.103.113:19530"
	username = "root"
	password = ""

	// 数据库和集合配置
	dbName         = "default"
	collectionName = "example_collection"
	partitionName  = "example_partition"

	// 向量配置
	vectorDim   = 128
	vectorCount = 100
)

func main() {
	// 创建连接池
	pool := milvus.NewPool()
	defer pool.Close()

	// 添加客户端到连接池
	err := pool.Add("main_client",
		client.WithAddress(address),
		client.WithAuth(username, password),
		client.WithDatabase(dbName),
		client.WithRetry(3, 2*time.Second),
	)
	if err != nil {
		log.Fatalf("❌ 创建客户端失败: %v", err)
	}
	fmt.Println("✅ 成功创建Milvus客户端")

	// 获取客户端
	cli, err := pool.Get("main_client")
	if err != nil {
		log.Fatalf("❌ 获取客户端失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 执行完整的CRUD操作
	if err := runCRUDExample(ctx, cli); err != nil {
		log.Fatalf("❌ 执行CRUD操作失败: %v", err)
	}

	fmt.Println("🎉 所有操作执行完成！")
}

// runCRUDExample 执行完整的CRUD操作示例
func runCRUDExample(ctx context.Context, cli client.Client) error {
	// 1. 创建数据库
	fmt.Println("\n📁 步骤1: 创建数据库")
	if err := createDatabase(ctx, cli); err != nil {
		return fmt.Errorf("创建数据库失败: %w", err)
	}

	// 2. 创建集合
	fmt.Println("\n📚 步骤2: 创建集合")
	if err := createCollection(ctx, cli); err != nil {
		return fmt.Errorf("创建集合失败: %w", err)
	}

	// 3. 创建分区
	fmt.Println("\n🗂️ 步骤3: 创建分区")
	if err := createPartition(ctx, cli); err != nil {
		return fmt.Errorf("创建分区失败: %w", err)
	}

	// 4. 创建索引
	fmt.Println("\n🔍 步骤4: 创建索引")
	if err := createIndex(ctx, cli); err != nil {
		return fmt.Errorf("创建索引失败: %w", err)
	}

	// 5. 加载集合
	fmt.Println("\n⚡ 步骤5: 加载集合")
	if err := loadCollection(ctx, cli); err != nil {
		return fmt.Errorf("加载集合失败: %w", err)
	}

	// 6. 插入数据
	fmt.Println("\n➕ 步骤6: 插入数据")
	if err := insertData(ctx, cli); err != nil {
		return fmt.Errorf("插入数据失败: %w", err)
	}

	// 7. 查询数据
	fmt.Println("\n🔍 步骤7: 查询数据")
	if err := queryData(ctx, cli); err != nil {
		return fmt.Errorf("查询数据失败: %w", err)
	}

	// 8. 搜索数据
	fmt.Println("\n🔎 步骤8: 搜索数据")
	if err := searchData(ctx, cli); err != nil {
		return fmt.Errorf("搜索数据失败: %w", err)
	}

	// 9. 更新数据（删除+插入）
	fmt.Println("\n✏️ 步骤9: 更新数据")
	if err := updateData(ctx, cli); err != nil {
		return fmt.Errorf("更新数据失败: %w", err)
	}

	// 10. 删除数据
	fmt.Println("\n🗑️ 步骤10: 删除数据")
	if err := deleteData(ctx, cli); err != nil {
		return fmt.Errorf("删除数据失败: %w", err)
	}

	// 11. 清理资源
	fmt.Println("\n🧹 步骤11: 清理资源")
	if err := cleanup(ctx, cli); err != nil {
		return fmt.Errorf("清理资源失败: %w", err)
	}

	return nil
}

// createDatabase 创建数据库
func createDatabase(ctx context.Context, cli client.Client) error {
	// 检查数据库是否存在
	// 注意：Milvus v2.x 可能不支持直接检查数据库是否存在
	// 这里直接尝试创建，如果已存在会返回错误

	err := cli.CreateDatabase(ctx, dbName)
	if err != nil {
		fmt.Printf("⚠️ 数据库已存在或创建失败: %v\n", err)
		// 如果是default数据库，可能已经存在，继续执行
		if dbName == "default" {
			fmt.Printf("ℹ️ 使用默认数据库: %s\n", dbName)
		}
	} else {
		fmt.Printf("✅ 成功创建数据库: %s\n", dbName)
	}

	// 切换到数据库
	err = cli.UseDatabase(ctx, dbName)
	if err != nil {
		return fmt.Errorf("切换数据库失败: %w", err)
	}
	fmt.Printf("✅ 成功切换到数据库: %s\n", dbName)

	return nil
}

// createCollection 创建集合
func createCollection(ctx context.Context, cli client.Client) error {
	// 检查集合是否已存在
	exists, err := cli.HasCollection(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("检查集合是否存在失败: %w", err)
	}

	if exists {
		fmt.Printf("⚠️ 集合已存在，先删除: %s\n", collectionName)
		// 先释放集合
		cli.ReleaseCollection(ctx, collectionName)
		// 删除集合
		err = cli.DropCollection(ctx, collectionName)
		if err != nil {
			return fmt.Errorf("删除已存在的集合失败: %w", err)
		}
		fmt.Printf("✅ 成功删除已存在的集合: %s\n", collectionName)
	}

	// 定义集合模式
	schema := &entity.Schema{
		CollectionName: collectionName,
		Description:    "示例集合，用于演示向量数据库操作",
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
					"dim": fmt.Sprintf("%d", vectorDim),
				},
			},
			{
				ID:       2,
				Name:     "text",
				DataType: entity.FieldTypeVarChar,
				TypeParams: map[string]string{
					"max_length": "200",
				},
			},
			{
				ID:       3,
				Name:     "category",
				DataType: entity.FieldTypeInt32,
			},
			{
				ID:       4,
				Name:     "score",
				DataType: entity.FieldTypeFloat,
			},
		},
	}

	// 创建集合
	err = cli.CreateCollection(ctx, schema, 1)
	if err != nil {
		return fmt.Errorf("创建集合失败: %w", err)
	}

	fmt.Printf("✅ 成功创建集合: %s\n", collectionName)
	return nil
}

// createPartition 创建分区
func createPartition(ctx context.Context, cli client.Client) error {
	// 检查分区是否已存在
	exists, err := cli.HasPartition(ctx, collectionName, partitionName)
	if err != nil {
		return fmt.Errorf("检查分区是否存在失败: %w", err)
	}

	if exists {
		fmt.Printf("⚠️ 分区已存在，先删除: %s\n", partitionName)
		// 删除分区
		err = cli.DropPartition(ctx, collectionName, partitionName)
		if err != nil {
			return fmt.Errorf("删除已存在的分区失败: %w", err)
		}
		fmt.Printf("✅ 成功删除已存在的分区: %s\n", partitionName)
	}

	err = cli.CreatePartition(ctx, collectionName, partitionName)
	if err != nil {
		return fmt.Errorf("创建分区失败: %w", err)
	}

	fmt.Printf("✅ 成功创建分区: %s\n", partitionName)
	return nil
}

// createIndex 创建索引
func createIndex(ctx context.Context, cli client.Client) error {
	// 为向量字段创建IVF_FLAT索引
	idx := index.NewIvfFlatIndex(entity.L2, 1024)
	err := cli.CreateIndex(ctx, collectionName, "vector", idx)
	if err != nil {
		return fmt.Errorf("创建索引失败: %w", err)
	}

	fmt.Printf("✅ 成功为字段 'vector' 创建IVF_FLAT索引\n")
	return nil
}

// loadCollection 加载集合
func loadCollection(ctx context.Context, cli client.Client) error {
	err := cli.LoadCollection(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("加载集合失败: %w", err)
	}

	fmt.Printf("✅ 成功加载集合: %s\n", collectionName)
	return nil
}

// insertData 插入数据
func insertData(ctx context.Context, cli client.Client) error {
	// 生成测试数据
	vectors := generateVectors(vectorCount, vectorDim)
	texts := generateTexts(vectorCount)
	categories := generateCategories(vectorCount)
	scores := generateScores(vectorCount)

	// 创建列数据
	vectorColumn := column.NewColumnFloatVector("vector", vectorDim, vectors)
	textColumn := column.NewColumnVarChar("text", texts)
	categoryColumn := column.NewColumnInt32("category", categories)
	scoreColumn := column.NewColumnFloat("score", scores)

	// 插入数据到指定分区
	ids, err := cli.Insert(ctx, collectionName, partitionName, vectorColumn, textColumn, categoryColumn, scoreColumn)
	if err != nil {
		return fmt.Errorf("插入数据失败: %w", err)
	}

	fmt.Printf("✅ 成功插入 %d 条数据，ID范围: %v\n", ids.Len(), ids)
	return nil
}

// queryData 查询数据
func queryData(ctx context.Context, cli client.Client) error {
	// 查询所有数据
	columns, err := cli.Query(ctx, collectionName, []string{partitionName}, "id > 0", []string{"text", "category", "score"})
	if err != nil {
		return fmt.Errorf("查询数据失败: %w", err)
	}

	fmt.Printf("✅ 查询到 %d 个字段的数据\n", len(columns))

	// 显示前5条数据
	if len(columns) > 0 {
		fmt.Println("📊 前5条数据示例:")
		for i, col := range columns {
			if i >= 5 {
				break
			}
			fmt.Printf("  字段 %d: %s (长度: %d)\n", i, col.Name(), col.Len())
		}
	}

	// 查询特定条件的数据
	columns, err = cli.Query(ctx, collectionName, []string{partitionName}, "category == 1", []string{"text", "score"})
	if err != nil {
		return fmt.Errorf("查询特定条件数据失败: %w", err)
	}

	fmt.Printf("✅ 查询到 category==1 的数据，共 %d 个字段\n", len(columns))

	return nil
}

// searchData 搜索数据
func searchData(ctx context.Context, cli client.Client) error {
	// 生成搜索向量
	searchVectors := generateVectors(3, vectorDim)
	searchVectorEntities := make([]entity.Vector, len(searchVectors))
	for i, v := range searchVectors {
		searchVectorEntities[i] = entity.FloatVector(v)
	}

	// 执行向量搜索
	results, err := cli.Search(ctx, collectionName, []string{partitionName}, []string{"text", "category", "score"},
		searchVectorEntities, "vector", entity.L2, 5, "", nil)
	if err != nil {
		return fmt.Errorf("搜索数据失败: %w", err)
	}

	fmt.Printf("✅ 搜索完成，返回 %d 个结果集\n", len(results))

	// 显示搜索结果
	for i, result := range results {
		fmt.Printf("🔍 搜索向量 %d 的相似结果:\n", i+1)
		for j, score := range result.Scores {
			if j >= 3 { // 只显示前3个结果
				break
			}
			fmt.Printf("  结果 %d: 相似度分数 = %.4f\n", j+1, score)
		}
	}

	// 带过滤条件的搜索
	results, err = cli.Search(ctx, collectionName, []string{partitionName}, []string{"text", "score"},
		searchVectorEntities, "vector", entity.COSINE, 3, "category == 1", nil)
	if err != nil {
		return fmt.Errorf("带过滤条件搜索失败: %w", err)
	}

	fmt.Printf("✅ 带过滤条件的搜索完成，返回 %d 个结果集\n", len(results))

	return nil
}

// updateData 更新数据（通过删除+插入实现）
func updateData(ctx context.Context, cli client.Client) error {
	// 删除特定条件的数据
	err := cli.Delete(ctx, collectionName, partitionName, "category == 2")
	if err != nil {
		return fmt.Errorf("删除数据失败: %w", err)
	}

	fmt.Println("✅ 成功删除 category==2 的数据")

	// 插入新的数据
	newVectors := generateVectors(10, vectorDim)
	newTexts := []string{"updated_text_1", "updated_text_2", "updated_text_3", "updated_text_4", "updated_text_5",
		"updated_text_6", "updated_text_7", "updated_text_8", "updated_text_9", "updated_text_10"}
	newCategories := make([]int32, 10)
	for i := range newCategories {
		newCategories[i] = 2
	}
	newScores := generateScores(10)

	vectorColumn := column.NewColumnFloatVector("vector", vectorDim, newVectors)
	textColumn := column.NewColumnVarChar("text", newTexts)
	categoryColumn := column.NewColumnInt32("category", newCategories)
	scoreColumn := column.NewColumnFloat("score", newScores)

	ids, err := cli.Insert(ctx, collectionName, partitionName, vectorColumn, textColumn, categoryColumn, scoreColumn)
	if err != nil {
		return fmt.Errorf("插入更新数据失败: %w", err)
	}

	fmt.Printf("✅ 成功插入 %d 条更新数据，ID范围: %v\n", ids.Len(), ids)

	return nil
}

// deleteData 删除数据
func deleteData(ctx context.Context, cli client.Client) error {
	// 删除特定条件的数据
	err := cli.Delete(ctx, collectionName, partitionName, "score < 0.5")
	if err != nil {
		return fmt.Errorf("删除数据失败: %w", err)
	}

	fmt.Println("✅ 成功删除 score < 0.5 的数据")

	// 查询剩余数据
	columns, err := cli.Query(ctx, collectionName, []string{partitionName}, "id > 0", []string{"text"})
	if err != nil {
		return fmt.Errorf("查询剩余数据失败: %w", err)
	}

	if len(columns) > 0 {
		fmt.Printf("📊 剩余数据: %d 条\n", columns[0].Len())
	}

	return nil
}

// cleanup 清理资源
func cleanup(ctx context.Context, cli client.Client) error {
	// 释放集合
	err := cli.ReleaseCollection(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("释放集合失败: %w", err)
	}
	fmt.Printf("✅ 成功释放集合: %s\n", collectionName)

	// 删除集合
	err = cli.DropCollection(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("删除集合失败: %w", err)
	}
	fmt.Printf("✅ 成功删除集合: %s\n", collectionName)

	// 删除数据库（如果是default数据库则跳过）
	if dbName != "default" {
		err = cli.DropDatabase(ctx, dbName)
		if err != nil {
			return fmt.Errorf("删除数据库失败: %w", err)
		}
		fmt.Printf("✅ 成功删除数据库: %s\n", dbName)
	} else {
		fmt.Printf("ℹ️ 保留默认数据库: %s\n", dbName)
	}

	return nil
}

// 辅助函数：生成随机向量
func generateVectors(count, dim int) [][]float32 {
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

// 辅助函数：生成随机文本
func generateTexts(count int) []string {
	texts := make([]string, count)
	for i := 0; i < count; i++ {
		texts[i] = fmt.Sprintf("示例文本_%d_%d", i+1, rand.Intn(1000))
	}
	return texts
}

// 辅助函数：生成随机分类
func generateCategories(count int) []int32 {
	categories := make([]int32, count)
	for i := 0; i < count; i++ {
		categories[i] = int32(rand.Intn(3) + 1) // 1, 2, 3
	}
	return categories
}

// 辅助函数：生成随机分数
func generateScores(count int) []float32 {
	scores := make([]float32, count)
	for i := 0; i < count; i++ {
		scores[i] = rand.Float32()
	}
	return scores
}
