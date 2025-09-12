package collection

import (
	"context"

	milvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/pkg/errors"
	"github.com/stones-hub/taurus-pro-milvus/pkg/milvus/client"
)

// Collection 定义集合接口
type Collection interface {
	// Name 返回集合名称
	Name() string

	// Description 返回集合描述
	Description() string

	// Schema 返回集合Schema
	Schema() *entity.Schema

	// Insert 插入数据
	Insert(ctx context.Context, columns ...entity.Column) (entity.Column, error)

	// Delete 删除数据
	Delete(ctx context.Context, expr string) error

	// Search 搜索数据
	Search(ctx context.Context, vectors []entity.Vector, vectorField string, outputFields []string, metricType entity.MetricType, topK int, params entity.SearchParam) ([]milvus.SearchResult, error)

	// Query 查询数据
	Query(ctx context.Context, expr string, outputFields []string) ([]entity.Column, error)

	// CreateIndex 创建索引
	CreateIndex(ctx context.Context, fieldName string, indexParams entity.Index) error

	// DropIndex 删除索引
	DropIndex(ctx context.Context, fieldName string) error

	// CreatePartition 创建分区
	CreatePartition(ctx context.Context, partitionName string) error

	// DropPartition 删除分区
	DropPartition(ctx context.Context, partitionName string) error

	// Load 加载集合到内存
	Load(ctx context.Context) error

	// Release 从内存中释放集合
	Release(ctx context.Context) error

	// Drop 删除集合
	Drop(ctx context.Context) error
}

// collection 实现 Collection 接口
type collection struct {
	cli         client.Client
	name        string
	description string
	schema      *entity.Schema
	opts        *Options
}

// New 创建新的集合实例
func New(cli client.Client, schema *entity.Schema, opts ...Option) (Collection, error) {
	if cli == nil {
		return nil, errors.New("client is required")
	}
	if schema == nil {
		return nil, errors.New("schema is required")
	}

	options := DefaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	return &collection{
		cli:         cli,
		name:        schema.CollectionName,
		description: schema.Description,
		schema:      schema,
		opts:        options,
	}, nil
}

// Name 实现 Collection 接口
func (c *collection) Name() string {
	return c.name
}

// Description 实现 Collection 接口
func (c *collection) Description() string {
	return c.description
}

// Schema 实现 Collection 接口
func (c *collection) Schema() *entity.Schema {
	return c.schema
}

// Insert 实现 Collection 接口
func (c *collection) Insert(ctx context.Context, columns ...entity.Column) (entity.Column, error) {
	return c.cli.Insert(ctx, c.name, "", columns...)
}

// Delete 实现 Collection 接口
func (c *collection) Delete(ctx context.Context, expr string) error {
	return c.cli.Delete(ctx, c.name, "", expr)
}

// Search 实现 Collection 接口
func (c *collection) Search(ctx context.Context, vectors []entity.Vector, vectorField string, outputFields []string, metricType entity.MetricType, topK int, params entity.SearchParam) ([]milvus.SearchResult, error) {
	return c.cli.Search(
		ctx,
		c.name,
		nil, // partitionNames
		"",  // expr
		outputFields,
		vectors,
		vectorField,
		metricType,
		topK,
		params,
	)
}

// Query 实现 Collection 接口
func (c *collection) Query(ctx context.Context, expr string, outputFields []string) ([]entity.Column, error) {
	return c.cli.Query(ctx, c.name, nil, expr, outputFields)
}

// CreateIndex 实现 Collection 接口
func (c *collection) CreateIndex(ctx context.Context, fieldName string, indexParams entity.Index) error {
	return c.cli.CreateIndex(ctx, c.name, fieldName, indexParams, false)
}

// DropIndex 实现 Collection 接口
func (c *collection) DropIndex(ctx context.Context, fieldName string) error {
	return c.cli.DropIndex(ctx, c.name, fieldName)
}

// CreatePartition 实现 Collection 接口
func (c *collection) CreatePartition(ctx context.Context, partitionName string) error {
	return c.cli.CreatePartition(ctx, c.name, partitionName)
}

// DropPartition 实现 Collection 接口
func (c *collection) DropPartition(ctx context.Context, partitionName string) error {
	return c.cli.DropPartition(ctx, c.name, partitionName)
}

// Load 实现 Collection 接口
func (c *collection) Load(ctx context.Context) error {
	return c.cli.LoadCollection(ctx, c.name, false)
}

// Release 实现 Collection 接口
func (c *collection) Release(ctx context.Context) error {
	return c.cli.ReleaseCollection(ctx, c.name)
}

// Drop 实现 Collection 接口
func (c *collection) Drop(ctx context.Context) error {
	return c.cli.DropCollection(ctx, c.name)
}
