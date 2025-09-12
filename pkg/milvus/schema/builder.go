package schema

import (
	"fmt"

	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

// Builder Schema构建器
type Builder struct {
	name        string
	description string
	fields      []Field
}

// NewBuilder 创建Schema构建器
func NewBuilder(name string) *Builder {
	return &Builder{
		name:   name,
		fields: make([]Field, 0),
	}
}

// WithDescription 设置描述
func (b *Builder) WithDescription(desc string) *Builder {
	b.description = desc
	return b
}

// AddField 添加字段
func (b *Builder) AddField(field Field) *Builder {
	b.fields = append(b.fields, field)
	return b
}

// Build 构建Schema
func (b *Builder) Build() (*entity.Schema, error) {
	if b.name == "" {
		return nil, fmt.Errorf("collection name is required")
	}

	if len(b.fields) == 0 {
		return nil, fmt.Errorf("at least one field is required")
	}

	// 检查是否有主键字段
	hasPrimaryKey := false
	for _, field := range b.fields {
		if f := field.Build(); f.PrimaryKey {
			hasPrimaryKey = true
			break
		}
	}

	if !hasPrimaryKey {
		return nil, fmt.Errorf("primary key field is required")
	}

	// 构建字段列表
	fields := make([]*entity.Field, len(b.fields))
	for i, field := range b.fields {
		fields[i] = field.Build()
	}

	return &entity.Schema{
		CollectionName: b.name,
		Description:    b.description,
		Fields:         fields,
	}, nil
}

// MustBuild 构建Schema，如果出错则panic
func (b *Builder) MustBuild() *entity.Schema {
	schema, err := b.Build()
	if err != nil {
		panic(err)
	}
	return schema
}
