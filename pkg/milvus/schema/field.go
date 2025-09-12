package schema

import (
	"fmt"

	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

// Field 定义字段接口
type Field interface {
	// Build 构建字段
	Build() *entity.Field
}

// BaseField 基础字段
type BaseField struct {
	name        string
	description string
	dataType    entity.FieldType
	primaryKey  bool
	autoID      bool
	typeParams  map[string]string
}

// NewBaseField 创建基础字段
func NewBaseField(name string, dataType entity.FieldType) *BaseField {
	return &BaseField{
		name:       name,
		dataType:   dataType,
		typeParams: make(map[string]string),
	}
}

// WithDescription 设置描述
func (f *BaseField) WithDescription(desc string) *BaseField {
	f.description = desc
	return f
}

// WithPrimaryKey 设置主键
func (f *BaseField) WithPrimaryKey(autoID bool) *BaseField {
	f.primaryKey = true
	f.autoID = autoID
	return f
}

// WithTypeParam 设置类型参数
func (f *BaseField) WithTypeParam(key, value string) *BaseField {
	f.typeParams[key] = value
	return f
}

// Build 构建字段
func (f *BaseField) Build() *entity.Field {
	return &entity.Field{
		Name:        f.name,
		Description: f.description,
		DataType:    f.dataType,
		PrimaryKey:  f.primaryKey,
		AutoID:      f.autoID,
		TypeParams:  f.typeParams,
	}
}

// VectorField 向量字段
type VectorField struct {
	*BaseField
	dim int
}

// NewVectorField 创建向量字段
func NewVectorField(name string, dim int, dataType entity.FieldType) *VectorField {
	if dataType != entity.FieldTypeFloatVector && dataType != entity.FieldTypeBinaryVector {
		panic(fmt.Sprintf("invalid vector data type: %v", dataType))
	}

	f := &VectorField{
		BaseField: NewBaseField(name, dataType),
		dim:       dim,
	}
	f.WithTypeParam("dim", fmt.Sprintf("%d", dim))
	return f
}

// IDField ID字段
type IDField struct {
	*BaseField
}

// NewIDField 创建ID字段
func NewIDField(name string, dataType entity.FieldType, autoID bool) *IDField {
	if dataType != entity.FieldTypeInt64 && dataType != entity.FieldTypeVarChar {
		panic(fmt.Sprintf("invalid ID data type: %v", dataType))
	}

	f := &IDField{
		BaseField: NewBaseField(name, dataType),
	}
	f.WithPrimaryKey(autoID)
	return f
}

// Int64Field Int64字段
type Int64Field struct {
	*BaseField
}

// NewInt64Field 创建Int64字段
func NewInt64Field(name string) *Int64Field {
	return &Int64Field{
		BaseField: NewBaseField(name, entity.FieldTypeInt64),
	}
}

// VarCharField VarChar字段
type VarCharField struct {
	*BaseField
	maxLength int
}

// NewVarCharField 创建VarChar字段
func NewVarCharField(name string, maxLength int) *VarCharField {
	f := &VarCharField{
		BaseField: NewBaseField(name, entity.FieldTypeVarChar),
		maxLength: maxLength,
	}
	f.WithTypeParam("max_length", fmt.Sprintf("%d", maxLength))
	return f
}

// FloatField Float字段
type FloatField struct {
	*BaseField
}

// NewFloatField 创建Float字段
func NewFloatField(name string) *FloatField {
	return &FloatField{
		BaseField: NewBaseField(name, entity.FieldTypeFloat),
	}
}

// DoubleField Double字段
type DoubleField struct {
	*BaseField
}

// NewDoubleField 创建Double字段
func NewDoubleField(name string) *DoubleField {
	return &DoubleField{
		BaseField: NewBaseField(name, entity.FieldTypeDouble),
	}
}

// BoolField Bool字段
type BoolField struct {
	*BaseField
}

// NewBoolField 创建Bool字段
func NewBoolField(name string) *BoolField {
	return &BoolField{
		BaseField: NewBaseField(name, entity.FieldTypeBool),
	}
}
