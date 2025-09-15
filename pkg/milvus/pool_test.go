package milvus

import (
	"testing"

	"github.com/stones-hub/taurus-pro-milvus/pkg/milvus/client"
	"github.com/stretchr/testify/assert"
)

func TestPool(t *testing.T) {
	// 创建连接池
	p := NewPool()

	// 测试添加客户端
	err := p.Add("test1",
		client.WithAddress("192.168.103.113:19530"),
	)
	assert.NoError(t, err)

	// 测试重复添加
	err = p.Add("test1")
	assert.Error(t, err)

	// 测试获取客户端
	cli, err := p.Get("test1")
	assert.NoError(t, err)
	assert.NotNil(t, cli)

	// 测试获取不存在的客户端
	cli, err = p.Get("not_exist")
	assert.Error(t, err)
	assert.Nil(t, cli)

	// 测试 MustGet
	cli, err = p.MustGet("test2",
		client.WithAddress("192.168.103.113:19530"),
	)
	assert.NoError(t, err)
	assert.NotNil(t, cli)

	// 测试 Has
	assert.True(t, p.Has("test1"))
	assert.True(t, p.Has("test2"))
	assert.False(t, p.Has("not_exist"))

	// 测试 List
	clients := p.List()
	assert.Len(t, clients, 2)
	assert.Contains(t, clients, "test1")
	assert.Contains(t, clients, "test2")

	// 测试移除客户端
	err = p.Remove("test1")
	assert.NoError(t, err)
	assert.False(t, p.Has("test1"))

	// 测试移除不存在的客户端
	err = p.Remove("not_exist")
	assert.Error(t, err)

	// 测试关闭所有客户端
	err = p.Close()
	assert.NoError(t, err)
	assert.Empty(t, p.List())
}
