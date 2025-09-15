package milvus

import (
	"fmt"
	"testing"
	"time"

	"github.com/stones-hub/taurus-pro-milvus/pkg/milvus/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// 测试配置
	testAddress  = "192.168.103.113:19530"
	testUsername = "root"
	testPassword = ""
)

// TestNewPool 测试创建连接池
func TestNewPool(t *testing.T) {
	t.Run("创建空连接池", func(t *testing.T) {
		pool := NewPool()
		assert.NotNil(t, pool)
		assert.Empty(t, pool.List())
	})
}

// TestPoolAdd 测试添加客户端
func TestPoolAdd(t *testing.T) {
	pool := NewPool()
	defer pool.Close()

	t.Run("添加客户端成功", func(t *testing.T) {
		err := pool.Add("client1",
			client.WithAddress(testAddress),
			client.WithAuth(testUsername, testPassword),
		)
		if err != nil {
			t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
		}
		assert.NoError(t, err)
		assert.True(t, pool.Has("client1"))
		assert.Len(t, pool.List(), 1)
	})

	t.Run("添加重复名称的客户端应失败", func(t *testing.T) {
		err := pool.Add("client1",
			client.WithAddress(testAddress),
			client.WithAuth(testUsername, testPassword),
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("添加多个不同名称的客户端", func(t *testing.T) {
		err := pool.Add("client2",
			client.WithAddress(testAddress),
			client.WithAuth(testUsername, testPassword),
		)
		if err != nil {
			t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
		}
		assert.NoError(t, err)

		err = pool.Add("client3",
			client.WithAddress(testAddress),
			client.WithAuth(testUsername, testPassword),
		)
		if err != nil {
			t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
		}
		assert.NoError(t, err)

		assert.True(t, pool.Has("client2"))
		assert.True(t, pool.Has("client3"))
		assert.Len(t, pool.List(), 3)
	})
}

// TestPoolGet 测试获取客户端
func TestPoolGet(t *testing.T) {
	pool := NewPool()
	defer pool.Close()

	// 先添加一个客户端
	err := pool.Add("test_client",
		client.WithAddress(testAddress),
		client.WithAuth(testUsername, testPassword),
	)
	if err != nil {
		t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
	}

	t.Run("获取存在的客户端", func(t *testing.T) {
		cli, err := pool.Get("test_client")
		assert.NoError(t, err)
		assert.NotNil(t, cli)
	})

	t.Run("获取不存在的客户端应失败", func(t *testing.T) {
		cli, err := pool.Get("non_existent_client")
		assert.Error(t, err)
		assert.Nil(t, cli)
		assert.Contains(t, err.Error(), "not found")
	})
}

// TestPoolMustGet 测试MustGet方法
func TestPoolMustGet(t *testing.T) {
	pool := NewPool()
	defer pool.Close()

	t.Run("获取已存在的客户端", func(t *testing.T) {
		// 先添加一个客户端
		err := pool.Add("existing_client",
			client.WithAddress(testAddress),
			client.WithAuth(testUsername, testPassword),
		)
		if err != nil {
			t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
		}

		// 使用MustGet获取已存在的客户端
		cli, err := pool.MustGet("existing_client")
		assert.NoError(t, err)
		assert.NotNil(t, cli)
		assert.True(t, pool.Has("existing_client"))
	})

	t.Run("获取不存在的客户端应自动创建", func(t *testing.T) {
		cli, err := pool.MustGet("auto_created_client",
			client.WithAddress(testAddress),
			client.WithAuth(testUsername, testPassword),
		)
		if err != nil {
			t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
		}
		assert.NoError(t, err)
		assert.NotNil(t, cli)
		assert.True(t, pool.Has("auto_created_client"))
	})

	t.Run("使用不同配置创建客户端", func(t *testing.T) {
		cli, err := pool.MustGet("custom_client",
			client.WithAddress(testAddress),
			client.WithAuth(testUsername, testPassword),
			client.WithDatabase("custom_db"),
			client.WithRetry(3, 1*time.Second),
		)
		if err != nil {
			t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
		}
		assert.NoError(t, err)
		assert.NotNil(t, cli)
		assert.True(t, pool.Has("custom_client"))
	})
}

// TestPoolRemove 测试移除客户端
func TestPoolRemove(t *testing.T) {
	pool := NewPool()
	defer pool.Close()

	t.Run("移除存在的客户端", func(t *testing.T) {
		// 先添加一个客户端
		err := pool.Add("removable_client",
			client.WithAddress(testAddress),
			client.WithAuth(testUsername, testPassword),
		)
		if err != nil {
			t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
		}

		assert.True(t, pool.Has("removable_client"))
		assert.Len(t, pool.List(), 1)

		// 移除客户端
		err = pool.Remove("removable_client")
		assert.NoError(t, err)
		assert.False(t, pool.Has("removable_client"))
		assert.Empty(t, pool.List())
	})

	t.Run("移除不存在的客户端应失败", func(t *testing.T) {
		err := pool.Remove("non_existent_client")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("移除后客户端应被关闭", func(t *testing.T) {
		// 添加客户端
		err := pool.Add("close_test_client",
			client.WithAddress(testAddress),
			client.WithAuth(testUsername, testPassword),
		)
		if err != nil {
			t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
		}

		// 获取客户端引用
		cli, err := pool.Get("close_test_client")
		require.NoError(t, err)

		// 移除客户端
		err = pool.Remove("close_test_client")
		assert.NoError(t, err)

		// 验证客户端已被关闭
		rawClient := cli.GetClient()
		assert.Nil(t, rawClient) // 关闭后应该返回nil
	})
}

// TestPoolHas 测试检查客户端是否存在
func TestPoolHas(t *testing.T) {
	pool := NewPool()
	defer pool.Close()

	t.Run("检查不存在的客户端", func(t *testing.T) {
		assert.False(t, pool.Has("non_existent"))
	})

	t.Run("检查存在的客户端", func(t *testing.T) {
		err := pool.Add("has_test_client",
			client.WithAddress(testAddress),
			client.WithAuth(testUsername, testPassword),
		)
		if err != nil {
			t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
		}

		assert.True(t, pool.Has("has_test_client"))
		assert.False(t, pool.Has("other_client"))
	})
}

// TestPoolList 测试列出所有客户端
func TestPoolList(t *testing.T) {
	pool := NewPool()
	defer pool.Close()

	t.Run("空连接池列表", func(t *testing.T) {
		list := pool.List()
		assert.Empty(t, list)
	})

	t.Run("添加多个客户端后列表", func(t *testing.T) {
		clients := []string{"client1", "client2", "client3"}

		for _, name := range clients {
			err := pool.Add(name,
				client.WithAddress(testAddress),
				client.WithAuth(testUsername, testPassword),
			)
			if err != nil {
				t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
			}
		}

		list := pool.List()
		assert.Len(t, list, 3)

		// 验证所有客户端都在列表中
		for _, name := range clients {
			assert.Contains(t, list, name)
		}
	})

	t.Run("移除客户端后列表更新", func(t *testing.T) {
		// 移除一个客户端
		err := pool.Remove("client2")
		assert.NoError(t, err)

		list := pool.List()
		assert.Len(t, list, 2)
		assert.NotContains(t, list, "client2")
		assert.Contains(t, list, "client1")
		assert.Contains(t, list, "client3")
	})
}

// TestPoolClose 测试关闭连接池
func TestPoolClose(t *testing.T) {
	pool := NewPool()

	t.Run("关闭空连接池", func(t *testing.T) {
		err := pool.Close()
		assert.NoError(t, err)
	})

	t.Run("关闭有客户端的连接池", func(t *testing.T) {
		pool2 := NewPool()

		// 添加多个客户端
		clients := []string{"close_client1", "close_client2"}
		for _, name := range clients {
			err := pool2.Add(name,
				client.WithAddress(testAddress),
				client.WithAuth(testUsername, testPassword),
			)
			if err != nil {
				t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
			}
		}

		assert.Len(t, pool2.List(), 2)

		// 关闭连接池
		err := pool2.Close()
		assert.NoError(t, err)

		// 验证所有客户端都被关闭
		assert.Empty(t, pool2.List())
	})

	t.Run("重复关闭连接池", func(t *testing.T) {
		pool3 := NewPool()

		// 第一次关闭
		err := pool3.Close()
		assert.NoError(t, err)

		// 第二次关闭应该不会出错
		err = pool3.Close()
		assert.NoError(t, err)
	})
}

// TestPoolConcurrency 测试并发操作
func TestPoolConcurrency(t *testing.T) {
	pool := NewPool()
	defer pool.Close()

	t.Run("并发添加客户端", func(t *testing.T) {
		const numGoroutines = 10
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer func() { done <- true }()

				clientName := fmt.Sprintf("concurrent_client_%d", id)
				err := pool.Add(clientName,
					client.WithAddress(testAddress),
					client.WithAuth(testUsername, testPassword),
				)
				if err != nil {
					// 如果是连接错误，跳过测试
					if err.Error() == "failed to create new client" {
						t.Skipf("跳过测试，无法连接到Milvus服务器")
					}
				}
			}(i)
		}

		// 等待所有goroutine完成
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// 验证所有客户端都被添加
		list := pool.List()
		assert.Len(t, list, numGoroutines)
	})

	t.Run("并发获取客户端", func(t *testing.T) {
		// 先添加一个客户端
		err := pool.Add("concurrent_get_client",
			client.WithAddress(testAddress),
			client.WithAuth(testUsername, testPassword),
		)
		if err != nil {
			t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
		}

		const numGoroutines = 20
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer func() { done <- true }()

				cli, err := pool.Get("concurrent_get_client")
				assert.NoError(t, err)
				assert.NotNil(t, cli)
			}()
		}

		// 等待所有goroutine完成
		for i := 0; i < numGoroutines; i++ {
			<-done
		}
	})

	t.Run("并发MustGet操作", func(t *testing.T) {
		const numGoroutines = 5
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer func() { done <- true }()

				clientName := fmt.Sprintf("must_get_client_%d", id)
				cli, err := pool.MustGet(clientName,
					client.WithAddress(testAddress),
					client.WithAuth(testUsername, testPassword),
				)
				if err != nil {
					t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
				}
				assert.NoError(t, err)
				assert.NotNil(t, cli)
			}(i)
		}

		// 等待所有goroutine完成
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// 验证所有客户端都被创建
		list := pool.List()
		assert.GreaterOrEqual(t, len(list), numGoroutines)
	})
}

// TestPoolErrorHandling 测试错误处理
func TestPoolErrorHandling(t *testing.T) {
	pool := NewPool()
	defer pool.Close()

	t.Run("使用无效配置添加客户端", func(t *testing.T) {
		err := pool.Add("invalid_client",
			client.WithAddress("invalid:address"),
			client.WithAuth("invalid", "invalid"),
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create new client")
		assert.False(t, pool.Has("invalid_client"))
	})

	t.Run("MustGet使用无效配置", func(t *testing.T) {
		cli, err := pool.MustGet("invalid_must_get_client",
			client.WithAddress("invalid:address"),
		)
		assert.Error(t, err)
		assert.Nil(t, cli)
		assert.False(t, pool.Has("invalid_must_get_client"))
	})
}

// TestPoolIntegration 测试集成场景
func TestPoolIntegration(t *testing.T) {
	pool := NewPool()
	defer pool.Close()

	t.Run("完整的客户端生命周期", func(t *testing.T) {
		// 1. 添加客户端
		err := pool.Add("lifecycle_client",
			client.WithAddress(testAddress),
			client.WithAuth(testUsername, testPassword),
		)
		if err != nil {
			t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
		}
		assert.True(t, pool.Has("lifecycle_client"))

		// 2. 获取客户端并使用
		cli, err := pool.Get("lifecycle_client")
		assert.NoError(t, err)
		assert.NotNil(t, cli)

		// 3. 验证客户端可用
		rawClient := cli.GetClient()
		assert.NotNil(t, rawClient)

		// 4. 移除客户端
		err = pool.Remove("lifecycle_client")
		assert.NoError(t, err)
		assert.False(t, pool.Has("lifecycle_client"))

		// 5. 验证客户端已关闭
		rawClient = cli.GetClient()
		assert.Nil(t, rawClient)
	})

	t.Run("混合操作场景", func(t *testing.T) {
		// 添加多个客户端
		clients := []string{"mixed1", "mixed2", "mixed3"}
		for _, name := range clients {
			err := pool.Add(name,
				client.WithAddress(testAddress),
				client.WithAuth(testUsername, testPassword),
			)
			if err != nil {
				t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
			}
		}

		// 使用MustGet获取已存在的客户端
		cli, err := pool.MustGet("mixed1")
		assert.NoError(t, err)
		assert.NotNil(t, cli)

		// 使用MustGet创建新客户端
		newCli, err := pool.MustGet("mixed4",
			client.WithAddress(testAddress),
			client.WithAuth(testUsername, testPassword),
		)
		if err != nil {
			t.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
		}
		assert.NoError(t, err)
		assert.NotNil(t, newCli)

		// 验证所有客户端都存在
		expectedClients := append(clients, "mixed4")
		for _, name := range expectedClients {
			assert.True(t, pool.Has(name))
		}

		// 移除部分客户端
		err = pool.Remove("mixed2")
		assert.NoError(t, err)
		assert.False(t, pool.Has("mixed2"))

		// 验证剩余客户端
		remainingClients := []string{"mixed1", "mixed3", "mixed4"}
		for _, name := range remainingClients {
			assert.True(t, pool.Has(name))
		}
	})
}

// BenchmarkPoolOperations 性能测试
func BenchmarkPoolOperations(b *testing.B) {
	pool := NewPool()
	defer pool.Close()

	// 添加一个测试客户端
	err := pool.Add("bench_client",
		client.WithAddress(testAddress),
		client.WithAuth(testUsername, testPassword),
	)
	if err != nil {
		b.Skipf("跳过测试，无法连接到Milvus服务器: %v", err)
	}

	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := pool.Get("bench_client")
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Has", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			pool.Has("bench_client")
		}
	})

	b.Run("List", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			pool.List()
		}
	})
}
