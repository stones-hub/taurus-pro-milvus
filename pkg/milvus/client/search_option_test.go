package client

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// searchKVs 构建搜索请求并返回 SearchParams 键值对
func searchKVs(t *testing.T, vectorField string, metric entity.MetricType, params map[string]string) map[string]string {
	t.Helper()
	opt, err := buildSearchOption("coll", []string{"p1"}, []string{"text"},
		[]entity.Vector{entity.FloatVector([]float32{0.1, 0.2})}, vectorField, metric, 7, "id > 0", params)
	require.NoError(t, err)
	req, err := opt.Request()
	require.NoError(t, err)
	assert.Equal(t, "coll", req.GetCollectionName())
	assert.Equal(t, []string{"p1"}, req.GetPartitionNames())
	assert.Equal(t, []string{"text"}, req.GetOutputFields())
	assert.Equal(t, "id > 0", req.GetDsl())

	kvs := map[string]string{}
	for _, kv := range req.GetSearchParams() {
		kvs[kv.GetKey()] = kv.GetValue()
	}
	return kvs
}

func TestBuildSearchOptionPassesParameters(t *testing.T) {
	kvs := searchKVs(t, "vec_b", entity.COSINE, map[string]string{"nprobe": "10", "ef": "64", "radius": "0.5", "flag": "true", "mode": "fast"})

	assert.Equal(t, "vec_b", kvs["anns_field"])
	assert.Equal(t, "COSINE", kvs["metric_type"])
	assert.Equal(t, "7", kvs["topk"])
	assert.JSONEq(t, `{"nprobe":10,"ef":64,"radius":0.5,"flag":true,"mode":"fast"}`, kvs["params"])
}

func TestBuildSearchOptionDefaults(t *testing.T) {
	kvs := searchKVs(t, "", "", nil)

	assert.Equal(t, "", kvs["anns_field"])
	assert.Equal(t, "", kvs["metric_type"])
	assert.Equal(t, "{}", kvs["params"])
}

func TestBuildSearchOptionMetricTypes(t *testing.T) {
	for _, m := range []entity.MetricType{entity.L2, entity.IP, entity.COSINE} {
		assert.Equal(t, string(m), searchKVs(t, "vector", m, nil)["metric_type"])
	}
}

func TestBuildSearchOptionRejectsNonFiniteParams(t *testing.T) {
	for _, v := range []string{"NaN", "nan", "Inf", "+Inf", "-Inf", "inf", "infinity"} {
		_, err := buildSearchOption("coll", nil, nil, []entity.Vector{entity.FloatVector([]float32{1})},
			"vector", entity.L2, 1, "", map[string]string{"radius": v})
		require.Error(t, err, v)
		assert.Contains(t, err.Error(), "radius")
	}

	// 超出范围的数值保持为字符串，仍可序列化
	kvs := searchKVs(t, "vector", entity.L2, map[string]string{"x": "1e999"})
	assert.JSONEq(t, `{"x":"1e999"}`, kvs["params"])
}

func TestSearchRejectsNonFiniteParamsBeforeRPC(t *testing.T) {
	// cli 为 nil：若未在本地拒绝而发起 RPC 会 panic
	c := &client{}
	_, err := c.Search(context.Background(), "coll", nil, nil, []entity.Vector{entity.FloatVector([]float32{1})},
		"vector", entity.L2, 1, "", map[string]string{"radius": "NaN"})
	assert.Error(t, err)
}

func TestAlterAliasRequestOrder(t *testing.T) {
	req := newAlterAliasOption("my_collection", "my_alias").Request()
	assert.Equal(t, "my_alias", req.GetAlias())
	assert.Equal(t, "my_collection", req.GetCollectionName())
}
