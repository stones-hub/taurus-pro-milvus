# 窄修第二轮记录（Claude Code 执行，未覆盖既有文档）

基线 HEAD 2b67425，未 commit。

## 修复
- AlterAlias：SDK `NewAlterAliasOption(alias, collectionName)`，库内原先传反；改用 `newAlterAliasOption` 包装并加 request 级回归。
- 握手失败泄漏：SDK `New` 在 DialContext 成功、Connect 失败时不关连接且不返回句柄。库内追加 `grpc.WithChainUnaryInterceptor`，仅在 `milvusclient.New` 执行期间记录首个 RPC 的 `*grpc.ClientConn`；`New` 失败则关闭，成功后清除引用、不再捕获。本机 fake gRPC 服务器回归覆盖 RPC 错误、业务错误状态、握手超时、成功后连接保留与认证头、Unimplemented Connect 被容忍；禁用关闭逻辑后这些回归失败（已变异验证）。
- `params` 中 NaN/Inf 本地拒绝并返回错误，不再发送空 params。
- 测试：metric 用例为每种 metric 建匹配索引并 flush，校验自身检索得分，负向用例要求领域错误（超时/取消/Unavailable 不算）；所有资源创建时即注册 `t.Cleanup`，使用独立 30s context，清理失败使测试失败；KEEP_TEST_DATA=true 才保留。

## 新增风险 / 未验证
- 真实 Milvus 集成未运行；负向用例期望的错误关键字（`metric`、`anns_field`、`no_such_field`/`not found`）按 2.6.5 的已知文案假设，需 Pi 实测。
- 拦截器方案依赖 SDK 在握手期间经由 DialOptions 链路发出首个 RPC（2.6.5 源码确认）；SDK 升级后需重跑 handshake 回归。DisableConn 时无 RPC，也无泄漏路径。
- 测试 fake 服务器引用 `milvus-proto/go-api/v2`（go.mod 中为 indirect，未改 go.mod/go.sum；`go mod tidy` 会把它改为 direct）。
- 客户端写锁跨 RPC、GetClient 与 Close 并发等既有风险仍未处理。
