# 生产准入整改：本机候选验收

日期：2026-11-03。基线 HEAD `2b6742518b584256d45a1ee1592cf8758663e565`，验收对象为当前未提交工作区，不是旧 HEAD 制品。

## 结论与边界

本轮最小整改的本机功能、并发、故障和连接资源回归通过；原报告记录的阻断项及后续确认的别名/握手泄漏问题已修复并实测。尚未提交、推送、发布或部署，不等于生产环境验收完成。

未覆盖真实认证/TLS、HA/集群、崩溃重启持久化、长期压力及容量。仍存在客户端写锁跨 RPC/Await（关闭需等待在途操作）、裸 GetClient 使用方需自行协调 Close、SDK固定重试与 WithRetry 配置不生效等边界。不能宣称全场景生产认证或任意负载下可用。建议调用方始终设置请求 deadline，并先小流量灰度；认证/TLS和容量要求需按接入环境另验。

## 实际改动

- `pkg/milvus/client/client.go`：Search 传递向量字段、metric及ANN参数，本地拒绝不可序列化非有限数；修正AlterAlias SDK参数顺序；握手阶段捕获gRPC连接，SDK New失败关闭连接，成功清除捕获引用且保留正常连接。
- `pkg/milvus/pool.go`：网络建连移至全局锁外；同名pending共享结果；默认10秒有界创建；新增ContextPool保持原Pool接口不变；取消、移除、关闭后的失去归属连接不发布并清理；Close终态。
- `pkg/milvus/pool_unit_test.go`、`client/search_option_test.go`、`client/handshake_test.go`：增加确定性池竞争/取消/超时/清理、请求构造、握手失败和正常连接回归。
- `pool_test.go`、`client/client_test.go`：集成测试显式环境配置，连接失败必须失败；自行准备唯一资源并以独立有界context清理；匹配metric索引及flush验证，领域错误断言排除传输失败。
- README及包文档：移除自动负载均衡承诺，说明池语义、Close终态、集成入口和当前SDK的WithRetry限制。go.mod/go.sum未改。

## 验收证据

Go 1.27.3 darwin/arm64，Milvus 2.6.5专属容器，只有loopback端口。没有访问113或生产服务。

Pi亲自检查完整改动和新增测试，直接运行普通test、race count=5、vet及diff --check通过。两轮独立Claude Code新会话复审；最终session `ad319189-c86d-4f92-adc3-1461864c1e22`，wrapper证明模型claude-sonnet-5-5，未发现本轮重点范围确定性缺陷。编码session `186cd6c3-61dc-4bb6-92bd-2f085da1d5c5`。

第二轮真实测试结果（叶子计数）：

| 组 | 通过 | 失败 | 跳过 |
| --- | ---: | ---: | ---: |
| 全套集成 | 101 | 0 | 0 |
| 全套集成 + race | 101 | 0 | 0 |
| 独立consumer核心 + race | 8 | 0 | 0 |
| consumer别名转移与非有限参数 + race | 7 | 0 | 0 |
| 30次握手失败释放专项 + race | 1 | 0 | 0 |

集成命令（当时隔离服务地址）：

`MILVUS_INTEGRATION=1 MILVUS_TEST_ADDRESS=127.0.0.1:59465 go test -json -count=1 -timeout=240s ./...`

`MILVUS_INTEGRATION=1 MILVUS_TEST_ADDRESS=127.0.0.1:59465 go test -race -json -count=1 -timeout=240s ./...`

consumer重新打包当前工作区制品，不使用replace、不使用旧HEAD包。临时版本 `v0.0.0-candidate.round2.4wdwai`，ZIP SHA256 `a7dec53f6ba9ec4bf3b6db4ddcb643483ca9274e048b6abcd3bb1e64af0e038b`。版本只供本地验收，不是正式发布。验证期间仅文档有更新，所有.go和go.mod/go.sum未变。

关键实际结果：

- 双向量字段分别返回预期ID；flush后索引metric不匹配正确报领域错误；range参数实际改变结果。
- 50并发同名MustGet全成功同实例；坏连接不影响已有名称；短deadline/主动取消/重新创建、Remove/Close创建竞争及关闭终态通过。
- 别名转移通过真实集合ID确认从A转到B。
- radius/range_filter的NaN/Inf/-Inf六例在服务暂停时仍15–123微秒本地返回invalid search param，无服务端JSON重试。
- 30次不存在DB握手失败：初始goroutine 4，正常连接基线10，30次失败后仍10，reader/keepalive始终各1；正常连接随后3次RPC成功，关闭后回到4、reader0/keepalive0。此前失败版本为4->184，现已消除该复现场景泄漏。
- 暂停恢复后原客户端仍可调用；这不是崩溃恢复测试。

Pi直接读取原始JSON核实结果计数、异常参数及连接回落证据。必要记录和consumer工装保留在 `docs/verification/2026-11-03/`；`.go.txt`为回放参考工装而非正式模块测试，可放入独立临时consumer模块执行。candidate-manifest仅代表打包时身份，其中README/HANDOVER后续文档变动属预期，Go文件摘要应一致。

## 安全扫描

本轮代码改动未重新扫描漏洞库；改动前同日govulncheck v1.9.0可达符号0、模块版本级OpenPGP告警1（无可达链）。依赖版本未变化；不把旧扫描当成最终候选重新扫描结果。

## 清理

本次所有专属容器/卷/网络/子进程已退出或删除，Pi再次查询核实。结果固化后，Pi已删除三处本次/tmp目录以及两轮合成模块的解压与下载缓存，再次确认目录不存在、专属Docker资源查询为空。验收manifest中的全部Go源码/测试及模块文件摘要与最终候选一致。未清理其他项目的Docker资源或全局缓存。113未安装任何服务。
