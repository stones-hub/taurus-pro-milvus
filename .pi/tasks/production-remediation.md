# 公共 Milvus 组件生产准入整改

## 授权与基线

用户：授权，修复。允许编码及本机隔离测试；禁止 commit/push/部署/SSH/113 访问。基线 2b6742518b584256d45a1ee1592cf8758663e565。初始未跟踪文件 HANDOVER.md、docs/ 是 Pi 的验证报告，必须保留。

Risk: 关键任务（共享 API、并发及生命周期）。
Executor: Claude Code，通过 run-claude，requested model: sonnet。真实模型以 wrapper 响应为准。

先读 docs/production-readiness-2026-11-03.md 和 docs/production-remediation-plan.md 及现有全部源码测试。

## Scope

允许 pkg/milvus/pool.go、pkg/milvus/client/client.go、options.go、相关正式测试与必要新增测试文件、README 文档；必要可新增测试脚本/集成说明。禁止改 go.mod/go.sum、无关 bin、Git 历史，禁止替换或删除已有验证报告。不要修改 Pi 的任务书、HANDOVER。

## 精确实现目标

- Search 当前完全丢弃 vectorField/metricType/params；使用 WithANNSField、WithSearchParam/WithAnnParam 等 SDK 正式契约修复，验证参数实际进入请求；metric 与索引不匹配应遵守服务端语义，不要伪造通过。
- pool.MustGet 当前 Get -> add 竞争；同名并发必须共享已成功创建客户端（或同次失败），Add 保留重名报错。不要单纯在同一全局锁里串行网络操作。
- pool.add 当前全局锁跨 NewWithOptions(Background) 且 grpc.WithBlock 无 deadline。保持 Pool 原方法兼容；优先新增可选上下文接口而非直接扩展 Pool 导致第三方实现编译破坏，可提供 ContextPool interface/函数并说明。需有有界默认创建超时（例如10秒，可配置），调用方更短 context 生效。新配置保持简单。
- 创建可在锁外执行，但预留/发布状态及 Close/Remove 竞争必须严格控制：Close 终态，取消创建并阻止关闭后的发布，关闭多余/失去归属的客户端；已存在客户端 Get 不应被其他名称坏连接阻塞。Close 后返回明确错误，不允许池复活。等待同一创建的调用方能独立取消。
- 测试不再写死 113。约定 MILVUS_INTEGRATION=1 显式开启，MILVUS_TEST_ADDRESS、MILVUS_TEST_USERNAME、MILVUS_TEST_PASSWORD；普通 go test 不连接服务器；显式集成缺配置、连接失败应 Fatal。可使用 build tags，选一种最简单且文档一致方式。仅集成未启用可以 skip，修复原有主动跳过/库不存在/别名冲突等：自行创建唯一资源、清理。不要改弱断言。
- 并发、锁、取消、生命周期需有无需服务器的确定性回归（可用不导出的工厂注入测试替身，但勿暴露不必要公共接口）；原有测试兼容新关闭终态语义。
- 文档删除“自动负载均衡”虚假承诺，说明池是名称注册复用、超时、集成启用及 Close 终态。

## Verify

你先执行 gofmt、go test ./...、go test -race ./...、go vet ./...，不能访问旧硬编码服务器。可以暂不启动 Docker，真实集成将由 Pi 独立重跑，需提供可复现入口。禁止把未跑的集成报已通过。

Pi 后续独立验证：本机隔离 Milvus 2.6.5、真实全量集成及 race、当前工作区打包安装至独立 consumer，覆盖双向量字段+参数、50并发同名、失败连接不阻塞正常池操作、超时/取消、Close/Remove竞争；新会话独立复审。所有测试资源清理。

## 交付

汇报实际文件、关键实现语义、命令结果、未验证项。遇执行器限制/限流/权限失败明确停下，不自行换执行器或重试。不要 commit。
