# 生产准入验证记录（2026-11-03）

## 结论

当前基线 `2b6742518b584256d45a1ee1592cf8758663e565` 不通过本轮生产准入验证。未修改正式业务代码或测试，未提交、推送或部署。不能用原有测试绿灯替代公开 API 契约验证。

## 环境与方式

- Go 1.27.3，darwin/arm64；Docker Engine 29.6.1。
- 本机隔离 Milvus `milvusdb/milvus:v2.6.5`，镜像 digest `sha256:fb57464669737ee9a9c43c6faa891527adab601241b0b488091c5f4eb4a13178`。
- 官方 embedded standalone 参数依据：https://github.com/milvus-io/milvus/blob/v2.6.5/scripts/standalone_embed.sh 。仅发布 loopback 端口 29539/29099。
- 没有连接 192.168.103.113，没有 SSH。用户允许必要时在 113 安装临时 Milvus并清理，但本轮无需使用。
- 原有测试在临时源码副本中运行，仅将两处硬编码测试地址换成本机地址；业务实现未变。
- 独立 consumer 从当前 HEAD 打包的本地 GOPROXY 制品安装组件，通过公开 API 验证；不是只在包内测试。临时合成版本仅作测试标签，不是正式发布。
- Pi 直接核查了代码、子任务实际命令输出、consumer 测试代码与失败日志，并再次检查资源清理。

## 结果

| 检查 | 结果 |
| --- | --- |
| 编译所有包和测试（不执行测试） | 通过 |
| go vet ./... | 通过 |
| go test -json -count=1 -timeout=120s ./... | 66 个叶子用例通过，0 失败，4 跳过 |
| go test -race -json -count=1 -timeout=120s ./... | 66 个叶子用例通过，0 失败，4 跳过，无 race 报告 |
| 独立 consumer：go test -race -v -count=1 -timeout=90s ./... | 1 测试通过，2 失败，无 race 报告 |
| govulncheck v1.9.0，source/symbol | 退出 0，可达符号漏洞 0，模块版本命中 1 |

原有测试包含父测试时为 90 pass、4 skip；不应将父子计数混为独立场景。四处跳过分别为主动跳过默认连接、test_db 不存在、custom_db 不存在、修改别名冲突。初次隔离网络配置导致宿主端口不可达、测试 nil panic 和工具超时，属于失败的环境尝试；修正隔离网络后才得到上述两轮有效结果，不计入有效通过统计。

## 上线阻断项

### 1. Search 丢弃三个公开参数

位置：`pkg/milvus/client/client.go:444`。

`vectorField`、`metricType`、`params` 没有用于构建 SDK SearchOption。真实双向量字段集合中，SDK 指定 b 字段正确返回期望 ID，本库明确传入 b 却返回：

`multiple anns_fields exist, please specify a anns_field in search_params`

单向量集合传入不存在字段、非法 metric、非法参数时均成功，结合源码证明契约未实现。非法参数是否被服务端检查不能仅从成功响应推断；双字段失败是直接运行证据。

### 2. 同名 MustGet 存在业务竞争

位置：`pkg/milvus/pool.go:62`、`:79`。

先 Get 后 add 不是原子语义。50 个 goroutine 获取同一名称，实测 4 个返回 `client same already exists`，未取得共享客户端。race 检测无告警不能排除这类业务竞争。

### 3. 建立失败连接阻塞整个池

位置：`pkg/milvus/pool.go:79-89`、`pkg/milvus/client/client.go:111`。

池持有全局写锁，以 Background context 加阻塞 Dial 建立连接；公开 Add/MustGet 不接受调用方 context。对未监听的本机 29540 端口执行 Add 后，Get/Has/List/Close 全部等待。独立进程观察 6 秒无操作返回，SIGQUIT 堆栈确认 DialContext 及锁等待后终止进程。有限观测不等于实测无限等待，但实现没有总 deadline。

## 其他风险与覆盖边界

- 暂停隔离容器期间，带 3 秒 deadline 的写 RPC 超时返回；Close 等待超过 1 秒，直到写操作释放锁后完成。关闭后 GetClient 为 nil、操作返回 client is closed。恢复服务后新 client 请求成功。
- 上述只是暂停/恢复，不是进程崩溃、原连接自动重连或数据持久化恢复验证。
- SDK 当前实现中的限流重试 interceptor 被注释；WithRetry 的实际契约尚未运行验证。
- govulncheck 数据库时间 2026-11-02T22:01:43Z，命中 GO-2026-5932（x/crypto v0.58.0 中 OpenPGP 包）。finding 仅含 module/version，没有 package/function 可达链，不能报告为本组件可达漏洞，也不能说依赖完全无告警。
- 尚未验证认证/TLS、集群/高可用、长期压测、崩溃后恢复、真实重启后的原连接恢复；未运行 benchmark，不做性能上线背书。
- README 的“自动负载均衡”描述与当前按名称存取客户端的池实现不符，需要后续同步。

## 清理与下一步

专属容器、数据卷、网络、临时源码/consumer/日志/工具、合成模块缓存及阻塞测试进程已清理；Pi 再次查询专属容器/卷/网络为空，临时目录不存在。未执行全局镜像清理（不影响其他工作区缓存）。

下一步先确认最小修复方案及编码授权：补齐 Search 参数、修正同名 MustGet、为建连增加可控超时/取消并缩小锁范围、将测试配置与集成入口隔离；然后重做公开 API 回归和未覆盖关键场景。不得将修复方案确认视为 commit/push/部署授权。
