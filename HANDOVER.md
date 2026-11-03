# 交接

更新：2026-11-03。

## 目标与结论

用户要求验证公共Milvus组件能否生产使用，并已明确授权修复确认的问题。当前基线HEAD `2b6742518b584256d45a1ee1592cf8758663e565`，修复仍在未提交工作区。

**最新deadline与数据库状态整改候选本机验收通过**，最新报告 `docs/client-deadline-acceptance.md`；前轮整改报告 `docs/production-remediation-acceptance.md` 及其他离线报告为历史阶段结果，不可冒充最新候选全量重验。不是已发布、已部署或所有生产场景认证。原始缺陷报告保留在 `docs/production-readiness-2026-11-03.md`，属于修复前历史证据，不代表当前候选状态。

## 已完成及实际证据

- Search参数传递、同名MustGet竞争、连接池全局建连阻塞修复。
- 真实集成发现AlterAlias参数顺序反了和SDK握手失败泄漏，已窄修；NaN/Inf本地拒绝、测试配置与清理及领域断言完善。
- 原Pool接口未扩展，新增ContextPool；默认创建超时10秒，Close终态。go.mod/go.sum未变。
- Pi检查全部差异和新增测试，普通测试、race count=5、vet、diff --check通过。
- 第二轮真实Milvus2.6.5：全套集成及race各101叶子pass、0fail、0skip；独立consumer核心8pass、别名/非法参数7pass、握手失败专项1pass，全部无race告警。
- 30次不存在DB握手失败后goroutine保持正常连接基线10，正常连接3次RPC成功，关闭后回到初始4，reader/keepalive归0。
- Pi直接核验原始日志，并比对manifest全部Go及模块文件摘要一致；文档在验收期间更新不影响候选源码身份。
- 必要结果、manifest、consumer参考工装已固化至 `docs/verification/2026-11-03/`；最终报告说明命令与范围。

## 执行器与复审

Claude Code，经run-claude，请求sonnet，wrapper证明实际模型claude-sonnet-5-5。
编码session `186cd6c3-61dc-4bb6-92bd-2f085da1d5c5`（首轮及同任务窄修）；独立新会话复审session `fc55c164-d780-486c-bd9b-1721f0235bfa` 和最终 `ad319189-c86d-4f92-adc3-1461864c1e22`。最终重点范围未发现确定性缺陷。

## 清理与权限

未访问113/SSH/生产服务；本机专属Milvus容器/卷/网络已删除，Pi再次核实。三处本次/tmp证据工装目录、合成模块缓存清理完成；未删除其他项目资源。

用户已明确授权commit、发布、打tag及Release，并选择v0.2.0；范围包括推送main和tag、创建正式GitHub Release，不包括服务部署。发布执行中，最终状态以Git/GitHub核验为准。无服务需要在113重启（未部署）。

## 新启动：离线专项验证

用户确认无线上接入场景，希望不上线完成验证，并已同意继续。已启动两个只读运行任务：认证/TLS `0084b797-debd-493`；真实重启/30分钟持续负载及并发基线 `3ec891a3-c6cc-4e4`。计划见 `docs/offline-qualification-plan.md`。两路仅用本机独立Docker和临时consumer，不改正式源码，不访问113。认证/TLS已完成并由Pi读取最终日志及manifest核验，脱敏报告 `docs/offline-security-acceptance.md`：3独立进程全部通过，认证/权限/不可信证书/主机名/可信CA路径通过，30次错误认证资源不增长。注意组件只暴露TLS开关，不支持公共选项配置mTLS或自定义TLSConfig，证书错误组件层表现为deadline。专属服务和含测试凭证/私钥的/tmp目录均已清理，Go/mod摘要与候选一致。
重启/30分钟持续负载任务已完成，Pi直接核验summary/events/manifest，报告 `docs/offline-soak-acceptance.md`：原客户端restart约13.527s、kill/start约15.831s恢复，Flush后ID100向量及搜索结果一致；30分5秒139179请求无正常错误，含阶梯基线正常窗口567443请求0错误，故障77错误另计。客户端goroutine/reader稳定，Milvus内存缓增，不能断言长期无泄漏。
重要新实测问题：首轮LoadCollection持写锁时，500ms Has有60次等待1.5–1.8s超预算，最终只读探针通过不代表已修。已保留 `docs/verification/offline-soak-2026-11-03/lock-deadline-overruns.json`。离线验收不能无保留全通过；需向用户说明并确认继续最小修复及重新验证。
测试服务已清理，Pi核实专属Docker资源为空；临时工装和合成模块缓存核验固化后定向清理。

## 剩余边界及下一步

- 未测真实认证/TLS、HA/集群、崩溃恢复、长期性能/容量，不能直接宣称全场景生产就绪。
- 客户端写锁跨RPC/Await，关闭可能等待在途请求；裸GetClient使用需调用方协调关闭；当前SDK WithRetry配置不生效（README已说明），生产调用必须设置deadline。
- 本次改动后未重新跑govulncheck；同日修复前扫描可达0、模块版本告警1，依赖未变，不能当成候选重扫。
- bin示例仍含旧113地址，本轮未执行或修改，不影响正式库/集成默认入口，但发布前可另做示例配置化。
- 用户已明确授权修复新超时问题。新任务 `.pi/tasks/client-deadline-remediation.md`、方案 `docs/client-deadline-remediation-plan.md`；Claude Code新会话编码已启动，agent `485a524d-69e1-43f`，请求sonnet（真实模型待wrapper结果），不resume上一任务。
- 简化方向：保留必要读写互斥，将普通RWMutex门禁换成可取消等待门禁，而非盲目删除跨RPC同步；Close先禁止新请求并唤醒等待者，再安全等待在途完成；UseDatabase保持隔离。新确定性回归/独立review/真实原失败场景必须重验，旧30分钟结果不冒充新候选验证。
- 新任务编码已完成，Claude session `ef665e14-0d38-4128-8a12-c75fa0edbb85`，wrapper证明真实模型claude-sonnet-5-5。证据 `/tmp/taurus-pro-milvus-deadline-LjBoUh/code.json`。新增gate.go/gate_test.go并替换client同步，UseDatabase失败回滚SDK状态，文档说明裸SDK生命周期边界。Pi已检查gate完整实现、client入口及SDK UseDatabase源码；普通test、race count=3、vet、diff --check通过，依赖未变。
- 独立review已完成：session `1e584ace-e761-465a-9d32-2c8986279aa5`，claude-sonnet-5-5，正常构造gate路径无确定性缺陷；指出零值Close panic后done不关闭的低危加固、参数负向断言及Await/cancel-grant覆盖缺口。
- Pi核查SDK config.parse发现真实潜在回滚缺陷：DBName为空时SDK从Address URL路径填config.DBName，本库却记录options.DBName；URL库连接后UseDatabase失败可能回滚到空/default而非原库。已通过steer要求运行agent补公开API复现，不提前改候选。
- 真实验收已完成，Pi读REPORT核查：全部集成/race各149条test run、0测试skip，原长写门禁500ms请求500.230–502.299ms返回、cancel及时，Close安全；restart14.185s、kill/start14.894s原连接恢复，305.024秒23303请求正常0错。候选仍不通过：URL初始库回滚确定性失败；另RPC UseDatabase超时为grpc DeadlineExceeded而非errors.Is context（区分RPC和门禁语义，不统一更改现有RPC契约）。
- 证据 `/tmp/taurus-pro-milvus-gate-IPQAv2` 保留待最终固化/清理，服务资源已清理。同Claude session窄修已完成（证据 `/tmp/taurus-pro-milvus-deadline-LjBoUh/narrow.json`）：config.DBName跟踪、URL/显式库优先回归、nil Close/defer done通知、NaN具体错误与门禁释放、fake Await阶段超时释放、cancel-grant竞争及两阶段错误契约。Pi已读新增测试及实际源码，独立普通test、race count=3、vet、diff --check通过，依赖未变。
- 最终真实验收已完成并由Pi直接核查日志和当前Go/mod/LICENSE manifest一致：全量集成/race各157条测试pass、0fail/0test skip（bin无测试只有包级skip）；URL回滚与显式库优先通过；500ms读等待501.546–502.259ms返回；Close安全；真实Await进入后取消/释放通过；restart14.430s、kill/start15.812s原连接恢复；新305.044秒23271请求正常0错/0超预算。最新报告 `docs/client-deadline-acceptance.md`，结果及参考consumer在 `docs/verification/deadline-final-2026-11-03/`。
- 最终独立review session `e093fb2a-977b-440e-a1c9-dd4b86538795`，wrapper证明claude-sonnet-5-5，正常路径未发现确定性缺陷。
- 本任务三处/tmp工装/执行器证据目录及两个合成模块缓存已定向清理；必要证据先固化。专属容器/卷/网络已删除，Pi再次核实查询为空；未碰113。较早认证/TLS及30分钟结果属于旧候选，本次未重跑这些完整专项，必须明确边界。
- 当前没有运行中的验收/编码任务；本次修复验收完成。用户已授权v0.2.0提交/推送/tag/Release，发布执行中；未授权服务部署。发布说明 `docs/releases/v0.2.0.md`。

续接：“继续taurus-pro-milvus交付，先读HANDOVER和生产整改验收报告。”
