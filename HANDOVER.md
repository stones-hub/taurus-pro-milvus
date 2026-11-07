# 交接

更新：2026-11-07。

## 最新状态（T10 请求驱动契约·组件阶段，未提交）

以本节为准；下文"T10"各条为当时执行器状态的历史记录，其中"未运行任何测试"等表述不代表当前状态。

- 已验证：请求驱动契约组件候选（"请求驱动恢复契约·组件阶段"条目所述改动）已由Pi实际执行 `gofmt -l`、离线 `go vet`、`go test`、`go test -race -count=3`，全部PASS；全新独立复审无阻断项（`taurus-x/.pi/evidence/T10-milvus-request-contract/review-summary.json`），提出5个非阻断项。
- 复审意见已收口：仅文档注释与测试，未改恢复逻辑/API/依赖。Pi重新执行gofmt、离线vet/test/race-count3全部PASS，证据 `taurus-x/.pi/evidence/T10-milvus-request-contract-polish/PI-VALIDATION.md`。
- 生成项目脱敏日志接入已完成候选测试及独立复审（无阻断）。真实跨请求验收PASS：同进程/同客户端A在Milvus停机期间10002ms期限失败，provider安全日志恰好1条；保持停服至少10秒再启动，总空闲20秒包含启动时间；B Search11692ms数据正确，Query172ms完整128条，B无新增失败日志。Pi独立核日志和资源清理。证据 `taurus-x/.pi/evidence/T10-milvus-cross-live/PI-VERDICT.md`。不能凭B耗时宣称触发内部重建，不能以本次覆盖所有故障。
- 当前15个Go/module文件与实服快照逐项hash一致。尚未commit/push/发布；基线HEAD `e89479d`。仅隔离候选replace验证，不是公开版本无replace交付。只读核GitHub tags最高v0.2.0，v0.3.0尚未占用（查询时事实）；建议v0.3.0体现默认恢复/预算/Close行为变化，正式交付动作须分别授权。

## 目标与结论

用户要求验证公共Milvus组件能否生产使用，并已明确授权修复确认的问题。以下为v0.2.0之前的历史整改记录，当时基线HEAD为 `2b6742518b584256d45a1ee1592cf8758663e565`；当前请求驱动恢复候选状态以顶部最新状态为准。

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

用户已明确授权commit、推送main、打tag及正式GitHub Release，选择v0.2.0。发布代码提交 `2a18203` 已推送origin/main；annotated tag `v0.2.0` 已推送并指向该提交；正式Release已创建：https://github.com/stones-hub/taurus-pro-milvus/releases/tag/v0.2.0 。本交接的后续文档状态提交不改变发布tag。未部署服务，113未安装或重启任何服务。

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
- 当前没有运行中的验收/编码任务；本次修复验收及v0.2.0源码发布完成。发布说明 `docs/releases/v0.2.0.md`。下一步可在使用方指定v0.2.0接入，不需要为了验收部署服务；若再修改代码需重新验证并发新版本，不移动现有tag。

## T10 Milvus只读恢复编码（2026-11-06，未提交）

方案 `taurus-x/docs/T10-Milvus只读恢复方案.md`；用户授权编码、独立复审、隔离验收，未授权commit/push/发布/部署。执行器Claude Code opus。改动仅在 `pkg/milvus/client`（client.go、options.go、新增recovery.go/recovery_test.go）及两个README；pool.go、gate.go、go.mod未改。

- 新增 `WithReadRecovery(bool)`（当时默认关闭；已由下方"默认启用"改为默认启用）与 `WithReadRecoveryObserver`；仅Search/Query启用。可恢复错误按码判断：gRPC Unavailable、merr 1/2/503（503即实测 `channel distribution is not serviceable`；505见下方窄修）。间隔5秒、自调用开始60秒预算，服从更短ctx，Close终止恢复。
- 客户端对象不变，内部按代际替换SDK客户端：同代并发失败共享一次重建（握手受ctx限时，在门禁外进行）；替换持写门禁，旧代经本库在途请求结束后才关闭旧客户端；重建期间UseDatabase切库则作废新客户端。内部替换不调用对外Close。
- 加载：只在Loading时轮询等待，从不发起Load/Release；NotLoad交由查询返回服务端错误。方案里的“必要加载”据此收口为“等待服务端恢复既有加载”，未发起重复Load。实测证据无法分离重复Load的影响，需隔离验收确认。
- 失败返回 `*ReadRecoveryError{First, Last}`；成功恢复时首次失败仅经observer获得；本库无日志。启用恢复后不得让GetClient裸句柄与Search/Query并发使用，已在README写明边界。
- 新代码直接import `milvus/pkg/v2/util/merr`（已是间接依赖，未新增模块）；go.mod里的 `// indirect` 标注因此过时，`go mod tidy` 会改动go.mod。go.mod不在本阶段授权范围，未改。
- 执行器无Bash，**未运行任何测试/fmt/vet**；须由Pi执行 `gofmt -l`、`go vet ./...`、`go test ./pkg/milvus/...`、`go test -race -count=3 ./pkg/milvus/...`。随后还需独立复审及按方案做真实重启隔离验收。
- Pi首轮核验（`taurus-x/.pi/evidence/T10-milvus-read-recovery/PI-FIRST-REVIEW.md`）：vet、测试、race通过，但发现阻断项：首次readOnce不受60秒预算和Close约束。同会话窄修：启用恢复时，预算与Close取消在首次门禁等待/首次查询前即生效；首次被截断返回ReadRecoveryError(Exhausted/closed)，调用方ctx先到期仍原样返回；未启用路径不变。恢复期间UseDatabase切库时停止恢复（ErrReadRecoveryDatabaseChanged），不再改到新库重试或查加载状态。原测试“切库后在新库重试成功”编码了不安全行为，已改为更严格的断言。新增回归：首次RPC阻塞、首次门禁等待、调用方ctx更短、Close取消首次查询、未启用时Close仍等待在途、加载等待中切库。GetClient文档改为：启用恢复时不提供并发安全保证，重新获取指针也不能保护在途调用。observer文档补充：耗时不计入预算，错误文本原样，需调用方脱敏。本轮同样未由执行器运行测试。
- 独立复审（`taurus-x/.pi/evidence/T10-milvus-read-recovery-review1/summary.json`，有条件通过）后同会话窄修2，执行器逐项核对源码后修复：
  - F1：dial返回SDK实际解析的库名，与首次查询所用库不一致（如URL带库且UseDatabase("")）时放弃候选，并以ErrReadRecoveryDatabaseChanged停止。
  - F2：预算到期时只把ctx引起、关闭或仍可恢复的错误归一为Exhausted，其他真实错误原样保留。
  - F3：ReadRecoveryError的Unwrap只返回Last（**契约变化**：errors.Is不再命中First）；新增GRPCStatus，只依据Last（Exhausted/超时→DeadlineExceeded，取消→Canceled，其余沿用gRPC码或Unknown）。
  - F4：重建记录库名；builder先在读门禁下预检库，切库结果由同库等待者共享，不再各自建连。
  - F5：加载状态查询返回其实际使用的代际，下一轮据此重建。
  - F6：Close在stop后等待已开始的重建结束（不持门禁、ctx已取消，不互锁），返回前候选客户端已关闭或已安装；服务端感知断开仍是异步的。
  - F7：README写明以下约定：Close的包装/非包装错误；observer同步、可并发、不计预算、panic外传、未启用时忽略、文本需脱敏；SDK自带写RPC重试；裸句柄与恢复并发构成数据竞争；服务端宕机时阻塞拨号；包结构。
  - 新增回归：URL带库空库名、并发共享切库结果、预算到期保留真实错误、GRPCStatus映射、加载状态失败重建新代际、Close等待未安装候选。
  - 未解决：go.mod的indirect标注（不在授权范围）；恢复后空结果的NumQueries==nq需真实服务验证；Milvus服务端对空库头使用default库、错误码1/2/503在2.6.5服务端一致，均来自复审推断，尚未读服务端源码确认。本轮未由执行器运行测试。
- 505窄修（2026-11-06，未提交）：真实4channel同客户端docker stop/start后Search 12040ms返回merr 505 `channel tsafe stalled`，observer 0（证据 `taurus-x/.pi/evidence/T10-milvus-live-client/docker-stop-run.out`）。`isRecoverableReadErr` 仅增加 `errors.Is(err, merr.ErrChannelTSafeStalled)`（merr按码匹配），未用字符串匹配、未泛化到全部retryable码；间隔5秒、自调用开始60秒预算不变且不重置，仅Search/Query，默认关闭、写不重试、库隔离与Close不变。重连不保证修好服务端tSafe，持续505到期以Exhausted失败。新增测试：分类正例（直接/包装/Status往返/未标retriable）与负例（504/506/507、retriable未知码、文本含tsafe）；Search/Query遇505恢复且observer保留首次错误；持续505预算耗尽；更短ctx与Close截断；未启用及Delete遇505不重试；NonRecoverable表加504/506。fake仅加可选 `deleteErr` 字段。执行器未运行测试；Pi随后在505候选执行vet、test、race全部PASS，独立review无阻断项。
- 默认启用只读恢复（2026-11-06，未提交）：用户确认公共组件目前无其他项目使用，不为假设的兼容负担保持默认关闭；授权窄修/测试/复审，未授权commit/push/tag/发布。执行器Claude Code opus。
  - 实现：`DefaultOptions()` 设 `ReadRecovery: true`。`New`、`NewWithOptions`、池默认工厂（`client.NewWithOptions`）因此无需 `WithReadRecovery(true)` 即具备恢复；`WithReadRecovery(false)` 明确关闭，选项仍按顺序后者覆盖前者。恢复逻辑（recovery.go）、间隔5秒/自调用开始60秒预算、可恢复码（gRPC Unavailable、merr 1/2/503/505）、写不重试、库隔离、gate与Close均未改；pool.go、gate.go、go.mod未改；未改SDK重试。
  - 裸SDK：库内非测试代码不调用 `GetClient()`、不缓存裸指针；仅集成测试 `flushCollection` 串行使用（不与Search/Query并发），bin示例不使用。故未改代码，只更新GetClient注释与README：默认下裸句柄不透明恢复、不得与Search/Query/Close/UseDatabase并发、不宜长期缓存，需要时另建 `WithReadRecovery(false)` 客户端。
  - 测试：新增 `TestReadRecoveryDefaultOptions`（默认true、显式false、顺序覆盖）；`TestReadRecoveryEnabledByDefault` 覆盖NewWithOptions未传开关（Search 503、Query 505均实际重建：Connect计数2→3、底层句柄替换、旧连接关闭、observer首次错误）、New便利入口（merr 1重建且认证头保留）、默认路径不可恢复错误原样返回与Delete不重试；`pkg/milvus/pool_default_test.go` 经 `NewPool().MustGet` 真实默认工厂遇503重建后Search成功（包外无法缩短间隔，按真实5秒等待一次）。原 `TestReadRecoveryDisabledByDefault` 改为 `TestReadRecoveryDisabledExplicitly`（显式false，原断言全部保留）；gate_test中涉及Query的 `TestShortDeadlineNotBlockedByLongWrite`、`TestCloseWaitsForInflightAndRejectsNew` 改为默认启用/显式关闭两种模式各跑一遍，断言不变。
  - 文档：两个README改为默认启用，写明默认Search/Query单次最多60秒、`WithReadRecovery(false)` 关闭、裸SDK边界。本仓库无CLI开关；组件默认值即生效。
  - 状态：执行器无Bash，**未运行任何测试/fmt/vet**，待Pi执行 `gofmt -l`、`go vet ./...`、`go test ./pkg/milvus/...`、`go test -race -count=3 ./pkg/milvus/...`。真实验证边界：此前显式开启候选的503实服恢复14.802秒PASS属于旧候选，默认开启尚待新的真实验证；505专项与Query独立真实恢复仍未覆盖。
- 请求驱动恢复契约·组件阶段（2026-11-06，未提交）：用户授权完整落实契约（非临时补丁），本阶段仅组件，生成项目日志接入由后续阶段串行进行。执行器Claude Code opus。
  - observer覆盖：启用恢复时Search/Query除首次直接成功外的每个最终出口在返回前恰好回调一次，新增覆盖首次即不可恢复、调用方ctx超时/取消（含首次门禁等待中）、Close后被门禁拒绝；这些情况错误仍原样返回（不包装），`ev.Err`与`ev.FirstErr`为同一值。被预算/Close截断与进入恢复后的出口不变（`*ReadRecoveryError`）。显式false仍从不回调。未新增开关/API/日志库。
  - `Attempts`语义改为"越过门禁（及库校验）后实际执行查询的次数"：首次门禁等待即失败为0；重试时门禁失败或切库校验失败不再计入；计入不保证到达服务端（如Search参数本地构建失败）。`ReadRecoveryError.Attempts`同义。文档写明事件不脱敏，生成项目只应记录Op/Collection/Attempts/Elapsed及errors.Is/status.Code分类，不输出错误原文；README示例改为只打状态码（已由下文2026-11-07收口更正：默认不记Collection，分类用errors.Is白名单）。README补"请求驱动：无后台重连/健康轮询/自动重放"。
  - 实现仅改 `recovery.go` 的 `read` 出口与事件/错误注释；`refresh` 只加未导出测试钩子 `onShare`（生产为nil，调用方开始等待他人发起的重建前调用，用于确定性通道同步）。复核refresh/rebuild/Close生命周期未发现需修复的真实缺陷，未改client.go/gate.go/pool.go/go.mod。
  - 新增测试：`TestReadRecoveryNextRequestRecoversAfterFailure`（Search/Query × 等待间隔/重建握手 × 内部预算/调用方deadline/cancel共12例：A及时失败、未安装、握手连接释放、building清除；300ms空闲窗口内包装层拨号/Connect/查询/加载状态/Load计数不变；B以新ctx先在旧连接遇503，经真实重建在新握手连接上成功——fake按TCP peer判定，旧句柄与gRPC内部重连不能使其成功）；`TestReadRecoverySharedRebuildInitiatorCanceled`、`TestReadRecoverySharedRebuildWaiterCanceled`（通道确认等待者已在共享等待后取消发起者/等待者，断言及时返回、接替/不受影响、拨号者序列、数据正确、building清除、连接数）；`TestReadRecoveryObserverEveryExit`（各出口恰好一次、Err一致、首次错误保留、Attempts边界含0与参数本地失败）；`TestReadRecoveryDisabledNeverObserves`。等待间隔阶段的cancel用"首次查询服务端返回后100ms"触发，结果断言（已包装、Attempts=1、未拨号）校验所处阶段。
  - 状态（执行器当时）：执行器无Bash，**未运行任何测试/fmt/vet**。组件此前的503实服成功与default observer 0均不构成本次"A失败→空闲→B恢复"验证。待办：Pi gofmt/vet/test/race；全新独立Claude复审；生成项目脱敏日志接入；经另行确认的实服验收；公开版本无replace交付。
  - 后续：Pi已对该候选实际执行gofmt、离线vet/test/race-count3全部PASS；全新独立复审无阻断（`taurus-x/.pi/evidence/T10-milvus-request-contract/review-summary.json`），非阻断项见下条收口。
- 复审意见收口（2026-11-07，未提交）：执行器Claude Code opus，新会话。只改注释、README、测试与本文件；`read`/`refresh` 等实现逻辑、公开API、测试钩子、go.mod均未改，未新增后台恢复。
  - 日志字段：`recovery.go`（ReadRecoveryEvent注释及Collection字段注释）、`options.go`（WithReadRecoveryObserver注释）、`client/README.md`（observer约定及示例）不再把 `Collection` 列为可直接记录的字段：它是调用方原值、未校验/转义，可能含租户/用户标识或换行，默认不记；确需区分时由调用方映射为自定义安全标识，仅转义不等于脱敏。默认只记固定Op、错误分类、Attempts、Elapsed，不输出错误原文。写明 `status.Code` 无法精确分类Milvus业务码（merr 1/2/503/505多为Unknown），须用 `errors.Is` 白名单；README示例删去 `ev.Collection` 与 `status.Code` 打印，改为 `classify` 白名单函数（status.Code仅作传输层兜底）。
  - `testFailThenNextRecovers`：等待间隔阶段A的间隔由10秒改为 `waitPhaseInterval`=600ms（大于300ms预算/deadline）；空闲窗口改为 `max(idleWindow, A所用间隔+100ms)`，即等待间隔阶段700ms、握手阶段仍300ms（`idleWindow` 未缩短）；窗口后新增断言 `generation()==0` 与 `building==nil`，计数断言不变。由此若A取消后把等待转入后台继续，其到期后的拨号/重建会落在窗口内被发现。
  - 取消时机：去掉 `time.Sleep(100ms)` 与后台取消goroutine。A改在独立goroutine执行（结果经缓冲通道返回），测试goroutine在收到 `firstRead` 后用 `require.Eventually` 在 `c.gate.mu` 下确认 `c.gate.readers==0`（首次读门禁已释放）再 `cancelA()`；握手阶段收到 `handshaking` 后取消。require只在测试goroutine调用；测试提前失败时 defer cancelA 使A结束。原有已进入恢复、Attempts=1、拨号/Connect/查询计数、未安装、连接释放等断言全部保留。
  - `requireOneEvent`：包装错误分支增加 `ev.Err == err`、`ev.FirstErr == rerr.First` 身份比较；原样返回分支由 `assert.Equal` 深比较改为 `ev.Err == err`、`ev.FirstErr == err` 身份比较。其他断言保留。
  - 新增 `TestReadRecoveryObserverEveryExit/{Search,Query}/首次门禁等待中Close`：测试持写门禁，`require.Eventually` 在 `gate.mu` 下确认调用已在门禁队列（`len(queue)==1`）后Close；接受原样 `errClientClosed`（须为同一值）或 `Last` 为它的 `*ReadRecoveryError`（First须为ctx错误）两种结果；断言恰好一次事件（经 `requireOneEvent` 的身份比较）、Attempts=0、查询RPC为0、Connect仍为1；释放写门禁后Close返回nil、`gate.isClosing()`、`rec.life.Err()!=nil`、事件仍为1。写门禁释放经 `sync.OnceFunc` 并defer，失败路径不会卡住Close/Cleanup。
  - 状态：执行器无Bash，**未运行任何测试/fmt/vet**，待Pi重验 `gofmt -l`、`go vet ./...`、`go test ./pkg/milvus/...`、`go test -race -count=3 ./pkg/milvus/...`。未做实服验证。

续接：“继续taurus-pro-milvus交付，先读HANDOVER和生产整改验收报告。”
