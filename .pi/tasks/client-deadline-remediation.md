# 客户端锁等待deadline整改

2026-11-03 用户明确授权“好，修复吧”。关键任务：公共API并发、生命周期与数据库切换。

## 基线和边界

HEAD 2b6742518b584256d45a1ee1592cf8758663e565，工作区已有全部上一轮修复及测试、docs/HANDOVER，全部必须保留，不回滚、不提交/推送/部署/SSH，不访问113。执行器Claude Code，请求sonnet，实际模型以wrapper为准。新任务新会话，禁止resume旧编码会话。

范围：pkg/milvus/client/client.go及必要新增内部同步实现、正式测试与README包文档；仅必要时调整相关池回归以验证Close，不升级依赖、不改go.mod/go.sum、不改bin、不扩展不必要公共API。Pi维护HANDOVER/本任务书，执行器不覆盖。

## 已确认事实

看 docs/offline-soak-acceptance.md 和 docs/verification/offline-soak-2026-11-03/lock-deadline-overruns.json。当前client所有写操作持RWMutex写锁跨RPC/Await，读操作持读锁跨RPC。实测LoadCollection持锁2s时Has deadline500ms等待1.5-1.8s；60次超过650ms。原锁保证UseDatabase不与其他操作交错，也保护SDK Close（SDK conn/service置nil并不与调用同步）。不能简单复制SDK指针后解锁或直接移除所有锁。

## 目标和简化优先

要求请求的context覆盖等待本库同步门禁阶段；取消/超时调用及时返回正确context错误，不发后续RPC、不改变DB状态、不泄漏等待goroutine。保留原读并发/写互斥及UseDatabase切换语义是可接受的最小方案：优先实现**可取消等待的读写门禁**，保留RPC期间占用门禁，而不是为消除跨RPC持锁引入复杂SDK生命周期重构。长写操作本身可以阻塞普通请求到其context到期，但短deadline请求不能被迫等待长操作结束。请实际评估公平性/写饥饿，避免每个等待者起goroutine锁普通RWMutex造成隐性泄漏。

GetClient无ctx需返回时仅使用短状态锁，Close开始后尽快返回nil；裸SDK句柄不属于本库同步保护，必须说明调用方不得与Close/UseDatabase并发调用裸SDK。

Close无context，在途操作可等待结束（保留原约定，不承诺即时取消），但必须先标记closing/closed拒绝新操作，不能让短deadline请求排在Close后卡住。Close并发重复调用语义明确，SDK Close仅一次且不得与仍在运行/等待Await的本库RPC竞争；所有waiting操作能被Close唤醒返回closed，而不是等后续创建的读写门禁。若这些目标无法用简洁门禁达成，说明取舍再实现，不可降低目标。确保零值client在现有请求构造负向测试仍能在发RPC前拒绝NaN，不panic或死锁。

UseDatabase等待可取消，失败/取消时不能更新SDK状态。必要核查SDK UseDatabase实际语义，不虚构。数据库切换写门禁需涵盖实际变更。原Pool接口保持兼容。

## 测试

无需真实Milvus的确定性fake gRPC场景：
- 阻塞LoadCollection/CreateIndex或另一个真实写RPC直到释放，短deadline Has/Query/Search在预算内返回context错误，无额外服务端RPC；主动cancel同样生效。
- 阻塞读RPC同时发写RPC，写等待可取消；已取消ctx不发RPC。
- Close正在等待在途长操作时，新请求及时closed、GetClient nil；释放后Close安全完成、重复关闭无race/重复SDK Close。
- UseDatabase与操作互斥、取消不改变库、正常切换后正确库（通过metadata或fake实现可观察）。
- 读并发仍可用、取消等待者/高并发多轮下无goroutine泄漏及写饥饿；对新回归做旧实现变异/反证可行部分。
- 保留上一轮搜索/别名/握手失败释放/pool/认证相关全部回归。

你执行gofmt、go test ./...、go test -race -count=10 ./pkg/milvus/...、go vet ./...。无需启动Docker，Pi将独立重跑真实集成/故障锁竞争和混合负载。不要把未跑的真实验收说通过。

独立review新会话在候选完成后进行，重点门禁取消/公平/Close/DB语义。Pi独立检查diff，重跑普通/race/vet及真实Milvus全部集成，明确以前长稳结果仅属旧候选，变更后重新做有界混合负载。

## 交付

报告改动、同步协议及锁顺序、测试结果、剩余风险。禁止commit。执行器失败/限流/权限拒绝立即停下报告，不重试/自动换模型或执行器。
