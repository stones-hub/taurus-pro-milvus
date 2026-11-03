# 客户端等待deadline修复方案

用户2026-11-03已明确授权修复。基线为当前未提交完整候选，HEAD2b67425。

## 目标

已实测读请求500ms deadline被写操作持锁拖至1.5–1.8s。修复context不能约束本库锁等待的问题，不承诺无context的Close立即取消在途请求。

## 最小实现方向

不简单移除跨RPC同步：现有同步还保证UseDatabase与其他操作互斥，以及SDK Close不与RPC读写conn/service竞争。优先替换为可取消读写门禁，保持读并发、写互斥，避免不必要的连接生命周期重构或新公共选项。门禁等待阶段观察ctx.Done及关闭信号，不为每个等待者启动无法取消的锁goroutine；需处理公平性与写饥饿。

Close先禁止新调用并唤醒等待者，之后等在途本库操作释放再关闭SDK，SDK关闭只能执行一次。GetClient仅获取状态，关闭开始后返回nil；裸SDK句柄仍要求调用方自行协调生命周期。UseDatabase保持完整写入互斥，取消等待不得改变库状态。

## 验证

fake gRPC确定性阻塞长写/读/切库场景，短deadline及主动取消及时返回且不发RPC；Close等待时新调用拒绝；无race/泄漏/饥饿。独立复审同步协议，再重跑隔离Milvus全套集成与race，实测原失败模式和变更后的混合负载。原30分钟结果仅属于旧候选，不冒充本次回归。

不提交、推送、部署或访问113，不升级依赖。执行范围详见 `.pi/tasks/client-deadline-remediation.md`。
