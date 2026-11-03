# 首轮候选复审记录

2026-11-03。新会话 Claude Code review：session `fc55c164-d780-486c-bd9b-1721f0235bfa`，请求 sonnet，wrapper 证明模型 claude-sonnet-5-5。只读静态复审，不替代运行验收。

## 已确认需要窄修

- 旧 TestDataOperations 在 L2 IVF_FLAT 索引上要求 IP/COSINE 成功，与新增负向测试矛盾。必须为不同 metric 准备匹配索引并验证结果，不能重新忽略 metric 或弱化正确的负向断言。
- parseParamValue 的 ParseFloat 接受 NaN/Inf，SDK 搜索构造忽略 json.Marshal 错误，导致 params 可能变成空字符串；应保证参数能正确编码，增加请求级测试。
- 测试资源必须在创建后立即注册失败兜底清理，使用独立有界 cleanup context；测试应能报告真实清理失败，而不是仅流程末尾删除或忽略所有错误。初始 TestNew 的连接失败后仍 defer nil.Close 也应修正为 require 后再使用。
- 负向字段/metric 测试应核对领域错误，避免超时/断连也能让测试通过。

## 首轮真实集成结果（已核验日志）

临时证据 `/tmp/taurus-pro-milvus-candidate-Yv4XAk`：普通集成及race两轮各89叶子pass、1fail、0skip；唯一失败为修改别名。Pi核查SDK `NewAlterAliasOption(alias, collectionName)`，本库传反参数，已交原Claude窄修。

独立consumer核心8叶子pass、0fail：双字段正确ID，flush后metric不匹配正确报错，range参数实际约束结果，50并发同名同实例，失败连接隔离，超时取消与重建，Remove/Close创建竞争和终态，暂停后原客户端恢复。

旧DataOperations距离类型用例实际两轮通过，不能按静态预期记失败；flush后consumer明确得到expected=L2/actual=IP。需将旧测试改为可确定的匹配metric与sealed数据路径。

非有限参数确实触发服务端JSON空输入并等待约6秒；不能因负向测试出现任意error就判质量合格。

30次不存在DB握手失败后，独立进程goroutine从4增至184；profile有30reader和30keepalive，已获得运行泄漏证据。用户已授权修复，该问题交原Claude按本地拦截器捕获握手ClientConn并失败关闭的最小方案处理，不升级依赖；须再验清理及正常连接不误关。

## 后续边界

SDK 2.6.5 在 grpc.DialContext 成功、connectInternal 失败后没有关闭连接：已直接读取依赖源码确认。New 返回 nil/error 导致本组件无法取得 SDK client 句柄。需要真实握手失败压力/连接证据再决定最小修复方案；不能宣称已解决。当前禁止依赖升级，若补救需要修改该边界需先明确方案。

客户端写锁跨 RPC、裸 GetClient 与 Close 并发等为原有额外风险，未在首轮最小修复中重构。bin 示例仍有旧113地址，属于示例而非正式库默认配置，本轮原任务禁止改bin；不可执行该示例连接旧服务器。

## 独立复验状态

Pi 已检查全部 diff/新增单测，普通测试、race count=3、vet通过；上述真实集成与consumer日志已直接读取核验。首轮容器/卷/网络已清理，工装证据暂保留待最终收尾。原Claude同session窄修已启动（agent 452da1fd-e083-46c），本记录不代表候选通过。任何后续代码/测试修正均须重跑受影响验收。
