# Milvus 真实恢复 / 30分钟持续运行 / 小集合性能基线

## Pi核验与最终判定

Pi直接读取summary及逐请求events，核对候选manifest全部摘要一致、最终请求数及首轮60次deadline超预算记录。结果和consumer参考工装已固化在 `docs/verification/offline-soak-2026-11-03/`，首轮失败证据 `lock-deadline-overruns.json`。下文/tmp路径为执行时历史位置，核验后临时目录和合成模块缓存已清理，不依赖其续接。

**判定：恢复探针与限定30分钟稳定性测试通过，但离线资格验证并非无条件全通过。锁等待不遵守调用方deadline已被真实复现，属于待整改的可靠性问题。不得因最终工装避开写锁竞争而抹掉首轮失败。**本轮只读验证，没有修该问题；应先确认后续最小锁生命周期修复方案，再由正式编码执行器处理并重验。

## 有限定的测试结果
最终有效测试通过：同一个原客户端、同一池注册实例，在真实 docker restart 后13.527秒恢复，在 docker kill --signal KILL 后 docker start 同数据卷15.831秒恢复；已Flush的确定性探针ID100、32维向量逐元素及Search top1 ID100/score0恢复一致。持续混合运行1805.082秒（30分5.082秒），139179请求，正常窗口错误0，deadline超预算断言0。独立consumer退出0，专属Docker容器/数据卷/网络均删除。

并非所有生产条件通过：首轮揭示已知客户端写锁等待不受context约束，Has可等待约1.5–1.8秒后才返回，即使deadline500ms。这一边界不因最终只读恢复探针通过而消失。Milvus内存持续缓增，30分钟不足以宣称长期无泄漏或生产容量。

## 候选与制品
- HEAD基线2b6742518b584256d45a1ee1592cf8758663e565；测试的是未提交工作区重新打包模块，不是HEAD、旧zip或replace。
- Go源码/测试/go.mod/go.sum/LICENSE清单SHA256：898a9572bfcaa68ba7b09052a4bbad4ed262cce7ef1c4cb1d984c211615e8fcf。
- ZIP SHA256：98a0f0b6844d8ed1d21f9bc7d023d364105433a8b925b46b6bf476b5a925799b。
- 独立consumer版本v0.0.0-candidate.soak.wxsh4f；consumer-module.json证明安装模块缓存，无replace。manifest.json保留每文件摘要，结束重算全部一致。
- Go1.27.3 darwin/arm64；Milvus已有镜像milvusdb/milvus:v2.6.5，镜像ID fb5746466973。参考v2.6.5官方standalone_embed.sh（脚本自身标签仍写v2.6.2，仅沿用embedded配置，不执行脚本）。embedded etcd + COMMON_STORAGETYPE=local + 专属持久数据卷，gRPC127.0.0.1:64369、health127.0.0.1:64370；不发布etcd。

## 工装和实际命令
本目录setup.py负责当前工作区打包和专属资源创建，consumer/main.go为独立公开API工装，run.py真实启动测试进程、外部RSS/Docker采样和2320秒总上限，finally清理专属资源。未改正式源码/测试、未commit/SSH/113/生产、未做认证/TLS（另一路负责）。

打包/启动：`python3 /tmp/taurus-pro-milvus-soak-wXsh4f/setup.py`

构建：`cd /tmp/taurus-pro-milvus-soak-wXsh4f/consumer && GOPROXY=file:///tmp/taurus-pro-milvus-soak-wXsh4f/proxy,https://proxy.golang.org GOSUMDB=off GOWORK=off go mod tidy && GOWORK=off go build -o ../soak-consumer .`

执行：`python3 /tmp/taurus-pro-milvus-soak-wXsh4f/run.py`

统计：`python3 /tmp/taurus-pro-milvus-soak-wXsh4f/analyze.py`

未靠单次sleep等待30分钟；consumer持续做真实RPC，内部30秒采样，外部supervisor每60秒采样，30分钟期间一直运行。最终supervisor总时长1910.154秒，consumer运行约1908.66秒；最终UTC03:55:24.973–04:27:13.631。主soak UTC03:57:04.989–04:27:10.093。

## 确定性数据 / 恢复
创建1024条固定ID0..1023，32维float向量：v0=id/1024，vj=(id%17)*j/1024。Flush.Await成功后创建IVF_FLAT L2 nlist16索引并Load，查询ID100及其向量逐元素，Search nq1/topK5/nprobe8/filter id<1024校验top1=100 score0；重启前后执行相同校验。

| 故障 | Docker动作耗时 | 同一原客户端恢复 | 故障Has错误 | deadline结果 |
|---|---:|---:|---:|---|
| 优雅 | restart --time 30: 1.452s | 13.527s | 7个DeadlineExceeded | 所有故障请求<=502.109ms |
| 非优雅 | kill SIGKILL:0.346s，再start成功 | 15.831s | 5个DeadlineExceeded、65个Proxy not ready | 所有故障请求<=502.191ms |

两个故障窗口分别196、258请求，含Has与pool.MustGetContext已有名称；pool实例地址恒等原实例，已有名称调用无永久池阻塞。500ms请求deadline，预算断言允许150ms调度余量；实际超出名义deadline最大2.191ms，未触及容差。恢复探针每次2秒、整体150秒有界。优雅与非优雅分别记录，使用同一容器、同一数据卷。

明确持久化边界：只验证Flush完成后数据；未验证未Flush数据保证、断电/宿主盘损坏、全部1024行逐一比对、HA/集群故障。两次恢复对确定性ID100/向量及Search结果做精确探针核对，不能说全量checksum已核对。Flush只能通过公开GetClient获取SDK Flush（本库无独立Flush封装），其余用本库公开API。

## 性能基线（只针对本机小集合）
1024基准行、32维、IVF_FLAT L2 nlist16/nprobe8；单实例并发共享池客户端，75% Search nq1/topK5、25% Query ID100，每请求都验证返回结果并调用MustGetContext核对原实例。基线各20秒，无节流；不是纯搜索QPS，不能外推生产容量。

| 并发 | 请求数 | 综合req/s | p50 ms | p95 ms | p99 ms | 最大ms | 错误 |
|---:|---:|---:|---:|---:|---:|---:|---:|
|1|27527|1376.31|0.538|1.627|2.038|6.667|0|
|8|137289|6864.24|0.968|1.949|2.572|11.141|0|
|32|263448|13170.86|2.046|4.180|6.237|33.788|0|

## 30分钟混合运行
8并发共享池读（每worker每请求后100ms节流），另每秒插入一条唯一高ID后立刻Delete、每5秒新建客户端Has后Close。逻辑活数据约1024行+最多1条临时行，物理新增仅1805条插入和1805个删除墓碑，不会无限膨胀。1805.082秒，139179请求，0错误，0预算断言失败。

| 操作 | 次数 | p50 ms | p95 ms | p99 ms | max ms |
|---|---:|---:|---:|---:|---:|
|Search|101406|5.406|7.135|10.261|34.310|
|Query|33802|5.169|6.844|9.992|22.347|
|Insert|1805|4.876|7.595|9.967|19.533|
|Delete|1805|2.420|6.620|9.122|12.790|
|New/Has/Close|361|7.572|11.964|15.850|26.680|

正常窗口（基线+soak）567443请求全部0错误；故障窗口77个预期传输/未就绪错误独立统计，不混作正常错误率。最后再Flush及Query/Search精确核对成功。

## 资源曲线
60次内部30秒采样，活跃阶段goroutine恒20，gRPC reader恒1/keepalive恒1；临时新建客户端可能发生在采样间隔中，因此不能说连接瞬时峰值从未>1。soak结束9 goroutine，池Close后3 goroutine、reader0/keepalive0。没有累计reader/goroutine泄漏证据。

| soak采样 | consumer RSS KiB | goroutine | Milvus内存 MiB |
|---|---:|---:|---:|
|30s|37696|20|345.8|
|300s|37856|20|345.7|
|600s|37920|20|349.5|
|900s|38144|20|356.3|
|1200s|38208|20|358.6|
|1500s|38224|20|368.1|
|1800s|38352|20|371.0|

consumer RSS净增656KiB，Close后RSS不保证归零（Go堆/分配器驻留），Milvus从345.8到371.0MiB增加约25.2MiB，含缓存和写入/删除/后台活动，未归因，不宣称内存已进入平台期。Docker可用内存7.75GiB。summary.json含全部采样及p50/p95/p99，events.jsonl有逐请求证据，supervisor.jsonl有独立外部曲线。

## 首轮异常 / 已知边界
首轮attempt1/完整保留：使用Docker随机发布端口，restart后HostPort由63369变63427，原客户端仍连旧地址，恢复150秒超时。判为工装端口漂移，不判数据丢失。修工装为固定loopback端口后最终恢复通过。

首轮同时使用LoadCollection恢复（持写锁2s等待RPC）与并发500ms Has（读锁），60次Has超过650ms断言，典型约1.5–1.8s；这是当前源码已知锁跨RPC导致deadline不能约束锁等待的真实证据。最终工装改成只读Query恢复探针消除工装主动写锁竞争，但不修正式代码，也不声称该边界解决。正常混合Insert/Delete有同样锁语义，本次实际负载最长34.31ms、未触发预算异常。需主Agent明确汇报此风险。

## 清理及证据
- supervisor终于执行docker rm -f专属容器、docker volume rm专属卷、docker network rm专属网络，全部code0；再次独立查询均为空。
- consumer子进程已退出0；未停止/删除任何其他Docker资源。
- 本目录日志/工装/制品先保留供主Agent核验，未写项目目录。合成模块缓存也暂留以利核验；主Agent验收后可仅删除v0.0.0-candidate.soak.wxsh4f对应缓存及本目录，不要全局clean modcache。
- 主要证据：REPORT.md、manifest.json、summary.json、events.jsonl、supervisor.jsonl、consumer-module.json、docker-inspect-start/end.json、milvus.log、goroutine-*.txt、consumer/main.go、setup.py、run.py、attempt1/。
