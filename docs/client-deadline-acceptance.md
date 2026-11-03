# 当前未提交候选最终运行验证

结论：本次要求的本机范围通过。未改正式文件，未commit/push/部署，未访问113/SSH/生产。不是旧30分钟负载的重新认证，也不代表全生产场景无风险。

## Pi最终核验

Pi直接核查集成JSON各157 test pass/0fail/0test skip、summary关键断言、manifest与当前工作区全部Go/mod/LICENSE摘要一致；独立执行普通test、race count=3、vet、diff --check通过。最终独立Claude review session `e093fb2a-977b-440e-a1c9-dd4b86538795`（wrapper证明claude-sonnet-5-5）正常路径无确定性缺陷。编码session `ef665e14-0d38-4128-8a12-c75fa0edbb85`。报告及必要结果、参考工装固化于 `docs/verification/deadline-final-2026-11-03/`。

本轮业务改动：新增 `pkg/milvus/client/gate.go` 可取消FIFO读写门禁，`client.go`所有本库RPC使用门禁且defer归还；Close先拒绝新调用并唤醒等待者，再等在途操作完成安全关闭；UseDatabase失败恢复SDK内部库名，初始库记录SDK解析后的config.DBName以兼容URL。`gate_test.go`覆盖读写、公平、取消竞争、Close、URL库回滚、Await释放及分阶段错误契约。未改依赖。

**结果有效范围：**本轮deadline与状态整改验收通过。较早候选的认证/TLS、30分钟结果仍保留为历史证据，本候选未重新全量安全专项或30分钟负载。Close仍无取消参数、可等在途长操作；裸GetClient需要调用方协调Close/UseDatabase；mTLS及SDK WithRetry限制仍在。不能把此结论扩展为全版本/全生产负载承诺。

下文/tmp地址属于执行时历史位置；必要证据固化后临时工装及合成模块缓存已定向清理，不作为下次续接依赖。

## 候选身份

新目录 /tmp/taurus-pro-milvus-gatefinal-sJercB；读取前轮 REPORT 和 consumer 源码后沿用全部原场景。
从当前工作区全部Go源码（含未跟踪文件和测试）+go.mod/go.sum/LICENSE重新生成manifest和模块ZIP，非HEAD/非旧ZIP。
版本 v0.0.0-candidate.gatefinal.sjercb，通过file GOPROXY安装，无replace。consumer-module.log提供实际安装Dir/Sum。
候选摘要 b624d3fe085fc938197277887f3d5946eb5670a2d3c92b3bbeee13a1c9d3e53c。
ZIP SHA256 00061504bcc6bb1bf31326929628c8ddf4b007a9dbc32b1a04c26368ce9ee989。
audit.json证明zip_matches_manifest=true、workspace_matches_manifest=true；candidate-unchanged.json证明原测试期间Go/mod未变；Await补项后再次audit也一致。
Go版本 go1.27.3 darwin/arm64。

## 环境及全量测试

专属 milvusdb/milvus:v2.6.5，DEPLOY_MODE=STANDALONE，嵌入etcd、local storage；专属network/volume，仅127.0.0.1:55165->19530和127.0.0.1:55166->9091。Await补项另建独立同版本容器/卷/网络，前一套清理后才复用固定loopback端口。
MILVUS_INTEGRATION=1，MILVUS_TEST_ADDRESS指向专属服务。
- go test -json -count=1 -timeout=15m ./...：退出0，157 test run /157 test pass，0 test skip。
- go test -race -json -count=1 -timeout=15m ./...：同上，退出0，无race告警。
- go vet ./...、git diff --check：退出0。
每个JSON有1条bin包级skip（no test files），不是跳过测试。

## 独立consumer原场景

race二进制最终退出0，complete failures=0。主consumer实测源码快照在primary-consumer-source/；consumer/为追加Await补项的源码，主测试在追加前已经build，不冒充在主305秒阶段执行过Await补项。

真实docker pause下，20秒长LoadCollection和CreateIndex持写门禁。其间500ms Has/Query/Search全部严格errors.Is(context.DeadlineExceeded)，501.546–502.259ms，全部<=650ms。主动cancel后0.060–0.101ms返回context.Canceled。等待UseDatabase同样严格errors.Is deadline且<=650ms。
Close在另一独立客户端长Load期间执行：GetClient及时nil，新Has精确client is closed（<1ms）；Close必须等待在途完成，unpause后长写/Close成功，重复Close成功。未换实例绕过。

UseDatabase成功切库、正常不存在库失败回滚、default不见marker均通过。不存在库要求明确database not found，不接受任意错误。
URL http://127.0.0.1:55165/gate_actualdb + WithDatabase("")初始marker=true；切不存在库返回database not found；随后原库marker仍=true、err=nil，前轮回滚缺陷本轮未复现。
显式gate_actualdb覆盖无效URL path；显式default覆盖有效URL path，marker正确不可见，均通过。
真实暂停服务后UseDatabase RPC 502.757ms返回codes.DeadlineExceeded，errors.Is context=false，严格status.Code断言通过；恢复后原库marker=true。
错误契约核查见SDK-CONTRACT.md，含当前README快照和实际SDK源码位置。门禁context与RPC status显式区分，不以任意error算成功。

## 恢复与混合负载

1024条32维确定性向量，Flush、IVF_FLAT/L2/nlist16、Search nprobe8 topK5；Query校验ID100及每个向量元素，Search top1=100/score0。
同容器/卷、原client和原pool实例；恢复先原client.LoadCollection，不用只读探针绕开写门禁。
- docker restart --time 30：14.4304秒恢复。
- docker kill KILL / start：15.8124秒恢复。
故障Has共179次，86个预期deadline/Proxy未ready错误，最大502.273ms，0超预算；pool existing179次0错且实例不变。

全新305.043746秒混合负载：8并发Search/Query（每次100ms间隔）、每秒Insert/Delete、每5秒New/Has/Close。共23271请求：Search16950、Query5650、Insert305、Delete305、New/Has/Close61。正常错误0、超预算0，最大25.848ms。两个5秒基线共32762请求0错。
30秒采样10次goroutine恒20、gRPC reader恒1；关闭后goroutine3、reader0。RSS从首个soak样本99312KiB至末样本103616KiB（峰105616），Milvus405.9–408.8MiB。只说明本轮观测，不能外推长期无泄漏。逐请求和分位数见events.jsonl/summary.json。

## 真实Await补项

await/为第二套专属服务原始日志及race consumer。先同样建立数据，再在原公开Client.LoadCollection调用期间抓取真实goroutine栈，确认milvusclient.(*LoadTask).Await已进入后pause；栈保存在await/goroutine-real-await.txt。
随后500ms Has等待门禁500.501ms严格context deadline返回；主动cancel在0.241ms返回gRPC codes.Canceled（errors.Is context=false）；unpause后Has、LoadCollection、Query再次成功，证明门禁释放；退出0、failures=0。
该实证覆盖进入Await后取消和释放，不声称真实观察到加载进度长期停在0。进度一直不完成后timeout及释放的确定性回归由TestAwaitTimeoutReleasesGate覆盖，普通/race两轮均pass（audit.json可查）。SDK Await select ctx.Done也可直接返回ctx.Err，轮询RPC则可返回status；不应断言所有Await错误固定一种包装。

## 异常、边界与清理

初始化第一次因复制consumer目录已存在导致FileExistsError，尚未启动容器；仅修/tmp工装mkdir(exist_ok=True)后继续，setup.log原样保留，无正式修复、无弱化断言。未出现候选测试失败。
两轮收尾unpause退出1因为服务本就未paused，日志保留；docker rm/volume rm/network rm均退出0。结束后独立docker列表核实对应容器/卷/网络全空，pgrep工装路径无残留进程，cleanup-verification.json保存查询。
原始日志、两套consumer工装、模块ZIP和manifest按要求保留供Pi核验；不清全局模块缓存。未改HANDOVER/README等正式文件。
未重新做认证/TLS、HA/全量数据checksum/长期容量；单机短负载不能替代生产认证。
