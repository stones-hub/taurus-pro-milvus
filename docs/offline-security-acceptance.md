# 离线认证与单向TLS验收

2026-11-03。当前未提交修复候选，基线HEAD 2b6742518b584256d45a1ee1592cf8758663e565。此项为离线专项结果，不代表长稳/重启任务已完成。

## 结果

真实Milvus2.6.5启用认证及单向TLS，独立Linux consumer安装当前工作区模块制品，无replace，正式代码未修改。最终3个独立测试进程均PASS、0FAIL、0SKIP（不是3个叶子场景；每个进程内有多个断言）。

- 正确合成用户名密码：建连与实际RPC成功。
- WithAPIKey传入用户名:密码格式token成功；错误token及缺失认证均精确返回Unauthenticated。
- 30次错误密码全部Unauthenticated。goroutine初始4，正常连接基线10，30次失败后仍10，reader/keepalive各1；正常客户端随后3次RPC成功，全部关闭回到4、reader0、keepalive0。
- 临时普通账号及空角色可以建连，但创建数据库精确返回PermissionDenied，服务端记录PrivilegeCreateDatabase拒绝。
- WithTLS及https自动启用TLS：仅隔离Linux进程通过SSL_CERT_FILE信任测试CA后，建连及RPC成功。未改宿主信任库、无InsecureSkipVerify。
- 默认不信任测试CA时，标准TLS探针断言x509.UnknownAuthorityError；信任CA但主机名错误时断言x509.HostnameError；本组件两个场景均拒绝连接，约1.2秒内由deadline结束（上界3秒）。组件自身并非返回上述x509类型，不混淆探针与组件结果。
- 取消/已过期deadline的认证请求返回对应状态，随后正常请求仍成功。

## 环境与候选身份

Go1.27.3，darwin/arm64交叉构建CGO_ENABLED=0 GOOS=linux GOARCH=arm64；Docker29.6.1，Milvus2.6.5 arm64，Alpine3.21测试消费者。专属internal网络内互连，服务重启后未实际发布宿主端口，未访问113/SSH/生产。

临时模块版本v0.0.0-security.20261103；ZIP SHA256 1f9369477b37fe33c0dd741741a6fc0c7b468739f3d902b2344a731f5ee95779。测试结束全部Go/mod文件摘要未变，Pi再核查对应manifest。

主要命令形态：先从本地GOPROXY安装当前候选，交叉编译go test -c，分别启动trusted、untrusted、mismatch三种独立consumer容器。可信模式仅该容器进程设置SSL_CERT_FILE，负向模式不关闭校验。合成密码、证书私钥不进入仓库。

Pi读取报告及三份最终运行日志，核对PASS、资源回落和权限拒绝记录，确认专属Docker资源为空。

## 异常与限制

首次服务启动遇默认HTTP/gRPC共端口与TLS冲突，关闭临时服务未使用的HTTP后成功；临时工装编译错误已在/tmp修正，不属于组件缺陷。服务端历史日志包含多轮尝试，不能把总认证告警数当成最终30次循环数量。

当前公共选项未暴露SDK WithTLSConfig，不能直接指定RootCAs、ServerName、客户端证书；私有CA只能依靠运行环境信任配置，本次已验证该路径。mTLS未支持/未测。grpc.WithBlock掩盖证书拒绝根因为deadline，诊断需额外TLS探针或日志。

未做本专项race、FD统计、密码轮换、证书过期/撤销、内部TLS/REST/集群验证。长稳和重启由另一任务验证，不能合并成此处已通过。

## 清理

专属容器/卷/网络和进程已清理，Pi已再次查询为空。含合成密码/私钥的临时目录在核验后删除，不原样归档；仅保留本脱敏结论。未修改宿主信任库、未全局安装工具。
