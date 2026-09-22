# Pod 清理隔离集群验收记录

日期：2026-09-22。环境见 [2026-09-22-environment.md](2026-09-22-environment.md)。本记录只覆盖 Pod 残留回收，不把 Drone Server 状态补偿、任务接管或重放计入验收。

## 结果表

| 组别 | 场景 | 结果 | 关键证据 |
| --- | --- | --- | --- |
| 基线 | 正常短任务 | 通过 | build #1 success；Pod `drone-89xctpv9xenag8yruryl`，UID `6be2de06-d392-4f52-b13c-12a4a894642c`；成功后约 2 秒确认 Pod 消失，清理记录结案删除 |
| 1 | Secret Create 返回 503 | 通过，按未知创建保留诊断 | build #2 error；两个 runner 均 Ready、restartCount=0；任务资源未发现；记录 `970717b1-d398-4e79-a66d-f0f5fc34c319` 为 Closed=true、Secret Unknown=true、Done=false |
| 1 | Pod Create 返回 503 | 通过，按未知创建保留诊断 | build #3 error；两个 runner 均 Ready、restartCount=0；任务资源未发现；记录 `400e9d66-f99a-4d36-af4e-4dd30aae15c0` 的 Pod Unknown=true、Done=false |
| 1 | Secret Create 标准 Kubernetes 403 Status | 通过 | build #4 error；代理只观察到一次 POST=403，无透明重发；约 2 秒后该任务的新记录结案删除；runner 未重启 |
| 2 | Pod DELETE 首次 503 | 通过 | build #5 success；Pod `drone-5i96pntsxc6vi7xd0xk9`；10:30:02.946Z 首次 DELETE=503，10:30:07.249Z 重试 DELETE=200，之后 Pod 消失 |
| 2 | Pod DELETE 持续 403 | 通过 | build #7；Pod `drone-1kub2mp0421drbslyeem`，UID `5e714e6a-49e4-4190-b2a3-ca2e410bd370`；记录 `415badb6-7e35-440c-9f63-9808ddc84c22` 保留，Closed=true、Done=false、LastError=Forbidden；解除故障后回收 |
| 2 | 未知 Create 晚到并跨 runner 重启 | 通过 | build #6；记录 `451b7d95-7c0f-48ef-b173-eda4cd7f9f5e`；Pod `drone-rdh8pv5epai670d5slce`，attempt `7d1d1daa-bb73-4d8c-bb92-493832cc623f`。10:31:53.599Z 请求被 hold；owner 容器 `6cee...288b` 被 SIGKILL；10:32:19.387Z 入站取消；10:32:24.408Z 先观察 GET=404；10:32:24.956Z 后台独立 POST=201；10:32:29.953Z DELETE=200；10:32:34.402Z GET=404，Pod 和记录均删除 |
| 3 | 正常取消，同时阻塞日志上传并拒绝 Secret DELETE | 通过 | build #8；Pod `drone-z17q04ihud8i7entn3h9`，UID `c410d0fc-91f6-4c2d-a924-de8cd754f880`。有效取消触发 10:36:34.371658Z；10:36:34.381Z 日志 upload 已阻塞；Pod DELETE 10:36:34.560Z=200；Secret DELETE 持续 403；Pod 在 31.576 秒消失，小于 60 秒 |
| 3 | 本地 timeout | 通过 | build #9；Pod `drone-lxu5l6jbt0tcqdf0hd8c`，UID `9e254a3c-15bf-4a2f-a1a8-234a49527c60`；deadline 10:39:31Z；10:39:31.441Z DELETE=200；10:40:04.222Z GET=404，deadline 后约 33.223 秒消失 |
| 3 | 全部 `/rpc/` POST/PUT/GET 阻塞时本地 timeout | 通过 | build #13；Pod `drone-5e4nyc1v9h07dsnchs7r`，UID `a012cfc0-c484-4825-99be-78a89e6438ef`，attempt `ab18dad2-ad4f-40f9-b258-d5d419b78c3c`；deadline 10:44:44Z；Pod 于 10:45:15.627Z 消失，deadline 后 31.573 秒，小于 60 秒。Server 终态不作为本项条件 |
| 4 | 两个真实 runner、旧容器终止事实、Lease、活跃任务隔离 | 通过 | build #11/#12 同时运行。崩溃目标 Pod `drone-9pskzm4i6l3p0yjbzj0l`，UID `1a61e8fc-9ffa-4fe5-83b5-4af8f8c9e78d`；owner 旧容器 `4172...a372` 于 10:42:13Z 以 exitCode=137 终止；Lease holder 为另一个 runner 会话 `3f2a75ba-1006-4b0a-ac6e-f47cdb4b0164`；10:42:15.627Z Pod DELETE=200，10:42:51.259Z GET=404，记录随后删除。另一 Pod `drone-2zomzuzfslh4lth50foj`，UID `eb5fd7d5-d9bb-483c-9f48-bec662f0a6f6`，在 10:43:26Z 仍为 Running，未被回收；build #11 最终 success |
| 4 | UID 不符不误删 | 通过 | synthetic 记录指向 `uid-mismatch-sentinel`，记录 UID 为 `2222...2222`，真实 UID `c3602bac-37c0-43d6-9118-63da9abc3057`；多个补偿扫描周期后 Pod UID 未变、deletionTimestamp 为空、记录保留 |
| 4 | 共享 Secret 不误删 | 通过 | synthetic 已关闭记录指向无 cleanup 归属标签的 `shared-sentinel`，真实 UID `2be16052-04bd-420f-8c7d-aff54902d4ae`；补偿日志明确 `ownership or UID mismatch; retained`，Secret 未进入删除状态 |
| 4 | 执行者终止证据不足不误删 | 通过 | 未关闭记录 `drone-cleanup-evidence-insufficient-real` 的 owner Pod 不存在；补偿日志持续为 `pods "missing-owner" not found`，未把 Pod 不存在当成死亡证据；`evidence-sentinel` UID `622772d3-26ca-444d-b711-b6afe1f965f7` 未进入删除状态 |

## 故障代理边界

代理部署为 `drone-lab/fault`：8443 代理 Kubernetes API、8080 代理 Drone RPC、9090 为仅本机 port-forward 使用的控制端口。runner 使用 `drone-lab/fault-ca` 的 `ca.crt`，保留真实 projected ServiceAccount token。代理事件只记录 UTC 时间、请求方法、路径、结果、状态码和规则名，不记录请求体、headers、Secret 或 token。

晚到 Create 只有在原入站请求 context 已取消且代理真实观察到对应对象 GET=404 后，才用独立 background context 向真实 API 提交一次 POST。代理对 Kubernetes 拒绝返回标准 `v1.Status` JSON；流式响应每次写入都会 Flush；删除 block 规则会释放在途请求。

## 保留项与边界

- build #2/#3 的 503 对客户端属于提交结果未知，两个诊断记录按设计保留，不能因代理知道实际未创建就假报结案。
- build #6/#10/#12 的 Drone 页面可继续显示 running；资源已回收，本期不补偿 Server 状态。
- 三个安全负例使用的 sentinel 与 synthetic ConfigMap 在取得证据后由测试命令人工删除；人工删除不计入产品自动回收结果。
- 观察到任务 clone 步骤/容器实际使用 `drone/git:latest`；本报告不把预导入镜像标签当作实际运行版本证据。

故障代理源码和最小控制示例保留在 [fixtures/fault-proxy](fixtures/fault-proxy/README.md)，不含测试 CA、私钥或凭据。
