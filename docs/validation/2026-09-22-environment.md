# 本地 Pod 清理验收环境

创建日期：2026-09-22。用户已授权创建一次性本地环境及执行隔离故障验收。

## 范围

- 独立 k3d 集群：`drone-cleanup-test`，单节点，节点容器内存上限 3 GiB。
- Kubernetes：`rancher/k3s:v1.35.5-k3s1`，Linux arm64。
- 专用 kubeconfig：`/tmp/drone-cleanup-validation.vJA9AB/kubeconfig`。所有操作显式指定该文件；未切换默认 context，未修改 `lowcode-cloud-local-dev` 集群。
- 工作目录：`/tmp/drone-cleanup-validation.vJA9AB`。测试凭据只保存在其中权限为 0600 的文件及测试集群 Secret 中，不纳入仓库。
- 三个命名空间：`drone-lab`（服务及两个 runner）、`drone-tasks`（流水线 Pod）、`drone-cleanup`（恢复记录及 Lease）。

## 服务

| 服务 | 地址或配置 |
| --- | --- |
| Drone Server | http://drone.localhost:18080 |
| 临时 Gitea | http://gitea.localhost:13000 |
| 测试仓库 | `cleanup/cleanup` |
| Kubernetes API | 仅本机 `127.0.0.1:16550` |
| runner | 当前代码构建的 `drone-cleanup-runner:validation`，两副本，每副本任务并发 1 |
| 重启补偿 | 显式开启，pool 为 `acceptance` |

宿主机仅绑定回环地址；测试集群内对两个 `.localhost` 名称配置专用 DNS 重写，使 OAuth、Webhook 和仓库访问使用一致地址。该配置只存在于测试集群。

Drone Server 固定到镜像索引摘要 `drone/drone@sha256:55897c8fb22ddc5dff6be4c85b7fbc3ce07c34fe1f03d7cf2cbbfd095833fca6`，本机标签为 `drone/drone:cleanup-test`；Gitea 为 `gitea/gitea:1.24.6`。两者使用临时 SQLite 数据，不接入已有数据库。

runner 由工作区尚未提交的实现构建；测试二进制 SHA-256 为 `467fc8495bfb98028d24cf4a3c0646251391b141c5a83b175243f7546dc95a3d`，镜像 manifest 为 `sha256:1b176df4f019a001dffae8a79de443cfb752a6a1c6076a3dc42e8ff54b8203af`。基础镜像、clone 镜像及任务镜像不属于本轮生产升级。

## 故障注入

仅测试环境中的 `fault` 代理承接 runner 的 Kubernetes API 和 Drone RPC 请求；任务仍在真实 Kubernetes 节点运行。代理支持限定路径的创建/删除拒绝、上报阻塞和延迟提交，记录请求时间、路径、结果，不记录请求体或认证信息。

晚到创建须在客户端返回结果未知、且首次真实查询得到 NotFound 之后，才向 API 提交原创建请求；仅延迟成功响应不算该场景通过。runner 异常退出通过精确终止测试容器触发，同一个 Pod 保留可核验的旧容器终止事实。

测试执行按用户要求使用 gpt-5.6-sol、medium 子 agent；主 agent 负责环境搭建、检查进展及必要干预。验收结论见同目录的验收报告，环境 Ready 本身不代表用例通过。

## 查看与清理

```sh
kubectl --kubeconfig /tmp/drone-cleanup-validation.vJA9AB/kubeconfig get pods -A
kubectl --kubeconfig /tmp/drone-cleanup-validation.vJA9AB/kubeconfig -n drone-cleanup get configmaps,leases
```

需要销毁这个一次性环境时，仅删除指定测试集群：

```sh
k3d cluster delete drone-cleanup-test
```

该操作会删除测试流水线和临时 SQLite 数据；先保留验收证据。临时目录含凭据和 kubeconfig，证据留存后可另行删除。本说明不授权删除其他集群或资源。
