# Pod 清理与重启补偿

本功能只处理任务资源回收，不恢复流水线、不重放步骤，也不修改崩溃任务在 Drone Server 上的阶段状态。基础清理默认启用；跨进程补偿需要显式开启。仅管理本版记录的新任务，历史 Pod 不会被自动纳管。

## Secret 权限兼容

**正常创建成功的任务 Secret 只需 `create/delete` 权限，无需新增 `get secrets`。** runner 保存创建响应中的 UID，清理时直接发送带 UID 条件的 DELETE。删除成功响应仅代表请求被接受；后续 DELETE 返回 NotFound 才确认资源已消失。同名对象的 UID 不符时，API 会拒绝删除，runner 保留待处理状态。

现有 Secret 规则可保持不变，基础清理和重启补偿的必要权限检查均不要求 Secret get/list/watch：

```yaml
# 现有角色中与任务 Secret 相关的规则
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["create", "delete"]
```

边界是创建响应丢失，或者恢复记录尚未保存成功创建的 UID：此时仍需 GET 核验任务、创建尝试和 pool 标记后才能取得可信 UID。若已有 get 权限，沿用该恢复路径；若没有，则保留待处理状态并告警，需要运维核验处理，不会按名称无条件删除，也不会把 Forbidden 当作清理成功。此限制不影响 Pod 优先清理。

升级镜像需重启 runner。若未开启持久恢复，旧进程中的待清理队列不会迁移，已遗留 Secret 需要另行核验处理；本功能不自动纳管没有恢复记录的历史资源。已有持久记录且保存了 Secret UID 时，新版可按原有回收条件继续处理。

## 开启条件与配置

runner 必须在 Kubernetes Pod 内作为容器 PID 1 运行，使用 in-cluster 客户端。默认镜像的 ENTRYPOINT 满足 PID 1 要求；使用 shell 包装时应通过 exec 启动 runner。集群外的 exec 和自定义 kubeconfig 不启用重启补偿。

| 环境变量 | 含义 |
| --- | --- |
| DRONE_CLEANUP_RECOVERY_ENABLED | 默认 false；设为 true 开启 |
| DRONE_CLEANUP_NAMESPACE | 预先建立的独立管理命名空间，不能用于任务 Pod 或临时任务命名空间 |
| DRONE_CLEANUP_POOL | 同一组 runner 使用相同值；最多 40 字符，符合 DNS label 格式 |
| DRONE_RUNNER_POD_NAMESPACE | 从 metadata.namespace 注入 |
| DRONE_RUNNER_POD_NAME | 从 metadata.name 注入 |
| DRONE_RUNNER_POD_UID | 从 metadata.uid 注入 |
| DRONE_RUNNER_CONTAINER_NAME | runner 容器在 Pod spec 中的名称 |

合入现有 runner Deployment 的容器配置示例：

```yaml
name: runner
env:
  - name: DRONE_CLEANUP_RECOVERY_ENABLED
    value: "true"
  - name: DRONE_CLEANUP_NAMESPACE
    value: drone-cleanup
  - name: DRONE_CLEANUP_POOL
    value: default
  - name: DRONE_RUNNER_POD_NAMESPACE
    valueFrom:
      fieldRef:
        fieldPath: metadata.namespace
  - name: DRONE_RUNNER_POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: DRONE_RUNNER_POD_UID
    valueFrom:
      fieldRef:
        fieldPath: metadata.uid
  - name: DRONE_RUNNER_CONTAINER_NAME
    value: runner
```

启动时核对 Pod UID、容器运行身份、管理命名空间和权限；容器状态尚未同步时，最多等待 30 秒。关键条件不满足会终止 daemon 启动。每个任务创建资源前，还会校验目标命名空间权限；拒绝时不创建任务资源，不静默退回仅内存模式。

## 权限

给 runner 使用的 ServiceAccount 配置以下权限。管理权限使用管理命名空间的 Role；runner Pod 读取权限和任务权限分别绑定到实际使用的命名空间。

| 作用域 | API 资源 | verbs |
| --- | --- | --- |
| 管理命名空间 | core/configmaps | get, list, create, update, delete |
| 管理命名空间 | coordination.k8s.io/leases | get, create, update |
| runner 所在命名空间 | core/pods | get |
| 每个任务命名空间 | core/pods | get, list, watch, create, update, delete |
| 每个任务命名空间 | core/pods/log | get |
| 每个任务命名空间 | core/secrets | create, delete；get 可选，仅用于 UID 未知的创建恢复 |
| 集群级，可用 resourceNames 限定管理命名空间 | core/namespaces | get |
| 仅使用自动临时命名空间时 | core/namespaces | get, create, delete |
| 权限自检 | authorization.k8s.io/selfsubjectaccessreviews | create |

使用自动临时命名空间时，还需要在新命名空间内具有表中的任务权限。默认使用已有固定任务命名空间时，无需额外授予 namespace 的 create/delete，也无需授予跨所有任务命名空间的删除权限。

## 记录与回收规则

- 每个任务一个 ConfigMap，名称为 drone-cleanup-加任务 UUID，带协议版本、pool 和任务标记；没有任务 Pod 的 ownerReference，因而不随任务资源一起消失。
- 记录仅保存执行者身份、进程会话及开始时间、创建尝试、资源名称/UID、清理进度与错误类别，不保存完整 Spec、脚本、环境变量或 Secret 内容。
- 每次资源 Create 前先保存发送意图。保存失败则不发送 Create；创建请求单次发送，不透明重发。未知结果即使连续查询为 NotFound 也会保留，取得读取权限并发现匹配的晚到对象后才补齐 UID 并清理。已保存 UID 的 Secret 直接按 UID 删除，无需重新读取标签或内容。
- 补偿器每 5 秒扫描本 pool 的记录，用 Lease 协调扫描者。每次写记录使用 resourceVersion，每次删资源使用 UID，删除记录同时校验 UID 和 resourceVersion。丢失 Lease 后停止新动作，在途请求可能完成。
- 任务明确交接清理，或者记录中的 Pod UID、容器 ID 与已终止运行实例匹配时，才允许回收。终止时间还必须不早于记录中的进程会话开始，防止启动时读到旧容器状态而误删新任务。
- 同名 runner Pod 被替换、runner Pod 不存在、NotReady、记录太旧或不同进程不认识任务，都不足以证明执行者已终止。缺乏证据时保留记录并告警。极短会话或时钟异常导致时间事实无法确认时同样保留。
- Pod 优先正常删除，保留 30 秒宽限期；只清理本任务创建的 Secret 和临时命名空间，不删除共享 PVC、共享 Secret 或既有命名空间。清理意图保存失败不会阻止已有资源的停止尝试。
- 补偿器完成仍存活执行者交接的任务后，先保留完成事实，由原执行者确认并删除记录，避免并发清理丢失晚到对象的 UID。执行者已确认终止时，由补偿器删除完成记录。

记录使用 io.drone.cleanup-version、io.drone.cleanup-pool、io.drone.cleanup-task 标签；任务资源另有 io.drone.cleanup-attempt 标签。排查时以这些标签、记录中的资源名称、UID 和 LastError 错误类别关联 runner 日志。

## 异常边界与关闭

Pod 在本功能发起删除之前已消失时，保留“执行停止未确认”的诊断。正常删除后确认的是 API 对象已消失；外部强制删除、节点分区等情形下，不据此宣称节点上的容器已停止，也不执行强删或节点隔离。这些情况需要运维另行核验。

权限或准入持续拒绝、创建结果长期未知、执行者终止证据缺失时，记录可能长期保留。不会用 TTL 清掉这些记录。Secret 或临时命名空间尚未回收时，整条记录也不会提前删除。

关闭开关后，基础清理仍生效，但不会创建新的持久记录或扫描旧记录。已有记录不自动删除；需要继续自动处理它们时，至少保留一个相同 pool、管理命名空间和协议版本的已启用实例。

组件回归及固定四组本地隔离集群验收已完成，包括重启、双副本和 60 秒回收目标；详见[验收报告](validation/2026-09-22-pod-cleanup.md)。验证只针对记录的本地测试环境，不代表已部署或验证生产集群。
