# Pod 残留治理实施清单

日期：2026-09-22。

依据：[已确认设计](../specs/2026-09-21-runner-lifecycle-design.md)。只解决 Pod 残留、回收记录丢失和误删风险，不重新讨论领取协议或 Server 状态一致性。

## 实施准备

- 基线：master，018c41607ac6366a59beaa9a2cf497669dc07258；已有未跟踪的 .agents/、docs/、kubectl.txt、skills-lock.json，保留原状，不批量暂存。
- 本机 Go 1.22.12。默认测试启动遇到 macOS 加载器 LC_UUID 错误；采用系统链接器后，engine 及 daemon 相关既有测试通过，未修改依赖。
- 初次准备时 Docker Desktop 未运行，现有默认 Kubernetes context 为 k3d-lowcode-cloud-local-dev。后续经用户授权，已在 Docker 运行后创建独立验收集群及专用 kubeconfig，未写现有集群、未切换默认 context。
- 缺陷复现采用本机模拟 Kubernetes API 的创建拒绝响应；它验证 runner 销毁路径，不代表已复现线上准入故障的全部成因。
- 已实际复现：Setup 因注入的创建拒绝失败，随后调用 Destroy，捕获到 close of nil channel。模拟 API 与复现程序均在本机临时目录运行，没有访问真实集群。

## 1. 固化崩溃复现并修复基础清理（代码及组件验证已完成）

主要文件：engine/engine_impl.go、engine/spec.go，以及对应新增的 engine 清理测试。

- 把“Secret/Pod 创建失败后 Destroy”的复现转成回归测试，覆盖部分成功、重复销毁和未完成初始化。
- 在资源创建前建立本任务的停止状态；关闭通道及重复清理必须安全。
- 保存本次创建的资源身份，Pod 优先清理；用 UID 条件避免删除同名替换对象，不能删除创建失败时碰到的他人已有资源。
- 为删除及确认设置独立请求截止时间。临时失败退避重试，持续失败返回明确错误并保留待处理状态，移除无限 watcher 等待。

验收：创建失败不 panic；重复清理安全；Pod 删除不被 Secret 删除失败阻塞；删除错误不再被报告为成功。

## 2. 贯通取消及执行收尾入口（代码及组件验证已完成）

主要文件：engine/execer.go、engine/execer_test.go、command/daemon/daemon.go、command/exec.go；复用第一批对 engine/launcher/watcher 的取消处理。

- runner-go 当前给 Setup 传入 noContext，并在终态上报后才 deferred Destroy。适配执行入口，向 Setup 传递真正的任务上下文，并独立触发清理。
- 容器启动请求、启动等待、退出等待及日志读取响应取消；防止等待方退出后，后台 goroutine 仍卡在发送结果或 launcher 请求上。
- 正常结束、初始化失败、取消和超时都进入同一任务清理流程；Server 上报或日志上传未成功不能阻止清理。保持现有步骤调度和结果分类。
- 优先使用适配层；如必须改动 runner-go，只修改与清理触发有关的必要入口，不整体升级依赖或复制工作流引擎。

验收：用阻塞的上报/日志替身验证清理仍被触发；取消能够打断所有相关等待。最终在正常 API/节点条件下验证 60 秒内 Pod 消失。

## 3. 增加最小重启补偿（代码及组件验证已完成）

主要文件：新增 engine 清理记录/补偿实现及对应测试，command/daemon/config.go、command/daemon/daemon.go；必要时调整 internal/kube/kube.go 的受管理 Create 调用方式。

- 默认仅启用基础修复；通过显式配置开启持久记录及重启补偿，校验管理命名空间、执行者身份和必要权限。
- 使用 ConfigMap 保存资源创建意图、名称、唯一标记、UID、执行者身份及清理状态，不保存完整 Spec 或敏感内容；记录在资源创建前建立，不改 Accept。
- 任务明确结束或旧执行者有可信终止证据时，才回收对应资源。活跃任务不因记录年龄、NotReady 或 Lease 到期被删除。
- Lease 协调重复处理，记录使用版本条件更新，资源删除使用 UID 条件；处理者失权后停止新动作。
- 受管理 Create 不透明重发；创建结果未知时保留记录并发现晚到资源，不以 NotFound 或 TTL 提前结案。

验收：重启及两副本场景可继续清理；晚到创建不丢失回收依据；同名不同 UID、历史资源、共享资源和证据不足的任务不被误删。

## 4. 隔离集群回归及交付（固定四组验收已完成）

只添加完成四组验收所需的用例和部署示例，不搭建通用故障平台。

- 在独立本地 Kubernetes 集群完成设计中的四组验收：当前故障回归、删除失败重试、取消/超时回收、重启及双副本安全补偿。
- Create 晚到用例须让服务端提交晚于第一次 NotFound；只延迟成功响应不算覆盖该时序。
- 记录 Pod UID、删除请求时间、确认消失时间，以及未回收资源的明确原因。告警/保留不能计作回收成功。
- 执行仓库要求的 go test -cover ./... 和构建检查；对新增并发状态做定向竞态检查。只重跑相关改动影响的检查。
- 补充恢复开关、最小 RBAC、身份注入及异常边界的部署说明。不开启线上部署，不清理历史 Pod。

通过四组验收和必要检查后交付；领取、阶段补报、Build/步骤一致性等意见登记延期，不作为本期新增任务。

## 当前实现与验证记录

第一批代码已完成：

- 创建失败后的 nil 通道崩溃已固化为失败回归，再通过修复；Secret/Pod 创建失败、重复 Destroy、Destroy 后拒绝再创建均已验证。
- 新增进程内清理状态，资源带任务标记；删除使用 UID 条件和 30 秒正常宽限期，先删除 Pod，再处理 Secret 和临时命名空间。共享及创建冲突对象不会被删除。
- 删除和查询请求各轮有截止时间，按最长 5 秒退避继续处理；Destroy 最多等待 60 秒，超时返回待处理错误，当前进程仍继续重试。确认对象消失后才返回清理成功。
- Create 使用单次发送；未知结果遇到 NotFound 保持待处理，组件用例验证随后出现的匹配 Pod 能被发现并回收。
- 销毁会取消任务 watcher、launcher 和运行等待，移除销毁中的无限等待；修复相关通道在取消后阻塞的问题。任务资源清理不再等待固定 5 秒日志延迟。

已通过：

```sh
go test -mod=readonly -race -ldflags=-linkmode=external ./engine ./engine/launcher ./engine/podwatcher -count=1
go test -mod=readonly -ldflags=-linkmode=external -cover ./...
sh scripts/build.sh
```

构建覆盖 Linux amd64、arm64、arm。系统链接器参数仅用于本机 Go 1.22.12/macOS 加载器兼容，不改项目 CI；race 链接时有系统链接器警告，测试退出成功且未报告数据竞争。依赖版本未变，未提交代码或部署集群。

第二批代码已完成：

- 新增共享执行适配层，将真实任务 context 传给 Setup；取消、截止时间和执行器内部的快速失败取消都独立触发 Destroy，清理请求使用独立 context。
- 所有非后台步骤已执行返回、明确跳过或因上报错误放弃执行后，启动清理；阶段最终上报之前也触发清理。最后一步的日志上传、跳过步骤的上报和最终阶段上报阻塞时，资源仍可回收。
- 仍有依赖步骤或 RunAlways 收尾步骤待执行时，不因某个步骤返回就删除 Pod。继续使用同一个 runner-go 执行器，保留跨任务共享的并发限制、DAG 调度和结果分类，不修改依赖版本。
- daemon 和本地 exec 都已接入；daemon 补上 poller 丢失的关闭信号，让执行中的任务能够收到 runner 退出时的取消。
- 新增 9 个回归场景，覆盖上述阻塞、超时、快速失败、条件跳过、RunAlways 依赖、并发限制及任务隔离。其中使用 Kubernetes 假客户端验证：Server 上报尚未恢复时，取消任务的 Pod 已被删除并确认不存在。

新增用例先复现“阻塞上报时取消不触发清理”，适配后通过。最新代码已通过定向竞态检查、全仓覆盖率测试和三个 Linux 架构构建；未报告数据竞争，macOS race 链接仍有上一批已记录的工具链警告。

```sh
go test -mod=readonly -race -ldflags=-linkmode=external ./engine -run 'Test(CleanupWhile|SetupUses|CleanupPrecedes|CleanupWaits|CancellationRemoves|SharedStep|CleanupOnFast)' -count=1
go test -mod=readonly -ldflags=-linkmode=external -cover ./...
sh scripts/build.sh
```

本轮保留 Server 调用原有的等待与重试方式，验证的是资源清理可以独立启动，不承诺阻塞的 Server 请求同步返回。后续仍有构建步骤未完成时，保持原调度次序；取消或超时能够独立回收资源。

第三批代码已完成：

- 增加默认关闭的重启补偿开关。启用后要求 runner 在集群内作为容器主进程运行，注入 Pod UID 和容器身份；启动时验证管理命名空间及记录/Lease 权限，创建任务前检查实际任务命名空间权限。
- 在 ConfigMap 中先保存资源创建意图，再发送单次 Create；记录只保存归属、资源身份及清理进度，不保存 Secret 内容或完整任务配置。记录写入失败会阻止新建资源，但不会挡住已有 Pod 的停止尝试。
- 明确交接的任务可直接补偿；否则要求执行者 Pod UID、容器运行身份及终止时间与记录匹配。不以 Pod 丢失、NotReady、记录年龄或 Lease 过期推断执行者死亡。
- Lease 协调本池补偿，记录以版本条件更新；任务资源按 UID 条件删除，记录结案同时使用 UID 和版本条件。失去处理权后停止新动作，保留未知创建、替换对象及缺少证据的记录。
- 未知创建首次 NotFound 后继续保留，可由新的补偿器发现晚到资源。活跃执行者确认接收完成事实后再删除记录，避免与补偿器并发时丢失晚到资源的 UID。
- 新增 13 个恢复测试函数及表驱动场景，覆盖持久意图、终止证据、补偿器重建、不同执行者的两副本、晚到创建、存储失败、UID/标记不匹配、并发及版本冲突、失权取消和权限拒绝。测试使用 Kubernetes 假客户端，另用本机 HTTP 服务核验记录删除的请求条件；不代表真实 Lease 竞争或容器重启已验收。
- 已补充[开关、身份注入、最小 RBAC 和异常边界说明](../../cleanup-recovery.md)。未改 Server 协议及依赖版本。

最新代码已通过：

```sh
go test -mod=readonly -race -ldflags=-linkmode=external ./engine ./command/daemon -count=1
go test -mod=readonly -ldflags=-linkmode=external -cover ./...
sh scripts/build.sh
```

全仓测试通过，engine 语句覆盖率为 63.5%；未报告数据竞争。Linux amd64、arm64、arm 构建通过。macOS race 链接仍有已记录的系统链接器警告，测试退出成功。

第四步已于 2026-09-22 在独立 `drone-cleanup-test` 集群完成。测试由用户指定的 gpt-5.6-sol、medium 子 agent 执行，主 agent 负责搭建、监督进度及结果核对；[环境说明](../../validation/2026-09-22-environment.md)和[验收报告](../../validation/2026-09-22-pod-cleanup.md)保存配置与关键时序。

- 创建失败不 panic，删除临时失败可重试；持续拒绝保留记录并明确错误。
- 真实 runner 容器异常退出后，补偿先观察到 NotFound，原创建随后才提交成功；晚到 Pod 仍被发现、回收并完成记录结案。
- 取消、本地超时、全部 Server RPC 请求阻塞时本地超时，Pod 分别在 31.576 秒、约 33.223 秒、约 31.573 秒内消失，均满足 60 秒目标。日志上传阻塞及 Secret 删除拒绝未挡住 Pod 清理。
- 两个真实 runner 参与 Lease 协调，确认旧容器终止事实后回收其任务；另一任务最终成功。UID 不符、共享 Secret 和执行者终止证据不足的真实集群负例均未误删。
- 故障规则已清空、在途阻塞已释放，测试任务命名空间已无 Pod；故意注入的两条未知创建诊断记录按设计保留。安全负例的人工清理不计作产品自动回收。

本轮未修改产品代码；故障代理源码编译通过，文档差异检查通过，未重复运行此前已通过且未受影响的产品测试。固定验收范围已完成，停止增加设计及测试场景。环境保留供查看，未提交、未推送、未部署线上，也未修改既有集群。崩溃任务的 Drone 页面状态修复仍不在本期范围。
