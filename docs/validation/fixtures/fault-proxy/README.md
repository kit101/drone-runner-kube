# 隔离验收故障代理

`main.go` 是 2026-09-22 Pod 清理隔离验收使用的最小代理源码。它只用于隔离测试：8443 终止测试 CA 的 TLS 后代理 Kubernetes API，8080 代理 Drone RPC，9090 暴露本地 port-forward 控制接口。上游分别由 `KUBE_UPSTREAM` 和 `RPC_UPSTREAM` 指定。

事件接口只保存时间、plane、method、path、outcome、status 和 rule，不保存 body、headers、Secret 或 token。`late_create` 会等待原请求 context 取消和同名 GET=404 两个事实都成立，随后用独立 context 向真实 API 只提交一次 POST。

控制端仅通过本机 port-forward 使用：

```sh
kubectl --kubeconfig /path/to/isolated-kubeconfig -n drone-lab port-forward svc/fault 19090:9090
```

清空规则会释放已经被 `block` 的请求，并继续转发原请求：

```sh
curl -X DELETE http://127.0.0.1:19090/rules
curl -X DELETE http://127.0.0.1:19090/events
```

规则最小示例：

```sh
# 第一次 Pod DELETE 返回 Kubernetes 503 Status，后续请求正常转发
curl -H 'Content-Type: application/json' \
  -d '{"name":"pod-delete-once","plane":"kube","method":"DELETE","pathPrefix":"/api/v1/namespaces/drone-tasks/pods/","action":"reject","remaining":1,"status":503}' \
  http://127.0.0.1:19090/rules

# 阻塞 Drone RPC 的 POST；删除规则时释放
curl -H 'Content-Type: application/json' \
  -d '{"name":"rpc-post-block","plane":"rpc","method":"POST","pathPrefix":"/rpc/","action":"block"}' \
  http://127.0.0.1:19090/rules

# 将一次 Pod Create 保持到客户端取消且补偿查询先观察到 GET=404，再独立提交
curl -H 'Content-Type: application/json' \
  -d '{"name":"pod-create-late","plane":"kube","method":"POST","pathPrefix":"/api/v1/namespaces/drone-tasks/pods","action":"late_create","remaining":1}' \
  http://127.0.0.1:19090/rules

curl http://127.0.0.1:19090/events
```

验收时使用临时生成的 CA、服务证书和私钥；这些文件不进入仓库。runner 挂载代理 CA，同时继续使用真实 projected ServiceAccount token。代理自身从标准 ServiceAccount 路径读取真实 Kubernetes CA。
