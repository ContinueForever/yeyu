# 可关联的 ReadIndex 分阶段追踪

追踪默认关闭，仅用于诊断。启用后记录数值时间戳、客户端源端口、请求编号、读 context、确认副本和 RPC sequence，不记录 KV key/value，也不改变网络消息或持久化格式。

三台 ECS 上的实际开关对照、逐请求关联、抓包和恢复结果见[2026-10-24 诊断报告](cloud-read-trace-20261024.md)。

```bash
build/bin/yewukv-node --id 1 --port 9101 \
  --members 1@127.0.0.1:9101,2@127.0.0.1:9102,3@127.0.0.1:9103 \
  --data-dir /absolute/data/node-1 --trace-prefix /absolute/logs/read-node-1
```

日志父目录须已存在并对服务用户可写。每次进程启动独占创建新的 `<prefix>-<pid>-<steady-ns>.jsonl`，保留旧记录。使用 systemd 的 `ProtectSystem=strict` 时，可以通过实验覆盖项的 `LogsDirectory` 提供独立可写目录。

## 控制与资源上限

- `<prefix>.enabled` 为普通文件时，首字节 `0` 暂停、`1` 恢复；文件须对服务用户可读。独立记录线程约每 250 ms 检查一次。文件缺失或内容无效时保留当前状态；初始默认启用。
- 切换后等待 `status` 中的 `trace_enabled` 确认，再开始预热／计时。不需要重启节点或更换 Leader。切换边界可能省略跨边界的记录，不应纳入完整覆盖分析。
- `--trace-sample-every 32`：按节点工作线程接收读的序号保留 1/32 的常规读。
- `--trace-slow-ms 10`：在准备响应时，网络回调接收至工作线程准备完成超过阈值的读也保留。之后才在 I/O 线程、内核或网络上变慢的请求仍可能只有比例样本；这些日志不代表所有端到端慢请求。
- `--trace-max-events 500000`：读和 peer 记录共享上限；最大允许配置为 1000000。
- 待写队列最多 8192 条；写线程交换出的一批及序列化缓冲另有相同量级的有界空间。事件数据文件默认上限 256 MiB，最后的摘要记录额外占少量空间。
- 工作线程与网络线程只提交数值结构，格式化和文件写入在独立线程执行。队列满时丢弃诊断记录。达到限额或 I/O 失败后停止接收诊断，不将日志错误当作 Raft 持久化失败。

`status` 提供 `trace_enabled / trace_written / trace_dropped / trace_limited / trace_failed`。正常退出时日志末尾提供 `summary`；有丢弃、限额、写入失败或缺少退出摘要的日志不能声称完整覆盖。诊断日志不做逐次同步落盘，不属于恢复状态。

## 时钟与字段

所有 `*_ns` 使用进程所在机器的 `steady_clock`。首行 `metadata` 同时记录 steady 与 realtime 时钟供粗粒度关联；跨机器不要直接相减两个 steady 时间戳。

成功 `read` 记录的阶段为：

| 字段 | 含义 |
| --- | --- |
| `received_ns` | 网络回调已解帧／解析请求，准备送入节点队列 |
| `handled_ns` | 节点工作线程开始处理请求 |
| `context_ns` | 请求被分配给新建的读确认轮次 |
| `quorum_ns` | 运行时观察到核心输出的多数派 ReadState |
| `ready_ns` | 已检查应用位置，查询状态机并准备响应 |
| `response_queued_ns` | 响应已编码，准备交给 I/O 线程 |
| `tcp_loop_ns` | I/O 线程开始处理这次响应交付 |
| `tcp_dispatch_done_ns` | 完成 SendOrdered 调用；可能已发送，也可能仍在有界输出缓存中 |

`tcp_dispatch_done_ns` 不是物理发包时刻；确认网络发送仍需包记录。失败的读可能没有 context 或 quorum，缺失阶段为 0，必须结合 `code` 解释。

`confirmed_by / confirmed_sequence` 来自核心已通过任期、成员、sequence、context 和日志位置检查的实际确认；仅用于观测，不参与协议决策。使用 `(node, term, confirmed_by, confirmed_sequence)` 关联该节点的 `peer` 记录，再校验 context 一致。使用 `(client_port, request_id)` 关联同一时间窗的客户端／包记录，跨连接复用端口时还需检查时间区间。

`peer` 记录包含排入副本发送槽、发送线程取出、调用 RPC、首次成功 send、请求帧写完、响应解码、发送线程获知完成、节点线程接收回复的时间。RPC 内部编号用 `request_id` 关联实际线上帧。时间戳通过原子字段传递，超时后仍可能继续写出的部分帧不会与诊断读取发生数据竞争。未完成阶段为 0，不应据此构造负延迟。

`read` 中的 `received_tid / handled_tid / tcp_tid` 及 `peer` 中的 `scheduled_tid / worker_tid / rpc_tid / node_tid` 使用 Linux TID，可与 `sched_switch / sched_wakeup` 事件关联。TID 在线程内缓存，仅在线程首次记录时查询；不为每条请求增加一次系统调用。调度事件也应使用单调时钟，并限定在对应进程的生命周期内，避免线程编号复用。阶段总时长不等于可运行但等待 CPU 的时间，仍须核对实际的唤醒和运行区间。

## 验证与解释

三机应用追踪与抓包结果见[第一阶段报告](cloud-read-trace-20261024.md)；TID 与调度事件关联、原程序 CPU 亲和性重复对照见[后续报告](cloud-scheduling-20261024.md)。

`ReadTrace` 单元测试覆盖运行中暂停／恢复、并发提交、事件和字节上限、退出排空。RPC 测试覆盖实际请求编号及超时无回复；`cluster_read_trace` 在真实三进程上验证阶段顺序、确认来源、关闭追踪仍可读取、纯读不新增日志，以及旧 Leader 失去多数派时不返回本地值。

性能实验先在同一程序、进程、Leader、数据集下交错测试关闭／开启追踪，再分析采样记录。由于慢请求保留策略存在选择偏差，采样日志的分位数不能替代完整客户端样本的分位数，也不能把多个阶段各自的 P99 相加。
