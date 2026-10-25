# 持久化与读请求的同线程时间关联

`--trace-prefix` 的数值诊断新增单次 `persist` 记录，日志 metadata schema 为 2；已有 `read`／`peer` 字段保持原含义。默认不创建追踪器。追踪仅观测，不改变磁盘格式、持久化确认顺序或协议消息定义。

## 单次持久化记录

所有时间戳使用该节点进程的 `steady_clock`。`sequence` 在一个 FileStore 的启用观测期内递增；`tid` 是实际调用持久化路径的 Linux TID。`started_ns / finished_ns` 覆盖一次 image 发布、检查点发布或 journal 记录追加。

| kind | 操作 |
| ---: | --- |
| 1 | image 发布 |
| 2 | journal 模式的完整检查点发布，包括新 journal 的准备与旧代次清理 |
| 3 | hard-state journal delta |
| 4 | 日志后缀 journal delta |

`spans` 是最多 12 个 `[stage, started_ns, finished_ns]`，按完成顺序保存。当前阶段不嵌套，核验时要求有序、不重叠且包含在整次持久化区间内。

| stage | 区间 |
| ---: | --- |
| 1 | Raft 镜像或 journal 帧的编码 |
| 2 | 该编码字节串的文件写入 |
| 3 | 一次文件 `fdatasync` 调用及相邻计数操作 |
| 4 | 一次目录 `fsync` 调用及相邻计数操作 |
| 5 | 发布文件的 `renameat` 调用 |

`crc_ns` 是编码中的累计 CRC 时间，已含于 stage 1，不能作为额外阶段相加。检查点可能包含多次文件／目录同步，分别保留。文件创建、清理、内存更新等剩余区间未冒充文件同步时间。

journal 帧记录从封装开始；调用者预先构造后缀 payload 的时间不在其中。KV 状态机序列化及后台冻结也不属于 FileStore 的编码区间。已有目录恢复中的直接读取／同步未统一接入本记录；新目录初始化的检查点可能出现于启动记录中。整次操作 `success=1` 仅表示该持久化步骤完成，不能当作业务请求已经多数派提交或成功返回。失败操作可能已经发布部分磁盘状态，不能反推修改一定未发生。

FileStore 的诊断回调在存储锁内只复制数值记录到原有有界队列，不得重入或执行文件 I/O；诊断回调异常被隔离。原有故障注入回调仍按原契约使实例停止服务。JSON 格式化、控制文件读取和日志写入均在独立线程完成。

## 开关、采样与资源

沿用[读追踪](read-tracing.md)的 `<prefix>.enabled` 开关、8192 条待写队列、事件数与字节上限。三种事件共享限额。新增记录使用固定数组，队列、交换出的写入批次和序列化缓冲仍有界，但启用诊断有额外 CPU、内存和磁盘开销。

- image／检查点、失败操作及达到 `--trace-slow-ms` 的操作全部保留。
- 其余 journal 操作按 `sequence % trace_sample_every == 0` 保留；默认 1/32。
- `--trace-sample-every 1` 请求完整记录普通 GET 与 journal 操作；仍须检查退出摘要、`trace_dropped / trace_limited / trace_failed` 和开关区间。
- `spans_limited=1`、有丢记录、达到限额或缺少退出摘要均不能宣称完整覆盖。暂停边界可能省略跨边界记录。

## 客户端身份与关联

新的 C++ 发压器每连接另保留 `local_port`、`connection_first_request_id` 和计时阶段的 `first_request_id`。每个样本对应一次顺序 RPC，跳过的计划时刻不发 RPC，因此第 i 条样本使用 `first_request_id + i`。样本六列、到达日历与 schema 2 保持兼容。

诊断时显式使用 `--trace-identity 1`。连接在预装前从系统随机源选择高范围的编号起点，降低连接端口复用后的误关联；默认负载仍用原来的顺序编号。此开关会增加部分请求编号的 Protobuf 编码长度，应在追踪开启／关闭两组均使用，不能把它与旧的小编号负载直接比较并宣称无开销。

随机编号不是全局唯一性或幂等保证。对完整活动调用 `persistence_timeline.validate_client_ranges`，核对所有连接（含预装、预热）的已用编号区间无重叠。单窗口工具只核对其输入中的连接；重复端口／请求编号出现多个候选时明确剔除，不猜测连接身份。

```bash
build/bin/yewukv-bench --host 127.0.0.1 --port 9101 \
  --clients 8 --duration-ms 10000 --warmup-ms 1000 \
  --write-percent 50 --value-size 128 --offered-rps 500 \
  --trace-identity 1 --prefix isolated-trace --output /tmp/new-trace-run
python3 benchmarks/persistence_timeline.py \
  --trace /absolute/logs/leader-pid-steady.jsonl \
  --samples /tmp/new-trace-run --output /tmp/new-timeline.json
```

关联器先验证客户端日历和结果，再用端口／请求编号匹配 GET。只计算该节点读请求 `handled_tid` 上持久化区间的并集，与网络接收至开始处理的排队区间、开始处理至准备响应的区间分别相交。各 I/O 阶段也使用区间并集，不把 CRC 重复相加。

输出明确记录已发 GET 数、成功关联数、缺失数、歧义数、慢 GET 覆盖和采集是否完整。采样、开关或丢记录时，交集只是已经观察到的部分。即使交集为零，也不能排除未观测的 KV 编码、副本持久化、线程调度或网络等待。客户端与 Leader 可在不同机器，工具不直接相减它们的单调时钟。

## 三机短时开关对照

`benchmarks/cloud_persistence_trace.py` 使用与长窗口相同的独立实验控制服务，在新 tag 下依次创建 sync／background 两组 15 MiB 冷数据。每组保持同一进程、Leader 和数据目录，按关／开／开／关／关／开执行六个 30 秒窗口，32 连接、计划 600 次／秒、50% 写入、128 B 热 value。开关两组均使用随机编号，默认 adaptive、4 MiB journal 检查点保持一致。

```bash
python3 benchmarks/cloud_persistence_trace.py --config /private/agent.json \
  --bench /private/yewukv-bench --output /private/trace-smoke --smoke
# 核验烟测后，在另一个全新输出目录运行；结束时关闭本 tag 控制服务
python3 benchmarks/cloud_persistence_trace.py --config /private/agent.json \
  --bench /private/yewukv-bench --output /private/trace-main --shutdown-agents
```

每约 10 秒检查原服务、实验节点、可用磁盘和 RSS；请求或诊断记录失败时停止并保留结果，不重放写入。每组结束后整组重启并检查全部冷键、256 个热键和锚值 revision。控制服务仅能切换当前实验目录内固定的 `trace.enabled` 文件，不接受任意路径或命令。

相邻窗口相关，trace writer 与持久化共享节点资源。该活动用于检查关联和观测开销，不是独立容量验收、无偏多轮性能比较或后台磁盘发布的完成证明。实际结果需另行报告。
