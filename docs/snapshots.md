# Raft 快照、日志回收与容量契约

实现日期：2026-10-21；2026-10-22 增加自适应回收节奏和暂停观测。当前仍是固定三个 voter、单 Raft Group 的本机开发服务。

## 快照覆盖什么

运行时只在 KV 已按序应用后调用 `Node::Compact(applied_index, state)`。快照保存按 key 排序的二进制 KV 数据、lastIncludedIndex 和 lastIncludedTerm；已删除的 key 不进入快照。默认 ReadIndex 的 GET 不占日志位置；旧 log 读模式和 no-op 仍占据位置。

`FileStore` 默认通过[增量 journal](journal.md)追加普通变更；生成／安装快照时，将 hard state、快照及保留后缀写入同一个带 CRC32 的检查点，先同步新代次 journal，再依次完成临时文件写入、`fdatasync`、原子替换和目录 `fsync`。旧前缀只在整个新镜像持久化成功后从内存回收。四个发布边界均有 SIGKILL 测试，恢复结果是完整旧镜像或完整新镜像。

重启先恢复快照中的 KV 和 applied/commit 下界，剩余日志不能直接当成已提交数据。新 Leader 的当前任期 no-op 重新建立多数派提交前缀，之后按序应用后缀。

## 副本追赶

Leader 发现副本需要的日志已被回收时，通过 `InstallSnapshot` 发送最多 16 KiB 的块。单次传输固定快照版本，即使本地又生成了新快照，也不在传输中途混用内容。

接收方检查成员、任期、边界、总长度、偏移及块长度，最多在内存组装 16 MiB。它接受内容一致的重传；接收方中途重启或块不连续时，从头重传。完整 KV 内容通过格式和排序验证、且快照与后缀原子落盘后，才确认安装成功。部分块不是恢复候选。

若本地日志在快照边界有相同 term，则保留之后的后缀；否则丢弃冲突的未提交尾部。已经提交或更新的快照不会回退。安装后先恢复 KV，再应用新提交后缀。

另有一个回归用例保护追赶活性：Follower 可能在 Leader 收到确认前压缩了已提交日志。Leader 必须检查副本提示的边界 term 并向前探测，不能永远向旧位置回退。100 种随机故障序列中的种子 56 曾触发该问题；现有专门回归测试。

## 文件格式与兼容性

以下描述保留的 image 模式：无快照使用 v1，首次快照后使用 v2。默认 journal 模式使用带 generation 的 v3 检查点，也可导入 v1/v2，格式见[journal 契约](journal.md)。v2 格式如下：

| 字段 | 字节数 |
| --- | ---: |
| magic `YRFT` / version `2` | 4 / 4 |
| currentTerm / votedFor / suffix entry count | 8 / 8 / 8 |
| lastIncludedIndex / lastIncludedTerm / snapshot byte length | 8 / 8 / 8 |
| KV snapshot Protobuf | 可变 |
| 后缀 entry：index / term / command length / command | 8 / 8 / 4 / 可变 |
| 覆盖全部前文的 CRC32 | 4 |

v2 固定头为 56 字节，日志索引从 lastIncludedIndex + 1 开始。损坏、截断、非法 term/index、超长内容及多余字节都会拒绝恢复，原文件保留。

升级须停止全部旧节点后统一使用新版本；不支持混合版本滚动升级。旧程序不能读取 v2；新增命令语义也不承诺旧程序重放兼容。所有回退都需要升级前的整集群备份。

新的 Leader 将 `KvRequest.state_machine_version=1` 写入复制日志，固定容量判断的重放语义。旧日志中 version=0 的成功写入按原语义恢复，不会因新增限制而被静默丢弃。若旧数据的活跃集合已经超过新快照容量，快照暂缓；先删除或缩小数据，降至限制内再回收日志，原 64 MiB 镜像硬上限仍适用。

## 容量与背压

默认使用 `--snapshot-policy adaptive`。保留日志达到 `--snapshot-bytes`（默认 1 MiB）仍无条件触发快照；已应用后缀达到 `--snapshot-entries`（默认 128）时，另要求保留日志至少达到 `min(snapshot_bytes, live_snapshot_bytes / 4)`，避免大活跃集合因少量小更新反复重写。`fixed` 保留原先“条目数或字节数”任一达到即生成的策略，便于比较与控制恢复后缀长度。

这是触发阈值，批次可以越过阈值；没有新应用条目时不生成快照，未提交日志也不能提前删除。自适应策略可能保留更多日志条目，因此启动重放时间与回收频率需要一并测量。默认容量仍受 16 MiB 活跃快照、1 MiB 日志触发和 64 MiB 逻辑状态上限约束；自定义阈值也应与容量上限匹配。

| 配置或限制 | 默认值 | 行为 |
| --- | ---: | --- |
| `--snapshot-entries` | 128 | 已应用位置距快照边界的触发阈值 |
| `--snapshot-policy` | adaptive | 大状态下按日志字节摊薄条目触发的全量重写；fixed 为旧策略 |
| `--snapshot-bytes` | 1048576 | 保留日志字节触发阈值 |
| `--max-state-bytes` | 67108864 | 逻辑状态编码上限，测试可调小；达到上限保守停用 |
| 活跃 KV 的编码快照 | 16777216 | 新 PUT 不能超过，返回 BUSY 且不改变 map；DEL、缩小 value 可回收空间 |
| `--event-queue` | 4096 | 运行时事件队列上限，最高 8192 |
| `--pending-limit` | 1024 | 等待写提案与 ReadIndex 读的总上限，最高 8192 |
| 单连接未回复请求 | 4096 | 超限关闭连接 |
| 单连接待排序回复加输出缓冲 | 1 MiB | 超限关闭慢连接 |

队列 BUSY 是追加前拒绝；活跃 KV 容量 BUSY 是已提交但不修改 map 的操作。两者都不会把被拒绝的 PUT 应用成成功。超时、丢连接和 UNAVAILABLE 仍是结果未知，客户端不自动重放。

达到真实磁盘错误或镜像硬上限时，节点停止参与协议。清除故障后重新打开持久目录，禁止删日志或清空目录来“修复”。测试专用 I/O 注入库仅预加载到独立测试进程，常规服务不加载。

## 观测与验证

`status` 包含 snapshot_index、snapshot_bytes、live_snapshot_bytes、retained_entries、retained_log_bytes、state_bytes、snapshots_saved、snapshots_installed、busy_rejections、completed_commands 和 quorum_timeouts。新增 snapshot_policy、snapshot_pause_ns 和 snapshot_pause_max_ns；默认同步路径的暂停耗时覆盖状态序列化、验证、检查点同步及发布，按进程累计。后台编码不计入节点线程暂停；后台磁盘时发布暂停仅覆盖准备和接收任务，完整耗时另见[后台磁盘指标](background-persistence.md#追踪与指标解释)。它不是请求延迟，也不能把累计最大值相减作为测量窗口的最大值。持久化计数分别区分 journal、检查点和同步次数。

```bash
ctest --test-dir build --output-on-failure -j2
python3 scripts/cluster-soak.py --node build/bin/yewukv-node \
  --client build/bin/yewukv-client --duration-seconds 900 \
  --output benchmarks/results/my-fault-campaign
python3 benchmarks/cluster_matrix.py --node build/bin/yewukv-node \
  --duration-seconds 10 --repeats 3 --output benchmarks/results/my-matrix
```

历史证据见[本机验收报告](local-acceptance-20261021.md)与[journal / ReadIndex 优化](local-optimization-20261021.md)。默认普通变更已使用追加 journal，GET 已使用 ReadIndex；默认磁盘路径仍同步重写检查点，另有可选[后台磁盘](background-persistence.md)。发布期间还会存在临时文件和新代次 journal；state_bytes 统计逻辑状态，不是目录占用或块设备物理写入量。后续三机证据见[实验索引](README.md#实验与验收)；这些早期本机结论不覆盖跨机器、真实断电或生产容量，当前仍无分片或动态成员变更。

设计依据：[Raft 原论文第 7 节](https://raft.github.io/raft.pdf)。故障历史方法参考 [etcd 鲁棒性测试](https://github.com/etcd-io/etcd/blob/main/tests/robustness/README.md)。
