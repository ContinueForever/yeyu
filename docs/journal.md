# 增量 Raft 日志与检查点

`yewukv-node` 默认 `--raft-store journal`。`--raft-store image` 保留原来的每次全量镜像发布，用于兼容旧测试和同一二进制的性能对照。低层 `FileStore` API 默认保留 image 以兼容已有调用方，三节点运行时显式启用 journal。两种模式都必须先完成同步持久化，再确认投票或日志；客户端成功仍要求多数派提交和状态机应用。

## 写入路径

- term/vote 变化：追加一个 hard-state delta，`fdatasync` 成功后更新内存。
- 追加、冲突替换和后缀截断：追加包含 first index 和替换条目的 suffix delta；完整同步后才替换内存后缀。Raft 已提交前缀保护仍由 `RaftLog` 检查。
- 普通追加不复制整个保留日志或 KV 快照，也不重写检查点。
- 快照生成／安装，或活跃 journal 已超过 `--journal-checkpoint-bytes`（默认 4 MiB）时，生成新的完整检查点。后者在下一条 delta 之前发生，单条记录可以使 journal 暂时越过阈值。

这是一个可回收、按检查点代次轮转的追加日志，尚不是多段并行管理的存储引擎。检查点仍需要编码整个快照与保留日志；阈值选择会在写放大、恢复成本和暂停时间之间取舍。

## 磁盘格式

`raft.state` v3 是大端、带 CRC32 的检查点：magic `YRFT`、version=3、term、vote、entry count、snapshot index/term/data length、非零 generation，共 64 字节头；之后为 snapshot data、原格式的 indexed/termed entries，最后为整个前缀的 CRC32。没有快照时 index/term/length 均为零。旧 v1/v2 镜像仍可导入。

活跃文件为 `raft.wal.<generation>`。每条记录：

| 字段 | 字节 |
| --- | ---: |
| `YRJL`、version=1、kind、payload length | 16 |
| 连续 sequence，从 1 开始 | 8 |
| reserved=0、前 28 字节的 CRC32 | 8 |
| payload | 可变 |
| 以上 header+payload 的 CRC32 | 4 |

kind=1 的 payload 是 term/vote（16 字节）；kind=2 为 first index、count（16 字节），随后每条包含 index、term、uint32 command length、command bytes。每条记录独立校验头长度、完整 CRC、序号、索引连续性、term 单调性和逻辑容量。CRC 用于检测意外损坏，不是密码学认证。

## 检查点原子切换

持有目录独占锁期间，按以下顺序操作：

1. 创建下一代空 journal，`fdatasync`，再同步目录，确保它先于检查点引用持久存在。
2. 编码完整下一状态到 `raft.state.tmp`，写完并 `fdatasync`。
3. 原子 rename 覆盖 `raft.state`，再 `fsync` 目录。
4. 切换进程内 generation／journal，最后移除旧代次 journal 并同步目录。

因此，切换途中被杀，旧检查点仍可找到旧 journal，或新检查点可找到已经同步创建的新 journal。只有检查点的 generation 决定恢复输入；不能拼接目录中的其他 journal。恢复后清理孤立的旧／未发布代次。缺少检查点或其指定 journal 时保守拒绝启动，不能把缺失文件当作空数据。

## 崩溃恢复与错误

先校验完整检查点，再按序重放其指定 journal。只有物理上不完整的最后一条记录可以被截断；完整记录的 CRC、头格式或序号错误，即使发生在末尾，也拒绝启动。恢复会同步截断结果和读到的完整 journal，避免把先前未同步但可见的状态直接当作可安全确认的状态。

追加、文件同步、检查点发布或目录同步失败后，实例进入失败状态，不继续提供可能过期的数据。追加后未收到成功的操作仍可能存在于恢复结果中；客户端不能据此自动重放写入。

测试覆盖每一种最终记录截断位置、完整损坏记录、追加三个边界与检查点四个边界的 SIGKILL、冲突替换、截断、旧格式升级和日志回收。真实断电、硬件虚报刷盘完成和任意介质损坏不在已经验证的范围内。

## 指标、容量与备份

`status` 增加 checkpoint/journal 保存次数、当前文件字节数、累计编码+CRC、write、同步+发布耗时，以及文件／目录同步次数。`disk_bytes` 统计成功持久化的用户态写入字节，不能解释为设备物理写入量。`state_bytes` 是逻辑状态编码大小；v3 文件头比旧格式额外占用少量字节。

活跃 KV 编码快照 16 MiB、逻辑持久状态 64 MiB 的上限保留。实际磁盘需要额外容纳 journal 以及检查点切换过程中的旧／新副本。

备份仍要求整组停止并复制完整节点目录，必须包括检查点指定的 journal。备份清单现在会检查该依赖。升级前保留整组旧备份；旧程序和 image 模式不能直接打开已升级的 v3 目录，回退应恢复升级前的整组副本。当前不支持混用旧二进制的滚动升级。
