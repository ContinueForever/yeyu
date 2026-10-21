# Raft 磁盘持久化（2026-10-20）

`raft::FileStore` 为 `Election`、`RaftLog` 提供真实磁盘存储，并通过 `raft::Node` 接入三节点运行时。下文保留 v1 磁盘基线说明；v2 快照及日志回收见[快照契约](snapshots.md)。当前服务的多数派提交、日志读屏障及故障验证见[集群说明](cluster.md)。

## 数据及接口

- `HardStateStore::Save` 保存 `currentTerm` 和 `votedFor`，拒绝任期倒退和同一任期改投／清除已有投票。
- `LogStore::ReplaceFrom` 保留指定索引之前的已保留前缀，替换后缀；空后缀可以截断。索引从快照边界 + 1 连续递增（无快照时从 1 开始），term 非零且不下降，不得超过已保存的 currentTerm。
- `LogStore::SaveSnapshot` 原子保存快照与保留后缀，格式和恢复契约见[快照说明](snapshots.md)。
- `ReadState()` 返回一致的 hard state 和日志副本。已失败的实例禁止读取及继续写入，须销毁后重新打开。
- 每个节点使用独立目录，例如 `node-1/raft`。该目录用 `LOCK` 做进程独占，不能与 KV `WalEngine` 共用同一个目录。

初始化示例：

```cpp
raft::FileStore store("node-1/raft");
auto recovered = store.ReadState();
raft::Election election(1, {1, 2, 3}, recovered.hard_state, store);
raft::RaftLog log(std::move(recovered.entries), recovered.snapshot.index, store,
                  std::move(recovered.snapshot));
```

调用方必须为当前 Election/RaftLog 提供唯一的节点状态机所有者和固定 voter 配置，并以磁盘日志末尾生成投票请求。磁盘文件目前不绑定 node ID/voter 配置，不能将不同节点的目录互换。处理较高任期的 AppendEntries 时，先验证发送方身份并通过选举核心持久化任期，再调用日志核心；存储成功前不能发送批准投票或追加成功回复。

## 原子写入协议

每次更新将 **hard state、可选快照与保留日志一起** 编码为一个镜像：

1. 持有目录锁，移除上一次中断留下的 `raft.state.tmp`。
2. 独占新建临时文件并写完整镜像。
3. `fdatasync` 临时文件。
4. 原子 `renameat` 替换 `raft.state`。
5. `fsync` 所在目录，然后更新内存状态并返回成功。

初始化也会同步目录祖先，覆盖新创建目录的持久性。恢复时只读取发布文件并再次同步目录，避免上次进程在 rename 后、目录同步前退出后，新的同状态 Save 未经目录同步就返回成功。

目录同步是单独必要的步骤，见 [Linux fsync 手册](https://man7.org/linux/man-pages/man2/fsync.2.html)。写入或同步失败后，本实例停止服务；rename 之后失败时磁盘可能已经是新镜像，调用方不得把失败理解成“写入一定没发生”。

## 磁盘格式 v1

所有整数使用大端；命令是允许包含 NUL 的原始字节。

| 字段 | 字节数 |
| --- | ---: |
| magic `YRFT` | 4 |
| version `1` | 4 |
| currentTerm | 8 |
| votedFor（0 表示未投票） | 8 |
| entry count | 8 |
| 每条日志：index / term / command length / command | 8 / 8 / 4 / 可变 |
| CRC32（覆盖此前所有字节） | 4 |

恢复会检查文件类型、版本、总大小、CRC、条目数量、命令边界、连续索引及任期规则。发布文件截断、损坏或出现多余字节时拒绝启动，保留原文件；不能通过清空文件“恢复”，否则可能忘记已投出的票。临时文件不是恢复候选，始终以发布文件为准。

## 容量与安全边界

- 默认镜像上限 64 MiB；可通过 Options 调小，用于资源控制和测试。单条命令最多 1 MiB。
- 每次更新全量复制、校验并重写快照及保留日志；自动快照回收历史前缀，但活跃数据较大时仍有写放大。尚不是分段追加 WAL。
- 镜像是磁盘编码方式；v2 在镜像中加入真正的 KV/Raft 快照，节点层另有 InstallSnapshot 分块传输。
- 快照边界是已提交且已应用的下界；快照后的 commit/applied index、角色、时钟和复制进度不在文件中。保留日志可能包含未提交条目，不能全部直接应用到 KV。
- 节点先恢复快照（无快照时为空），由协议重新确认提交位置后按序回放后缀；独立 KV WAL 不能作为 Raft 日志。
- 持久性依赖本地文件系统正确执行 `fdatasync`、原子替换和 `fsync`。已覆盖进程崩溃，没有进行真实断电、磁盘控制器故障或网络文件系统验收。

## 验证

磁盘模块单独落地时，完整 CTest **73/73 通过**；ASan/UBSan 下 Raft 测试 **32/32 通过**，启用泄漏检查。此后已增加节点模拟器和集群测试，最终集成结果见[集群说明](cluster.md)。

新增 18 项 `RaftFileStore` 测试，与已有 14 项 Raft 规则测试一起运行：

```bash
./build/bin/yewu_tests --gtest_filter='Raft*'
ctest --test-dir build --output-on-failure -j2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
  ./build-asan/bin/yewu_tests --gtest_filter='Raft*'
```

核心测试覆盖实际磁盘上的重启投票限制、冲突后缀替换、截断及继续追加；所有长度的截断镜像、CRC 错误及 CRC 正确但语义非法的镜像都会被拒绝。写盘故障注入覆盖文件同步前、rename 前、目录同步前及同步后，检查失败实例停用和重新打开后的状态。真实 SIGKILL 覆盖同样四个阶段的后缀替换，以及对已经成功返回的投票和追加进行强杀恢复。

SIGKILL 测试证明进程崩溃下的行为；rename 后但目录未同步的断电结果仍取决于文件系统，不能把进程测试表述为断电实验。
