# 固定三节点 Raft 服务

该版本将真实磁盘存储、选举、日志复制、多数派提交、KV 状态机和客户端连接起来。默认是当前电脑上的三个独立进程，监听回环地址，不对外网提供服务。原单机 `yewukv-server` 仍可独立运行。

## 早期集成验收（2026-10-20）

2026-10-21 新增快照、资源失败和持续故障／多负载实验，最终记录见[本轮验收报告](local-acceptance-20261021.md)。下面保留上一轮的验收范围。

- 普通构建：CTest 84/84 通过。
- ASan/UBSan 构建：CTest 84/84 通过，启用泄漏检查。
- 新增和修改的 C++ 文件通过 clang-format 21 检查。
- 三节点已注册为本机用户后台服务；通过实际 Leader SIGKILL、自动接管及被杀节点自动重启检查，已确认写入仍可读。
- 备份清单、原子恢复及恢复后读取已经在后台集群实际验证，恢复前数据副本仍保留。
- 普通和 sanitizer 构建的故障并发历史均经独立模型检查通过；节点日志保存在各构建目录的 `cluster-evidence/`。
- 三轮重复 A/B 的原始数据、二进制／源码身份及延迟样本已保存。以上为本机验收；本轮没有运行远端 GitHub CI。

## 使用

```bash
# 先按 README 编译，然后注册当前用户的后台服务
scripts/local-cluster.sh install
scripts/local-cluster.sh status
scripts/local-cluster.sh put hello world
scripts/local-cluster.sh get hello
scripts/local-cluster.sh del hello
scripts/local-cluster.sh restart
scripts/local-cluster.sh logs
scripts/local-cluster.sh stop
scripts/local-cluster.sh start
```

默认 RPC 地址为 `127.0.0.1:9101`、`:9102`、`:9103`。数据默认位于源码目录旁的 `cluster-runtime/data/node-{1,2,3}`；设置 `YEWUKV_CLUSTER_ROOT` 可以改变运行目录，安装和后续管理须使用同一值。服务随用户服务管理器启动，没有配置未登录时的系统级常驻。

也可以分别在三个终端前台运行（修改 `--id`、`--port`、`--data-dir`）：

```bash
build/bin/yewukv-node --id 1 --port 9101 \
  --members 1@127.0.0.1:9101,2@127.0.0.1:9102,3@127.0.0.1:9103 \
  --data-dir /tmp/yewukv-demo/node-1
build/bin/yewukv-client --nodes 127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103 \
  --timeout-ms 5000 put hello world
```

## 请求与一致性

- 节点使用逻辑时钟驱动选举与心跳，运行时每 50 ms 推进一次。默认选举窗口 600–1200 ms，心跳间隔 100 ms，peer RPC 超时 400 ms；拥塞和调度会延长实际时间。
- 网络收发、唯一的节点状态机线程、两个 peer 传输工作线程分工执行；有界事件队列最多 4096 项，等待提案最多 1024 项。
- 新 Leader 持久化当前任期 no-op 后开始复制。只有当前任期条目被多数派保存，才推进提交；旧任期前缀随之提交。
- PUT、GET、DEL、PING 都经过日志和多数派提交后按序应用。GET 在自己的日志位置查询内存状态，隔离的旧 Leader 不能靠本地数据完成读请求。这会增加读延迟和日志容量消耗，已通过快照回收旧日志；后续可以实现 ReadIndex 优化。
- 批处理默认最多 32 个已就绪事件；客户端命令共同进行一次本地原子持久化。没有刻意等待凑批。peer 复制仍受每包 32 条／32 KiB 限制。
- 返回 `OK` 的请求已提交并应用。返回 `NOT_LEADER` 的请求尚未追加，可重新找 Leader。追加后超时、丢连接或失去领导权时返回 `UNAVAILABLE` 或传输错误，结果未知，客户端不会自动重放。没有实现跨连接请求去重／exactly-once 语义。
- `status` 仅为本地诊断，包含角色、任期、leader ID、last/commit/applied index、等待请求数、key 数和进程内磁盘持久化指标；不是业务一致性读。

## 恢复与备份

每个数据目录持有独占锁，并保存不可变的节点／成员地址绑定。不能交换节点数据目录，也不能修改成员列表后直接复用目录。

节点重启先读取 term/vote、快照和保留日志，恢复快照中的 KV 与 applied 下界；无快照时从空 map 开始。新 Leader 的当前任期条目建立提交前缀后，各副本回放已确认后缀；未提交尾部不会自动当成成功写入。

```bash
scripts/local-cluster.sh backup /absolute/path/to/new-backup
scripts/local-cluster.sh restore /absolute/path/to/new-backup
```

备份短暂停止整个集群，复制三个节点目录，生成 SHA-256 清单并同步文件系统，之后恢复原运行状态。目标必须不存在，且不能位于活动数据目录内部。

恢复先检查完整文件清单、校验和及备份中固定的本机节点配置，再准备副本；停止全部节点后使用 Linux `renameat2(RENAME_EXCHANGE)` 原子交换数据目录。恢复前数据保留为 `before-restore-*`，不直接删除。整个数据目录缺失时也可恢复，配置校验不依赖丢失的原文件。恢复整个旧备份会回到备份时的数据状态，因此不能用此命令仅回退某个在线副本，也不能保留同一集群的其他旧进程继续运行。

本机已经实际完成备份、校验、恢复及恢复后读取验证。恢复是整个静态集群的运维操作，不等同于 Raft 在线快照传输。

## 故障与一致性测试

- 选举／日志／磁盘核心：任期投票、日志匹配、已提交条目保护、损坏镜像、写盘失败停用及四个持久化边界 SIGKILL。
- 节点模拟器：100 个固定种子，每个种子 1000 步，混合隔离、重启、丢包、乱序、重复回复和快照，检查选举唯一性、提交历史与恢复后的收敛。
- 真实 TCP：通过六条可控的有向代理切断 peer 链路，保持旧 Leader 的客户端端口可达，验证少数派不能完成读写。
- 真实进程：Leader SIGKILL、全部节点强杀重启、落后副本追赶、连接内流水线请求顺序、旧请求索引被替换后不能错误返回成功。
- 客户端：模拟明确拒绝后重新发现 Leader，以及请求发出后断连不重放。
- 并发历史：3 个客户端在 Leader 强杀期间执行 54 次混合操作，保存调用／返回时间，用独立的逐 key 穷举寄存器模型检查；结果未知的写入作为可选、无完成上界的操作处理。

```bash
ctest --test-dir build --output-on-failure -j2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
  ctest --test-dir build-asan --output-on-failure -j2
```

并发历史保存在对应构建目录的 `cluster-history.json`；测试失败时应同时保留该文件和节点日志。这些是可复现的有限实验，不能代替形式化证明或生产验收。

## 当前边界

- 固定三个 voter、单 Raft Group；已有快照、日志前缀回收和分块 InstallSnapshot；没有成员变更或分片。
- 原子镜像上限 64 MiB，活跃 KV 快照上限 16 MiB；默认每 128 个已应用位置或 1 MiB 保留日志触发快照。新 PUT 超出活跃容量时返回 BUSY，删除可回收空间。每次仍重写快照和后缀，不是分段 WAL；硬上限或真实 I/O 错误使节点保守停用。详见[快照与容量契约](snapshots.md)。
- 业务 key 最多 1024 字节，value 最多 8192 字节。Peer 消息使用已有 64 KiB RPC 帧。
- 当前仅支持回环部署；同机进程故障与网络模拟已经验证，跨主机、宿主机失效、真实断电、多日运行和真实磁盘耗尽尚未验收。本轮已通过测试专用系统调用注入验证 ENOSPC/EIO，并验证节点容量上限、队列满和慢连接。
- Peer ID 校验不是密码学认证；没有 TLS、身份认证或多租户隔离。

性能方法和原始证据见[三节点开发基线](../benchmarks/cluster-baseline-20261020.md)。
