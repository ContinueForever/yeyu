# yewukv

基于 C++17／Linux 的分布式键值存储实验项目，面向小型配置与元数据场景。实现固定三个 voter、单 Raft Group，覆盖网络通信、共识、持久化、强一致读、快照恢复和故障验证。

[设计与操作文档](docs/README.md) · [最新性能对照](docs/cloud-peer-disk-20261026.md) · [六小时稳定性报告](docs/background-soak-20261026.md) · [简历与面试说明](docs/resume.md)

## 当前能力

| 模块 | 实现 |
| --- | --- |
| 网络与 RPC | `epoll` Reactor、one-loop-per-thread、`eventfd` 跨线程唤醒；TCP 长度前缀帧、Protobuf RPC、连接生命周期与慢连接保护 |
| Raft | 持久化 term／vote／log，随机选举、心跳、日志冲突修复、当前任期多数派提交；逻辑时钟／消息模拟器支持固定种子重放 |
| 强一致 KV | 写入经多数派提交并按序应用后确认；GET 默认经 ReadIndex 新鲜多数派确认及应用屏障；支持 revision／CAS 与删除重建后的版本保护 |
| 持久化与恢复 | 增量同步 journal、原子代次检查点、单批最多 32 条提案共享持久化；KV／Raft 快照、日志回收与分块 InstallSnapshot |
| 可选后台路径 | KV 快照编码与磁盘执行是两个独立开关；有序磁盘线程通过完成通知推进 Raft，保留落盘后确认及高任期立即撤销；繁忙副本使用两槽、300 ms 有界等待 |
| 运维与应用 | 有界队列与容量保护、状态／追踪指标、停机备份与清单校验、私网 IPv4／systemd 部署；[配置发布与刷新示例](examples/README.md) |

默认值为 `--raft-store journal --read-mode read-index --snapshot-policy adaptive --snapshot-encoder sync --disk-executor sync`。后台编码和后台磁盘均已实现，但只显式启用；后台磁盘未达到预设的全部性能门槛，默认保持同步。

明确的 `NOT_LEADER` 可按客户端策略重定向；超时、断连或 `UNAVAILABLE` 后的写入结果可能未知，不自动重放。传输 request ID 不等同于持久化去重协议。具体边界见[集群操作](docs/cluster.md)、[ReadIndex](docs/read-index.md)和[后台磁盘契约](docs/background-persistence.md)。

## 验证结果与解释范围

截至 2026-10-28，交付基线的 GitHub CI 普通构建与 ASan＋UBSan 各 **189 项 CTest** 通过，另检查代码格式、公开实验原始证据及单节点 Compose 重建恢复。TSan 的 **31 项**仅覆盖持久化队列与后台执行组件；离线证据负例单列，不叠加为 CTest 数量。

| 验证 | 已观测结果 | 条件与限制 |
| --- | --- | --- |
| [三机新旧对照](docs/cloud-peer-disk-20261026.md) | 18 个 30 秒窗口、318,263 次成功请求、5,737 个未发计划时刻；18 次整组恢复通过 | 32 连接、600 offered rps、50% 写、128 B 热值；旧后台／新后台／同期新同步各三轮，按冷数据大小分别比较 |
| 有界副本等待优化 | 1 MiB 冷数据下，新后台 P99.9 从 491.046 降至 46.344 ms，约下降 **90.6%**；15 MiB 为 472.232→244.726 ms | 数字为三轮各自分位数的中位数；大状态仍为旧值 **51.82%**，未达减半门槛，整体 `performance_gate=false` |
| [可选后台磁盘六小时稳定性](docs/background-soak-20261026.md) | 360×60 秒计时；12,836,802 次已发请求全部成功、0 次失败；123,198 个计划时刻未发（0.9506%）；六次阶段整组恢复通过 | 墙钟约 6 小时 12 分 48 秒；1／8／15 MiB、两种 KV 编码器、0／50／90% 写；最长请求 597.404 ms，验收稳定性，不改变性能门槛结论 |
| 故障与正确性 | 选主／旧 Leader 隔离、少数派保护、SIGKILL、快照追赶、I/O 错误、容量保护及有界并发历史校验 | 覆盖具体回归与有限实验，不能替代形式化证明或真实断电测试；[故障报告](docs/engineering-next-20261024.md)、[CAS 历史工具](docs/long-window.md) |

云端始终使用现有三台 ECS，发压器与一个 Follower 共置；600 offered rps 是输入计划，忙时未发时刻另计，不能视作独立发压条件下的容量上限。六小时恢复在线比较全部值，但冻结记录只保留计数、revision 和状态，不能离线重建每个恢复响应字节。

公开归档保留实际版本、负载、全部客户样本、失败记录及校验清单；核验由当前仓库工具离线重算，不执行归档代码来自证。复算命令见[开发说明](CONTRIBUTING.md#实验材料与仓库边界)。历次结果见[实验索引](docs/README.md#实验与验收)，原首页与旧路线记录见[开发历史](docs/development-history.md)。

## 构建与测试

Ubuntu 24.04 可以先安装系统依赖：

```bash
sudo apt-get update
sudo apt-get install -y git g++ cmake ninja-build protobuf-compiler libprotobuf-dev libgtest-dev python3
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j2
ctest --test-dir build --output-on-failure -j2
```

产物位于 `build/bin/`：`yewukv-node`、`yewukv-client`、`yewukv-server`。请在 Git 检出中运行完整测试，故障采样工具需要 Git 元数据来归档被测源码。测试会启动自己的临时集群，需要本机 TCP 权限。

Sanitizer 构建：

```bash
cmake -S . -B build-asan -G Ninja -DCMAKE_BUILD_TYPE=Debug -DYEWUKV_ENABLE_SANITIZERS=ON
cmake --build build-asan -j2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
  ctest --test-dir build-asan --output-on-failure -j2
```

受限容器可能禁止 LeakSanitizer 检查进程；这类环境可以设置 `ASAN_OPTIONS=detect_leaks=0`，并在允许泄漏检测的环境补跑。GitHub Actions 配置了普通构建、sanitizer 和原单节点 Compose 恢复检查。

## 启动三节点

在三个终端分别启动节点。三台进程使用同一成员列表、不同 ID 和数据目录：

```bash
# 终端 1
build/bin/yewukv-node --id 1 --port 9101 \
  --members 1@127.0.0.1:9101,2@127.0.0.1:9102,3@127.0.0.1:9103 --data-dir data/node-1
# 终端 2
build/bin/yewukv-node --id 2 --port 9102 \
  --members 1@127.0.0.1:9101,2@127.0.0.1:9102,3@127.0.0.1:9103 --data-dir data/node-2
# 终端 3
build/bin/yewukv-node --id 3 --port 9103 \
  --members 1@127.0.0.1:9101,2@127.0.0.1:9102,3@127.0.0.1:9103 --data-dir data/node-3
```

在另一个终端发起请求：

```bash
build/bin/yewukv-client --nodes 127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103 --timeout-ms 5000 put hello world
build/bin/yewukv-client --nodes 127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103 --timeout-ms 5000 get hello
build/bin/yewukv-client --port 9101 status
```

使用相同目录重启节点即可恢复。节点身份与成员地址保存在目录中，不能交换不同节点的数据目录。

支持用户 systemd 的 Linux 也可使用管理脚本。它默认使用源码目录旁的独立 `cluster-runtime/` 目录：

```bash
scripts/local-cluster.sh install
scripts/local-cluster.sh status
scripts/local-cluster.sh put hello world
scripts/local-cluster.sh get hello
scripts/local-cluster.sh restart
scripts/local-cluster.sh stop
```

[集群操作说明](docs/cluster.md)介绍服务管理、超时语义、备份及恢复。旧程序不能读取新 v2/v3 目录；升级时停止全部节点，并保留升级前的整组备份。

三台服务器各运行一个节点时，使用各自的私网 IP 和一致的成员列表，按[云服务器部署说明](docs/cloud-deployment.md)操作。回环部署的数据目录不能改成员地址后直接复用。已有集群的吞吐量、P95/P99 和并发矩阵按[跨机压测步骤](docs/cloud-benchmark.md)测量；工具不会重新启动本地集群。

## 原单节点服务

```bash
build/bin/yewukv-server --port 9000 --io-threads 4 --protocol rpc --data-dir data/standalone
# 另一个终端
build/bin/yewukv-client --port 9000 put hello world
build/bin/yewukv-client --port 9000 get hello
```

不传 `--protocol rpc` 时使用文本协议。Docker Compose 当前部署此单节点服务：

```bash
docker compose -f deploy/docker-compose.yml up --build -d
# 自动检查写入、重启及容器重建后恢复；使用独立项目并清理自己的测试卷
scripts/ci-compose-smoke.sh
```

## 文档与目录

| 需要了解的内容 | 入口 |
| --- | --- |
| 模块职责、写确认与一致性 | [Raft 契约](docs/raft-contract.md)、[ReadIndex](docs/read-index.md)、[CAS](docs/cas.md) |
| 文件格式、崩溃边界与快照 | [journal](docs/journal.md)、[Raft 持久化](docs/raft-persistence.md)、[快照](docs/snapshots.md) |
| 后台任务与性能修复 | [后台编码](docs/background-snapshot.md)、[后台磁盘](docs/background-persistence.md)、[有界副本等待](docs/peer-disk-admission.md) |
| 求职使用与源码阅读 | [简历与面试说明](docs/resume.md) |
| 当前状态与可选后续工作 | [工程状态](docs/roadmap.md) |
| 所有设计、操作、实验和开发流程 | [文档导航](docs/README.md)、[开发说明](CONTRIBUTING.md) |

```text
src/net/       Reactor、TCP 连接与缓冲
src/rpc/       Protobuf RPC、帧编解码与异步客户端
src/raft/      持久化、选举、日志、快照与节点状态机
src/cluster/   三节点运行时、KV 状态机与诊断
src/storage/   原单节点 WAL 存储与执行队列
app/           服务端、集群节点与客户端入口
proto/         消息定义
tests/         单元测试、真实进程测试及模型检查器
scripts/       本机运维、备份、故障活动与证据核验
benchmarks/    负载工具与已发布证据
examples/      配置发布与动态刷新应用
docs/          设计契约、操作说明与验收报告
deploy/        单节点 Docker Compose、云节点 systemd 服务
```

## 已知边界

- 固定三个 voter、单 Raft Group；活跃 KV 的编码快照上限 16 MiB，逻辑 Raft 持久状态上限 64 MiB。磁盘还需容纳 journal、临时文件与原子发布过程中的副本，逻辑大小不等于物理占用。
- 内存状态机配合持久化日志与快照；尚无 LSM／SSTable、SQL、Multi-Raft、分片、动态成员变更、跨 key 事务或 Watch。
- 默认仅监听回环地址，可显式指定私网 IPv4；尚无 TLS 与身份认证。
- 后台路径保留线程、所有权及缓冲开销；大状态仍有明显尾延迟，完整异步等待归因未完成。真实断电、多日负载与独立发压容量未验收。
- CAS 快照升级要求整组停机备份；不支持旧程序打开新快照或新旧命令语义混跑。原单节点 WAL 与 Raft 持久化是两套独立路径。

当前作为求职项目交付，保留现有功能和证据；后续工作按[工程状态](docs/roadmap.md)选择，不以继续增加组件作为投递前提。
