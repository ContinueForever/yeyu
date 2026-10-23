# yewukv

基于 C++17 的分布式键值存储实验项目。当前实现固定三节点、单 Raft Group，支持持久化选举与日志、多数派提交、强一致 KV 读写、状态机快照和落后副本追赶。

节点默认只监听本机，支持通过 `--listen-host` 指定私网 IPv4，并提供[三台云服务器部署步骤](docs/cloud-deployment.md)与系统级后台服务。当前验收范围仍是 **一台 Linux 机器上的三个进程**；实际跨机器、宿主机故障、真实断电和生产性能仍待验证。

首次使用可从下面的构建与三节点启动步骤开始；设计、操作说明和历次实验统一收录在[文档导航](docs/README.md)，修改与验证流程见[开发说明](CONTRIBUTING.md)。

## 已实现的能力

- **网络层**：基于 `epoll` 的 Reactor、one-loop-per-thread、`eventfd` 跨线程唤醒，TCP 长度前缀帧和 Protobuf RPC。
- **Raft 核心**：持久化 term/vote/log，随机选举、心跳、日志冲突修复、当前任期多数派提交，以及可按种子重放的逻辑故障模拟。
- **KV 状态机**：PUT / DEL / PING 经日志提交后按序执行；GET 默认使用 ReadIndex，经本轮多数派确认并等待应用位置。明确的 `NOT_LEADER` 可以重试，结果未知的写入不会自动重放。
- **增量持久化**：默认追加同步 Raft journal，按代次发布原子检查点；保留 image 持久化与日志读模式，可用同一二进制做独立对照。
- **快照与恢复**：原子发布 KV 快照及保留日志，回收已应用前缀，分块 InstallSnapshot，接收方重启后重新传输，并兼容旧版磁盘格式。
- **资源保护**：有界事件／提案队列、活跃数据容量、连接数和回复缓存；慢读取连接隔离，磁盘错误时保守停用节点。
- **本机运维**：三节点启动、诊断、整组停止后备份、清单校验和原子目录恢复。
- **私网部署**：显式 IPv4 监听、固定成员地址、独立服务用户及系统级 systemd 开机自启；安装前核对地址与数据身份。
- **原单节点服务**：独立的文本／RPC 接口、同步 WAL、恢复及 WAL 回收，仍可单独运行。

## 验证结果

2026-10-22 的[本机继续优化报告](docs/local-followup-20261022.md)记录了读确认与写复制重叠、自适应快照、CRC 优化以及恢复验证：

| 本轮验证 | 本机结果 |
| --- | --- |
| 普通构建 / ASan + UBSan | 各 125 项 CTest 通过，启用泄漏检查 |
| 工作区磁盘上的补充恢复 | 43 项通过 |
| 冻结旧版本 / 新版本与同二进制策略对照 | 72 轮、4,353,146 次计时操作，逐请求核对通过 |
| 50% PUT、128 B、64 key | 默认 ReadIndex 吞吐约 +19.7%，P99 5.637 → 4.427 ms |
| 15 MiB 冷 value ＋小热集合 | 默认吞吐约 6.62 倍，P99 320.959 → 3.959 ms，写入字节/操作约 -97.6% |
| 大状态全节点强杀 / 重启 | 27 轮、29,376 个最终值均一致 |
| 新旧两个 v3 二进制整组切换 | 五阶段读写、删除和独立 CRC 校验通过 |
| 延长真实故障活动 | 一小时、3,302 轮、176,543 次模型操作通过；最终全节点恢复通过 |

以上为同机闭环负载的三轮统计量中位数。部分场景日志读仍更快；均衡场景 P99.9 变差，15 MiB 新默认单次请求仍出现约 164 ms。检查点仍同步生成，不能把 P99 改善解释为消除了最长暂停。详见报告中的完整对照与限制。

历次基线、版本差异和原始证据见[实验索引](docs/README.md#实验与验收)。历史数字对应各自的源码归档，不能混作当前版本的结果。模型检查是有界检查，不能替代形式化证明。

[配置存储示例](examples/README.md)提供发布、读取、动态刷新和过期健康状态，可用于体验完整应用闭环。

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

三台服务器各运行一个节点时，使用各自的私网 IP 和一致的成员列表，按[云服务器部署说明](docs/cloud-deployment.md)操作。回环部署的数据目录不能改成员地址后直接复用。

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

- [Raft 持久化格式与原子性](docs/raft-persistence.md)
- [增量 journal、检查点与升级](docs/journal.md)
- [ReadIndex 一致性契约](docs/read-index.md)
- [配置存储应用](examples/README.md)
- [快照、容量与兼容性契约](docs/snapshots.md)
- [Raft 模块集成契约](docs/raft-contract.md)
- [性能与故障实验复现](benchmarks/README.md)
- [当前优化与验收报告](docs/local-followup-20261022.md)
- [全部文档与实验索引](docs/README.md)
- [开发与提交前验证](CONTRIBUTING.md)
- [后续路线与验收标准](docs/roadmap.md)

```text
src/net/       Reactor、TCP 连接与缓冲
src/rpc/       Protobuf RPC、帧编解码与异步客户端
src/raft/      持久化、选举、日志、快照与节点状态机
src/cluster/   三节点运行时、KV 状态机与诊断
src/storage/   原单节点 WAL 存储与执行队列
app/           服务端、集群节点与客户端入口
proto/         消息定义
tests/         单元测试、真实进程测试及模型检查器
scripts/       本机运维、备份与故障活动工具
benchmarks/    负载工具、报告与已发布证据
examples/      配置发布与动态刷新应用
docs/          设计契约、操作说明与验收报告
deploy/        单节点 Docker Compose、云节点 systemd 服务
```

## 当前边界与后续工作

当前为固定三个 voter、单 Raft Group；活跃 KV 编码快照上限 16 MiB，逻辑持久状态上限 64 MiB。默认增量追加日志和 ReadIndex；检查点仍全量编码，实际跨机器验收及多日稳定性测试仍是后续方向。实际磁盘需额外容纳 journal 和原子发布过程中的副本。

LSM / SSTable、Multi-Raft、分片、动态成员变更、TLS 和身份认证尚未实现。历史实验附带实际运行时源码归档和二进制哈希，不能仅凭较早的 Git HEAD 复现当时的未提交实现。
