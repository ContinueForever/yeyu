# yewukv

基于 C++17 的分布式键值存储实验项目。当前实现固定三节点、单 Raft Group，支持持久化选举与日志、多数派提交、强一致 KV 读写、状态机快照和落后副本追赶。

当前部署范围是 **一台 Linux 机器上的三个进程**。跨机器部署、宿主机故障、真实断电和生产性能仍待验证。

## 已实现的能力

- **网络层**：基于 `epoll` 的 Reactor、one-loop-per-thread、`eventfd` 跨线程唤醒，TCP 长度前缀帧和 Protobuf RPC。
- **Raft 核心**：持久化 term/vote/log，随机选举、心跳、日志冲突修复、当前任期多数派提交，以及可按种子重放的逻辑故障模拟。
- **KV 状态机**：PUT / GET / DEL / PING 都先经过日志提交再按序执行；明确的 `NOT_LEADER` 可以重试，结果未知的写入不会自动重放。
- **快照与恢复**：原子发布 KV 快照及保留日志，回收已应用前缀，分块 InstallSnapshot，接收方重启后重新传输，并兼容旧版磁盘格式。
- **资源保护**：有界事件／提案队列、活跃数据容量、连接数和回复缓存；慢读取连接隔离，磁盘错误时保守停用节点。
- **本机运维**：三节点启动、诊断、整组停止后备份、清单校验和原子目录恢复。
- **原单节点服务**：独立的文本／RPC 接口、同步 WAL、恢复及 WAL 回收，仍可单独运行。

## 验证结果

[2026-10-21 本机验收报告](docs/local-acceptance-20261021.md)记录了测试方法、限制和完整结果：

| 验证 | 本机结果 |
| --- | --- |
| 普通构建 / ASan + UBSan | 各 103 项测试通过，sanitizer 验收启用泄漏检测 |
| 逻辑故障模拟 | 100 个种子 × 1000 步，覆盖丢包、乱序、重复消息、隔离、重启和快照 |
| 真实进程持续故障 | 15 分钟、948 轮，50,970 次操作通过逐 key 寄存器模型检查 |
| 快照回收 | 累计确认写入 73.7 MB，观察到的最大已发布镜像约 1.05 MB，全节点重启后恢复 |
| 多负载性能实验 | 27 轮、348,770 次计时操作，保留逐请求延迟与逐秒趋势 |
| 同负载快照阈值对照 | 该实验中吞吐约 3.35 倍、P99 降低约 77.5% |

性能数字来自同机三个节点与发压进程，是特定负载下的开发基线。模型检查是有界检查，不能替代形式化证明。[公开证据与校验方法](benchmarks/published/local-20261021/README.md)可用于核对原始样本及故障历史。

## 构建与测试

Ubuntu 24.04 可以先安装系统依赖：

```bash
sudo apt-get update
sudo apt-get install -y g++ cmake ninja-build protobuf-compiler libprotobuf-dev libgtest-dev python3
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j2
ctest --test-dir build --output-on-failure -j2
```

产物位于 `build/bin/`：`yewukv-node`、`yewukv-client`、`yewukv-server`。测试会启动自己的临时集群，需要本机 TCP 权限。

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

[集群操作说明](docs/cluster.md)介绍服务管理、超时语义、备份及恢复。旧程序不能读取 v2 快照目录；升级时停止全部节点，并保留升级前的整组备份。

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
- [快照、容量与兼容性契约](docs/snapshots.md)
- [Raft 模块集成契约](docs/raft-contract.md)
- [性能与故障实验复现](benchmarks/README.md)
- [当前验收报告](docs/local-acceptance-20261021.md)
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
deploy/        原单节点 Docker Compose
```

## 当前边界与后续工作

当前为固定三个 voter、单 Raft Group；活跃 KV 编码快照上限 16 MiB，持久化镜像上限 64 MiB。每次落盘仍重写快照和保留日志，GET 仍走日志提交。分段追加日志、ReadIndex、跨机器适配及更长时间稳定性测试是后续方向。

LSM / SSTable、Multi-Raft、分片、动态成员变更、TLS 和身份认证尚未实现。历史实验附带实际运行时源码归档和二进制哈希，不能仅凭较早的 Git HEAD 复现当时的未提交实现。
