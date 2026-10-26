# yewukv

基于 C++17 的分布式键值存储实验项目。当前实现固定三节点、单 Raft Group，支持持久化选举与日志、多数派提交、强一致 KV 读写、状态机快照和落后副本追赶。

节点默认只监听本机，支持通过 `--listen-host` 指定私网 IPv4，并提供[三台云服务器部署步骤](docs/cloud-deployment.md)与系统级后台服务。验证覆盖本机三进程回归、[三机基础演练](docs/cloud-acceptance-20261023.md)、[跨机基线](docs/cloud-performance-20261024.md)及新增的[混合负载故障、后台快照与 CAS 验证](docs/engineering-next-20261024.md)。环境、版本、故障日志和逐请求样本已有可离线核验的公开归档；宿主机故障、真实断电、多日负载和生产容量仍待验证。

首次使用可从下面的构建与三节点启动步骤开始；设计、操作说明和历次实验统一收录在[文档导航](docs/README.md)，修改与验证流程见[开发说明](CONTRIBUTING.md)。

## 已实现的能力

- **网络层**：基于 `epoll` 的 Reactor、one-loop-per-thread、`eventfd` 跨线程唤醒，TCP 长度前缀帧和 Protobuf RPC。
- **Raft 核心**：持久化 term/vote/log，随机选举、心跳、日志冲突修复、当前任期多数派提交，以及可按种子重放的逻辑故障模拟。
- **KV 状态机**：PUT / DEL / PING 经日志提交后按序执行；GET 默认使用 ReadIndex，经本轮多数派确认并等待应用位置。明确的 `NOT_LEADER` 可以重试，结果未知的写入不会自动重放。
- **增量持久化**：默认追加同步 Raft journal，按代次发布原子检查点；保留 image 持久化与日志读模式，可用同一二进制做独立对照。
- **版本条件更新**：基于提交日志位置的键版本、CAS 冲突返回、删除重建后的版本保护，以及配置发布者的条件更新；见 [CAS 契约](docs/cas.md)。
- **快照与恢复**：原子发布 KV 快照及保留日志，回收已应用前缀，分块 InstallSnapshot，接收方重启后重新传输，并读取旧版磁盘格式；新增可选的[后台快照编码](docs/background-snapshot.md)，磁盘原子发布仍串行完成。
- **资源保护**：有界事件／提案队列、活跃数据容量、连接数和回复缓存；慢读取连接隔离，磁盘错误时保守停用节点。
- **本机运维**：三节点启动、诊断、整组停止后备份、清单校验和原子目录恢复。
- **私网部署**：显式 IPv4 监听、固定成员地址、独立服务用户及系统级 systemd 开机自启；安装前核对地址与数据身份。
- **原单节点服务**：独立的文本／RPC 接口、同步 WAL、恢复及 WAL 回收，仍可单独运行。

## 验证结果

完成[有序持久化提交／完成接口](docs/persistence-queue.md)第一阶段，普通／ASan＋UBSan 各 **169 项**通过。新增 15 项测试覆盖有界积压、乱序与旧完成通知、实际 Raft 投票／日志／快照确认，以及同步后失败的恢复；[源码增量与回归记录](benchmarks/published/persistence-queue-20261026/README.md)可核对。后续[后台磁盘执行与非阻塞 Raft 主循环](docs/background-persistence.md)已接入可选 `--disk-executor background`，默认服务仍同步落盘；最新普通／ASan＋UBSan 各 **186 项**、队列与后台持久化组件 TSan **31 项**通过。新增真实三进程验收覆盖慢盘心跳、强杀、磁盘错误、CAS 与快照追赶，尚无云端提速结论。

新增[持久化与读请求的同线程时间关联](docs/persistence-tracing.md)，普通／ASan＋UBSan 各 **154 项**通过。[三机开关对照](docs/cloud-persistence-trace-20261025.md)完成 12 个窗口、212,268 次成功请求，另有 3,732 个未发计划时刻；开启组 53,065 个 GET 全部关联。慢 GET 与持久化的累计区间交集约 60%／66%，包含调度等待且不能当作可实现收益；追踪增加 CPU 开销，默认关闭。[完整证据与复算](benchmarks/published/persistence-trace-20261025/README.md)已公开，后台磁盘发布仍待实现。

新增[持久化分阶段指标、CAS 故障历史与 C++ 发压器](docs/long-window.md)，短任务普通／ASan 各 148 项通过，相关[源码与历史](benchmarks/published/tooling-20261024/README.md)可离线核验；发压到达日历修正另有普通／ASan 专项回归。

2026-10-24 / 25 完成[六小时三机稳定性与恢复验收](docs/cloud-soak-20261024.md)：1／8／15 MiB × sync／background 共 360 个一分钟窗口，12,872,548 次已发请求成功、0 次失败，另有 87,452 个未发计划时刻（0.674784%）。六次整组重启后的冷键、热键与锚值 revision 均验证通过，原服务与保护值保持原状。[完整样本与离线核验](benchmarks/published/cloud-soak-20261024/README.md)已公开；最长请求仍为 520.703 ms，后台编码仍有 391.430 ms 的快照发布暂停。客户端与 Follower 共置，每配置一个约一小时相位，不能据此宣称容量上限或多日稳定性。

最新[三机工程验证](docs/engineering-next-20261024.md)：普通／ASan 各 143 项通过，状态机／快照组件 TSan 13 项通过。完成 18 轮真实混合负载故障、2,491 个有界模型操作，另完成 42 轮正式负载对照、637,823 次已发计时请求；全部样本及探索轮也保留在[公开归档](benchmarks/published/engineering-20261024/README.md)。

| 本轮变化 | 实测结果与边界 |
| --- | --- |
| C++ 后台快照编码 | 8 MiB 冷 value＋固定 128 条快照压力下，吞吐中位数 +40.46%，P99 270.133→175.921 ms；后台组仍有 299.259 ms 请求，磁盘发布仍阻塞，默认保持 sync |
| 负载与过载行为 | 三种比例、8／32／128 并发及三档计划速率；保留 148,257 个未发计划时刻；小队列探测返回 1,016 BUSY／8 OK，后续读写恢复 |
| 配置条件更新 | revision／CAS、并发发布者冲突、ABA 保护、快照恢复及旧 v1 目录整组升级；未实现 Watch |

只有现有三台机器，发压仍与 Follower 共置，不能据此宣称独立服务端容量。故障活动另保留 48 个结果未知写入；上表的零失败负载数据不能替代故障结果或未发到达计数。

新增 [48 轮读路径／调度公开数值证据](benchmarks/published/cloud-diagnosis-20261024/README.md)，包含 5,024,663 次计时 GET 与逐请求核验。原程序在固定机器和 32 连接热读下，经 CPU 放置调优，吞吐中位数约 +32%、P99 43.225→7.617 ms；仍保留 87.525 ms 单次尾部。这是部署调优结果，条件和限制见[报告](docs/cloud-scheduling-20261024.md)。

2026-10-23 / 24，在现有三台 2 vCPU / 4 GiB ECS 上完成 [45 轮跨机压测](docs/cloud-performance-20261024.md)：三种读写比例、1/4/8/16/32 并发、每轮 30 秒、每组合重复三次，共 1,954,767 次计时请求，未记录失败。客户端与一个 Follower 共置，128 B value、每连接 8 个独立 key。32 并发的三次统计量中位数为：

| 负载 | 吞吐 ops/s | P95 ms | P99 ms |
| --- | ---: | ---: | ---: |
| 纯读 | 5,868.4 | 7.269 | 43.611 |
| 50% 写入 | 2,336.8 | 22.488 | 27.235 |
| 90% 写入 | 2,200.9 | 22.899 | 27.125 |

原始样本、三次范围、P99.9、最大值与重算脚本见[性能归档](benchmarks/published/cloud-performance-20261023/README.md)。纯读高并发 P99 与 P95 明显分离；写密集轮次中仍有 152.175 ms 的单次 GET。这是已有小数据集的闭环基线，未测出独立客户端条件下的容量上限。

2026-10-23，源码 `909df76` 在三台 Ubuntu 24.04 ECS 上各通过 126 项普通构建 CTest，并完成六个[基础手工场景](docs/cloud-acceptance-20261023.md)。两次停止当时的 Leader 后，剩余两台均能读取旧值并写入新值；只剩一个节点时读写不返回成功；全停恢复后四个已确认值均读回，三个节点的提交与应用位置均为 11。三台源码提交与运行二进制哈希一致；五项故障/恢复有文本记录及离线核验，首次三条基础读写序列保留截图核对。实验为少量顺序请求，不包含跨机性能测量。

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

- [Raft 持久化格式与原子性](docs/raft-persistence.md)
- [增量 journal、检查点与升级](docs/journal.md)
- [ReadIndex 一致性契约](docs/read-index.md)
- [配置存储应用](examples/README.md)
- [快照、容量与兼容性契约](docs/snapshots.md)
- [Raft 模块集成契约](docs/raft-contract.md)
- [性能与故障实验复现](benchmarks/README.md)
- [当前三机工程验证报告](docs/engineering-next-20261024.md)
- [读调度与自适应快照优化](docs/local-followup-20261022.md)
- [三台 ECS 基础正确性手工记录](docs/cloud-acceptance-20261023.md)
- [三台 ECS 性能基线与尾延迟](docs/cloud-performance-20261024.md)
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

当前为固定三个 voter、单 Raft Group；活跃 KV 编码快照上限 16 MiB，逻辑持久状态上限 64 MiB。默认增量追加日志、ReadIndex、自适应快照及同步编码；可选后台编码仍同步发布磁盘检查点。三台 ECS 的基线、尾延迟关联、18 轮混合负载故障、容量探测及六小时默认策略窗口已完成；同步发布剩余暂停、独立发压机、重复长对照、多日稳定性和真实断电仍待补充。实际磁盘需额外容纳 journal 和原子发布过程中的副本；升级 CAS 快照须整组停机备份，不支持旧程序打开新快照。

LSM / SSTable、Multi-Raft、分片、动态成员变更、TLS 和身份认证尚未实现。历史实验附带实际运行时源码归档和二进制哈希，不能仅凭较早的 Git HEAD 复现当时的未提交实现。
