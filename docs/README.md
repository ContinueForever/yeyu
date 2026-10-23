# 文档导航

当前实现为固定三个 voter、单 Raft Group 的 C++17 KV 服务，默认使用增量 journal、ReadIndex 和自适应快照。验证范围为同一台 Linux 机器上的三个进程。

## 使用与开发

| 需要做的事 | 入口 |
| --- | --- |
| 构建、测试、启动和读写 | [项目首页](../README.md) |
| 修改代码与提交前检查 | [开发说明](../CONTRIBUTING.md) |
| 本机服务、状态、超时、备份和恢复 | [集群操作](cluster.md) |
| 配置发布、HTTP 消费者和动态刷新 | [配置应用](../examples/README.md) |
| 跑性能对照或故障活动 | [实验复现](../benchmarks/README.md) |
| 当前进度、后续方向和放行标准 | [工程路线](roadmap.md) |

## 设计与正确性契约

| 主题 | 文档 |
| --- | --- |
| 网络、存储与模块边界 | [总体设计](design.md) |
| 状态机、持久化和传输层的职责 | [Raft 集成契约](raft-contract.md)、[模块说明](../src/raft/README.md) |
| 文件格式、校验、原子发布和失败处理 | [Raft 持久化](raft-persistence.md)、[增量 journal](journal.md) |
| 新鲜多数派确认、应用位置和读写重叠 | [ReadIndex](read-index.md) |
| 快照触发、日志回收、容量和磁盘兼容性 | [快照契约](snapshots.md) |

## 实验与验收

按时间从新到旧排列。每份报告描述对应版本、负载、恢复检查与限制，证据包含原始样本、故障历史、源码归档和校验清单。

| 版本与变化 | 回归 | 性能与故障活动 | 原始证据 |
| --- | --- | --- | --- |
| [2026-10-22：读写重叠、自适应快照与 CRC 优化](local-followup-20261022.md) | 普通／sanitizer 各 125 项，磁盘补充 43 项 | 72 轮、4,353,146 次计时操作；一小时、3,302 轮、176,543 次模型操作 | [核验入口](../benchmarks/published/local-followup-20261022/README.md) |
| [2026-10-21：增量 journal、ReadIndex 与配置应用](local-optimization-20261021.md) | 普通／sanitizer 各 120 项，磁盘补充 13 项 | 36 轮、1,265,078 次计时操作；20 分钟、1,143 轮、61,113 次模型操作 | [核验入口](../benchmarks/published/local-optimization-20261021/README.md) |
| [2026-10-21：image／日志读与快照基线](local-acceptance-20261021.md) | 普通／sanitizer 各 103 项 | 27 轮、348,770 次计时操作；15 分钟、948 轮、50,970 次模型操作 | [核验入口](../benchmarks/published/local-20261021/README.md) |

验收日志中的“磁盘补充”不与完整回归数相加。性能操作、并发模型操作和顺序持久值检查分别计数。较早的单节点实验位于 `benchmarks/results/` 中已跟踪的历史目录，解释见[实验说明](../benchmarks/README.md)。

最新报告保留了均衡负载 P99.9 变差及大状态单次约 164 ms 的结果。检查点仍同步生成；跨机器、真实断电、多日稳定性和生产容量尚未验收。
