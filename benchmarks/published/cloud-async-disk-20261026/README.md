# 三机同步／后台磁盘对照：2026-10-26

12 个正式 30 秒窗口、360 秒测量，210,783 次已发请求成功、0 次失败；5,217 / 216,000 个计划时刻未发（2.4153%）。PUT 105,210，GET 105,573，最长 544.880069 ms。正式每轮后整组三节点重启，核对冷键、256 个热键和锚值 revision；12 次通过。2 个预检窗口、2,400 次请求与两次重启单列。

同一二进制只切换 `--disk-executor sync|background`；固定后台 KV 快照编码、默认 journal／ReadIndex／adaptive、1 MiB／15 MiB 冷数据、32 连接、600 offered rps、50% 写、128 B 热值。每数据档三组配对并交替先后顺序，每轮使用新目录；测量未创建追踪记录器。客户端共置 Follower 3，各节点及发压进程所在服务均限一个 CPU／512 MiB，不能视为独立容量结果。

后台模式本轮退化：1 MiB 的每轮 P99 中位数 21.301→53.023 ms，15 MiB 为 19.690→46.805 ms；六组配对的 P99 全部变差。保留退化、未发时刻和资源代价，不延长该配置的运行窗口，也不据此改默认同步策略。完整解释见[实验报告](../../../docs/cloud-async-disk-20261026.md)。

## 核验

从仓库根目录运行：

```sh
python3 scripts/verify-cloud-async-disk.py benchmarks/published/cloud-async-disk-20261026
python3 tests/test_async_disk_evidence.py
sha256sum --check benchmarks/published/cloud-async-disk-20261026/SHA256SUMS
```

`nodes/node-3/async-main` 保留全部正式客户样本、状态、实际启动参数、资源采样与逐值恢复哈希；`async-smoke` 单列预检。`summary.json` 的分位数为每轮分位数的中位数／范围，没有当成合并分布。单次最大值来自全部已发请求；计划到达的未发时刻另计。

`source/runtime-source.tar.gz` 与清单保留实际编译的 C++ 核心；执行代码为 `66bd2a9`，工具为 `21f5601`，编译使用 GCC 13.3 RelWithDebInfo。`executed-tools.tar.gz` 保留云端实际使用的九个工具文件；核验仅运行当前仓库脚本，不执行归档代码。二进制 SHA-256 及单核／内存限制见 `control/execution.json`。

客户 gzip 样本和不含身份信息的数值 JSON 保留原字节；私网地址、主机名和工作区路径只在诊断文本／启动参数中映射。`control/export-map.json` 列出原始与公开文件的字节数和 SHA-256，映射文件不再声称字节相同；原始三份私有下载包的哈希另存。未公开私钥、控制令牌、原服务业务值、数据库或可执行二进制。原服务的 512 个旧实验键、四个保护值、程序哈希与 PID 均保持原状；只公开保护值集合的哈希。

`tests/ci-push21-failure.log.gz` 保留新增后台测试在 CI 首次出现的指标假设错误：ReadIndex 可以先在持久前缀完成多数派确认，随后继续等待写入应用，所以不一定和新日志同包发送。修正保留同步路径的同包断言，同时明确验证后台路径的新鲜多数派、50 条 PUT 的日志数量、每个 GET 的值，以及磁盘闸门内 GET 仍不完成。实际生产 C++ 未因该修正改变。相关普通／ASan 回归、七项计划检查和测试补丁一并保留；历史 186 项核心完整回归在[上一阶段](../async-persistence-20261026/README.md)。

资源 CPU 由同一进程生命期的 Linux ticks 和所属机器单调时钟计算；采样范围包含热键预载、预热及最终排空，和纯计时窗口并不完全一致。RSS 是采样最大值，不能当作真实峰值。状态指标也包含预载／预热，异步的 owner 接收耗时不能与同步磁盘发布耗时直接作提速比较。没有真实断电、多日稳定性或异步任务等待因果归因结论。
