# 后台磁盘执行与非阻塞 Raft：2026-10-26 本机验收

本阶段接入可选 `--disk-executor background`：一个磁盘线程、一个待确认任务，所属线程不等待磁盘调用。默认同步模式与磁盘格式保持兼容。协议效果经过明确的准备／持久化完成边界；更高任期可在磁盘忙时立即停止旧 Leader 行为。设计、使用与限制见[实现说明](../../../docs/background-persistence.md)。

普通完整回归 **186/186，83.26 s**，ASan＋UBSan **186/186，102.67 s**（泄漏检测开启）。另有 TSan **31/31**，仅覆盖队列 15 项＋本阶段 16 项组件，不包含整个服务。一个新的 CTest 入口包含 13 项真实进程 unittest，部分用例按 journal／image 或故障模式再次运行；这些不能再次加到 186 中。

验收覆盖未落盘不能投票或复制、hard state／日志两次屏障、任期变化、先前写入的 GET 应用条件、磁盘忙时的状态与心跳、快照后缀、强杀、停机排空、ENOSPC／EIO／部分写入，以及复用的历史模型／隔离接管／CAS／快照追赶。可控闸门只在测试库内存在，测试不是性能测量。没有新云端运行或吞吐／尾延迟提升结论。

## 核对材料

- `tests/ctest-{normal,asan}.log.gz` 与 `last-test-*.log.gz`：最终全量输出。
- `tests/tsan.log.gz`：31 项组件实际执行记录；编译选项和范围见 `manifest.json`。
- `tests/first-failure-*.log.gz` 与对应 `first-failure-last-test-*`：首次完整回归均为 185/186。新增强杀用例使用的读取助手重设了 socket 超时，导致读到了应用期限到达后的 UNAVAILABLE，而不是预期的 100 ms 超时。现改用直接 MSG_PEEK，保持闸门释放前无响应的断言。`initial-helper-diagnosis.json` 保留针对旧助手的实测返回码与耗时；`corrected-runtime-*` 和最终全量输出保留修正后结果。
- `tests/ctest-startup-environment-failure.log.gz`：一次工具启动环境错误，未运行测试，和正确性失败分别记录。
- `source/changes.tar.gz`、`changes-manifest.json`：相对基线 `939399ef61ba3e7c6d3810b850fd14b9f81fb456` 的本阶段变更文件及原始字节 SHA-256。完整工程应从仓库检出；该增量用于核对被测实现与说明。
- `manifest.json`：测试数、编译／运行选项、被测二进制 SHA-256 和范围。二进制本体不公开。

从仓库根目录运行：

```sh
sha256sum --check benchmarks/published/async-persistence-20261026/SHA256SUMS
ctest --test-dir build --output-on-failure
ctest --test-dir build --output-on-failure -R '^(AsyncPersistence\.|cluster_async_persistence$)'
```

哈希检查只证明归档完整性；当前源码的正确性由实际构建、CTest 和 CI 重新执行，不能靠冻结脚本自行证明。日志只映射私有工作区根目录；测试数据、数值、成功／失败输出不改写。原有云端归档保持原样。

磁盘任务和请求应用条件已有依赖令牌；旧同线程交集分析会拒绝异步输入，完整依赖复算仍待完成。仍需关闭追踪的重复三机性能对照、资源代价及较长运行窗口后，再评估切换默认策略。
