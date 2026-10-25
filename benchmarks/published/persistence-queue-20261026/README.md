# 有序持久化接口：源码增量与回归记录

这是接口及确认边界阶段，契约见[设计说明](../../../docs/persistence-queue.md)。默认 Server 仍直接使用同步 FileStore；未接入独立磁盘线程或非阻塞 Raft owner，未运行新的云端／性能实验，也未宣称尾延迟改善。

最终普通／ASan＋UBSan 各 **169/169** 通过，分别 66.83／82.29 秒，sanitizer 开启泄漏检测。新增 15 项组件测试，包括真实 Node／RaftLog／FileStore 的完成通知、投票／追加／快照确认、同步后失败、字节等价及恢复；专项选择为 15 项组件＋1 项三进程追踪，共 16 项。原有 154 项计入完整 169 项，不重复累加。

首次普通回归 168/169 通过：旧三进程追踪测试要求自然短时负载必须产生持久化交集，但该次采集合法地得到零交集。现复用测试专用 I/O interposer，仅在该追踪负载期间延迟 Leader 的 Raft 文件同步 5 ms，保留非零交集断言并验证文件同步区间实际相交；不修改生产程序。原资源测试的默认 250 ms 延迟保持原值。

首次 ASan 完整 169 项通过的日志也保留。另一个较早的 ASan 构建在“列出测试名称”时受沙箱 ptrace／LeakSanitizer 限制，属于测试发现环境错误；仅测试发现改用关闭泄漏检测，实际完整回归仍使用 `detect_leaks=1`。所有记录明确分开，不以重跑掩盖首次失败。

## 文件与核对

| 文件 | 范围 |
| --- | --- |
| `tests/ctest-{normal,asan}.log.gz` | 修正后完整 169 项 |
| `tests/last-test-{normal,asan}.log.gz` | 对应完整回归的逐测试输出 |
| `tests/ctest-normal-initial-failure.log.gz` | 修正前普通回归的零交集断言失败 |
| `tests/ctest-asan-before-trace-fix.log.gz` | 修正前 sanitizer 的 169 项成功记录，单独标记版本差异 |
| `tests/targeted-{normal,asan}.log.gz` | 修正后 16 项专项 |
| `tests/build-asan-discovery-environment.log.gz` | 构建中列测试名称时的运行环境错误，非实际回归失败 |
| `source/changes.tar.gz` / `source/changes-manifest.json` | 基于 `3b2d520412b17914be8852e5ecb7d151730dc31f` 的本轮代码／文档增量及逐文件摘要 |
| `manifest.json` | 测试范围、工具链、选项及被测本机程序摘要 |
| `SHA256SUMS` | 全部归档文件完整性摘要，路径相对仓库根目录 |

在仓库根目录核对：

```bash
sha256sum --check benchmarks/published/persistence-queue-20261026/SHA256SUMS
```

摘要用于核对文件完整性；正确性依据是可复跑的当前源码测试和 GitHub CI。归档代码供查看及重建指定版本，完整性检查不执行其中的脚本。日志仅替换私有工作目录路径，保留测试名称、计数、时间和失败信息；未公开二进制、节点数据库或连接凭据。旧实验归档不变。
