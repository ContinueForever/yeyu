# 持久化分阶段观测、CAS 历史与长窗口验证

本轮先交付可快速回归的诊断与验证工具，再运行云端长窗口。[六小时实际验收](cloud-soak-20261024.md)已完成：360 个窗口、12,872,548 次已发请求成功，另有 87,452 个未发计划时刻；原始证据可离线重算。新增指标与工具不表示已经完成后台磁盘发布、独立客户端容量验收或 Watch。

短任务提交 `c4c02f1` 的普通／ASan＋UBSan 各 148 项通过，启用泄漏检查；[短任务证据](../benchmarks/published/tooling-20261024/README.md)保留测试日志、CAS 历史及实际源码。C++ 发压器另覆盖错值拒绝、样本上限和 SIGTERM 保存后退出。

## 持久化指标

`status` 新增下列单调累计字段；重启进程后清零，跨窗口求差前应确认同一 PID／节点身份。统计运行期成功完成的相应阶段，失败调用可能只留下部分前序指标；不能将失败进程的计数当作完整成功记录。

| 字段 | 含义 |
| --- | --- |
| `disk_crc_ns` | 编码中的 CRC 耗时，是 `disk_encode_ns` 的子集，不能重复相加 |
| `disk_file_sync_ns` | 运行期被计量的文件 `fdatasync` 耗时 |
| `disk_directory_sync_ns` | 运行期目录 `fsync` 耗时，包含旧 journal 清理 |
| `disk_checkpoint_max_ns` | 成功的 image／journal 检查点路径最长耗时，包含该路径编码与发布 |
| `disk_journal_sync_max_ns` | 成功 journal delta 的文件同步最长耗时 |
| `disk_latency_bounds_ns` | 下方两个直方图的共同有限边界，单位 ns |
| `disk_checkpoint_duration_buckets` | 成功检查点耗时的非累计区间计数 |
| `disk_journal_sync_duration_buckets` | 成功 journal delta 文件同步的非累计区间计数 |

有限边界为 10／50／100／500 μs，1／2／5／10／20／50／100／200／500 ms，1／5 s，另一个桶容纳大于 5 s 的值。桶以右边界闭合；求窗口分布应逐桶相减，不能将最大值相减得到窗口最大值。直方图只提供区间精度，精确请求百分位仍从客户端原始样本计算。

既有 `disk_sync_ns` 保留原统计范围，还包括部分发布／回调开销。恢复启动的全部 I/O 并未统一计入这些运行期字段。磁盘格式、同步确认与原子发布次序保持原契约。

## Revision／CAS 历史检查

`tests/revision_history.py` 是单独的有界检查器，不改变较早公开 PUT／GET／DELETE 归档的模型。它检查新建 key 的已版本化语义：读取值与版本、CAS 成功／冲突、删除重建、成功修改的版本单调性。每 key 最多 22 个操作。

超时或丢失回复的修改仍可在超时后生效，也可能不生效。未观察到的版本以存在性区间表示，后续 GET／CAS 会约束区间；不擅自猜测精确 Raft 索引。该检查不覆盖旧数据迁移、跨 key 事务或完整内部日志顺序。

真实进程回归 `cluster_revision_fault_history` 保留三个故障历史：Leader 强杀、暂停和隔离，附带刻意丢失回复、并发 CAS、ABA、快照及整组重启。历史保存在构建目录的 `cas-history-evidence/`，未知写入以 `optional=true, end=null` 归档，检查时转成无穷完成时间。

## C++ 发压器

构建新增 `build/bin/yewukv-bench`。它面向 Linux、数字 IPv4 和明确的单个 Leader 地址，使用每工作线程一条 TCP 长连接，每连接最多一个在途请求；传输期限覆盖整帧收发，不自动重连或重放写入。SIGTERM 会请求有界退出并保留已完成样本，标记该轮无效。

```bash
build/bin/yewukv-bench --host 127.0.0.1 --port 9101 \
  --clients 8 --duration-ms 10000 --warmup-ms 1000 \
  --write-percent 50 --value-size 128 --offered-rps 500 \
  --prefix my-isolated-benchmark --output /tmp/my-new-cpp-run
python3 - <<'PY'
import sys
sys.path.insert(0, 'benchmarks')
from cpp_load import summarize
print(summarize('/tmp/my-new-cpp-run'))
PY
```

工具会写入 `prefix-client-key` 的独立键空间（每连接 8 key），应选择专用前缀／实验集群。输出目录必须不存在。`--offered-rps 0` 为闭环；正值按连接分配计划时刻，错过的时刻记为未发，不无限堆积。计时排除连接、预装和预热，包含请求编解码、校验、客户端调度与最后在途请求完成。

`result.json` 记录计划、成功、失败、未发和计时边界；各 `client-N.json` 保存全部请求的实际／计划时间、操作与结果。返回码 0 表示结构完整且无请求错误；2 表示工具记录了错误或样本达到上限；1 表示启动／参数／输出错误。错误读值记为 −2，传输或协议错误记为 −1，其他数字对应服务端返回码。遇到未知写入会停止该次负载并保留记录。

当前样本 schema 2 用全局到达序号计算 `floor(index × 1e9 / rate)`，再将序号分配给连接，避免反复取整周期在窗口末尾多排一个时刻。核验器仍能读取 schema 1 的早期烟测；正式长窗口要求 schema 2。32 连接、600 次／秒的非整周期回归覆盖这一边界，最初烟测保留为工具问题记录。

默认 `--max-samples 1000000`，按连接向上取整分配，合计最多上限＋连接数−1 条。达到上限主动结束并标记无效，不能把中断结果作为成功性能轮。`cpp_load.summarize` 重算日历和逐请求计数，`final_values` 可重构成功轮的最终键值，供独立 RPC 校验。预装与 warmup 字节不计为计时请求。

## 六小时云端窗口

先按[隔离实验部署说明](../benchmarks/published/engineering-20261024/README.md#在独立实验目录复跑)准备新 tag、私网控制服务、同一节点程序与 C++ 发压器。控制端点和凭据仅在私网使用，不进入公开结果。新版临时节点／代理有 1 核 CPU 配额、512 MiB 内存、192 个任务和 7,500 秒生命周期上限；每个节点相位约一小时，控制服务另设覆盖整个实验的上限。

```bash
# 必须先做短烟测，输出目录各自全新
python3 benchmarks/cloud_soak.py --config /private/agent.json \
  --bench /private/yewukv-bench --output /private/soak-smoke --smoke
python3 scripts/verify-cloud-soak.py /private/soak-smoke
# 正式活动结束或失败后请求关闭本 tag 的控制服务
python3 benchmarks/cloud_soak.py --config /private/agent.json \
  --bench /private/yewukv-bench --output /private/soak-main --shutdown-agents
```

正式计划为 1／8／15 MiB 冷 value × sync／background，顺序交错，共六个相位。每相位 60 个 60 秒窗口，总计 21,600 秒计时负载；窗口间还有预装、预热、压缩、检查和相位重启，实际墙钟会更长。各窗口轮换 0／50／90% 写入，32 个连接、128 B 热 value、计划 600 次／秒。使用默认 adaptive、128 条／1 MiB 快照参数、4 MiB journal 检查点和 1,500 ms 请求期限。

所有节点和数据路径独立。客户端仍与节点 3 的 Follower 共置，原有服务继续运行；这是给定资源条件下的分窗口稳定性验证。每个配置只含一个长相位，不能代替多次重复的无偏性能对照，也不等于全天持续无间断请求或真实断电验证。

每约 10 秒保存进程资源，检查原服务状态、实验节点、可用磁盘和 RSS。低于 2 GiB 空闲磁盘、节点 RSS 超过 480 MiB、原服务状态变化或请求失败都会停止活动并保留故障。样本逐窗口压缩，结果预算 1 GiB。每相位整组重启，核对全部冷 key、256 个热 key，以及保护值的内容和 revision。

`progress.json` 每窗口更新，包含当前位置、累计计时长度、最新吞吐／尾延迟／未发数。相位资源与节点计数还包含该轮热 key 预装、预热和保护检查，不能冒充纯计时窗口指标。完整结束后用当前仓库的 `verify-cloud-soak.py` 重算；云端脚本不需要本地聊天一直连接，临时服务有独立超时收尾。

应用内的定时跟进另外负责读取进度、检查失败和最终收集材料。若应用运行在本机，电脑需在线且应用运行；云端脚本的自身监测、停止和进度文件不依赖应用在线。

日历修正的原因、初始烟测及普通／ASan 专项复核见[边界记录](../benchmarks/published/tooling-calendar-20261024/README.md)。
