# 2026-10-21 本机持久化、ReadIndex 与配置应用优化

本页保留本轮历史实现和测量；后续读调度、自适应检查点与延长故障验证见[2026-10-22 报告](local-followup-20261022.md)。

## 实现范围

- 默认增量同步 journal，加上原子发布、按代次绑定的检查点；旧 image 模式保留作对照。普通追加不重写 KV 快照或已保留日志前缀。
- 默认 GET 使用安全 ReadIndex：当前任期提交门槛、新请求对应的新 context、多数派确认、应用位置屏障及超时／任期变化取消。日志读模式保留作对照。
- 原有 100 × 1000 步逻辑故障模拟之外，增加 100 × 1500 步的读写／分区／重启／快照模拟；持续真实进程故障加入 Leader 暂停与恢复。
- 加强逐 key 历史检查器的输入验证和相邻时间区间顺序，未知写入继续按可选、无完成时间上界处理。
- 增加配置发布、内容 revision、轮询刷新、功能开关与过期健康状态的应用；验证切主、无多数派、恢复和全节点重启。

详细契约：[journal](journal.md)、[ReadIndex](read-index.md)、[配置应用](../examples/README.md)。

## 已通过的回归

普通构建和 ASan/UBSan 构建完整 CTest 各 **120/120** 通过；sanitizer 启用泄漏检测。优化前留存的基线为 103/103。另在工作区磁盘文件系统补跑 journal、快照、ReadIndex 与配置应用相关的 **13/13** 项测试通过。全部 72 个 C++ 文件通过 CI 固定的 clang-format 18 检查。新增 Python 可编译，原有 421 个公开证据文件、348,770 个性能样本和 948 轮旧故障历史也重新校验通过。

新增 journal 测试检查尾部各位置截断、完整记录损坏、旧格式导入、冲突替换与截断、轮转回收、缺失文件，以及追加三边界和检查点四边界 SIGKILL。错误会使实例停止继续提供数据。备份验证新增了检查点指定 journal 的存在性检查。

真实 RPC 验证连续 100 次 GET 不增加任何副本的日志位置、持久化次数或写入字节；混合流水线的 GET 等待此前 PUT 应用；隔离旧 Leader 无法成功读本地值，读等待额度受限。原有 ENOSPC/EIO 注入、快照传输、落后副本追赶、已确认数据恢复和并发历史测试继续通过。

本机沙箱禁止 socket 创建及 LeakSanitizer 进程检查；完整网络／sanitizer 测试在获准的本机执行环境中完成。测试使用自己的临时数据和随机回环端口。

## 性能与故障证据

同一二进制，image/log、journal/log、journal/ReadIndex 三种模式，四种负载，每种交错重复三轮、每轮测量 10 秒。**36 轮、1,265,078 次计时操作全部通过请求状态与 GET 值核对**，原始样本计数、百分位和逐秒分桶交叉检查通过。预填充和每客户端 20 次预热 GET 不计入测量。

环境为本机 4 个可用逻辑 CPU、约 8 GiB 内存、GCC 15.2、RelWithDebInfo；节点数据位于工作区磁盘文件系统，普通测试的 `/tmp` 是 tmpfs，因此另外补跑了工作区磁盘上的恢复测试。三个节点、代理及最多四个发压进程共享 CPU 和磁盘；后台服务未隔离，未固定 CPU 频率或磁盘缓存。

下面是三个独立运行值的中位数，P99 不是合并样本的 P99；所有场景都是 8 客户端、同连接一次一个在途请求：

| 场景 | 原 image/log ops/s | journal/ReadIndex ops/s | 吞吐比 | 原 P99 ms | 新 P99 ms | 三节点写入字节/操作：原 → 新 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 纯读，128 B、64 key | 1793.6 | 15730.1 | 8.77× | 6.964 | 1.285 | 11259.8 → 0 |
| 50% PUT，128 B、64 key | 1710.8 | 3408.9 | 1.99× | 7.263 | 5.513 | 15437.1 → 422.0 |
| 50% PUT，8192 B、64 key | 253.9 | 2176.1 | 8.57× | 56.193 | 12.128 | 754220.8 → 18799.9 |
| 50% PUT，128 B、1024 key | 838.1 | 3260.3 | 3.89× | 14.825 | 8.089 | 147145.4 → 2121.0 |

### 成本定位

新指标分别记录编码+CRC、write、同步+检查点发布耗时。耗时和字节都累加三个副本，属于应用观测，不能解释为块设备物理写入量或函数 CPU profiler 结果。

大 value 的 image/log 基线每操作累计编码+CRC 约 **6282.6 µs**，约占观测到的持久化总耗时 **76.2%**；更多 key 场景约占 **52.6%**。这直接支持全量编码／校验和重写是主要成本之一。改为 journal/ReadIndex 后，大 value 每操作编码+CRC 约 **176.6 µs**，写入字节下降约 **97.5%**；该场景服务端平均 CPU 从约 1.77 核降至 0.95 核，同时吞吐提高。

小 value 均衡场景更受同步成本影响：原模式每操作累计同步+发布约 927.4 µs，journal/log 约 408.6 µs。delta 不再每次创建和原子替换完整文件，只在检查点时执行目录同步，降低了这部分成本。

### ReadIndex 的取舍

独立对照显示：纯读从 journal/log 的 3816.4 ops/s 提高到 15730.1 ops/s，且持久化字节与日志增长均为零。大 value 场景进一步减少读日志和快照工作。

但在 50% PUT 的小 value 均衡负载，journal/log 为 **3693.8 ops/s、P99 4.385 ms**，journal/ReadIndex 为 **3408.9 ops/s、P99 5.513 ms**：吞吐约下降 **7.7%**。ReadIndex 新增独立的多数派轮次与应用等待，旧日志读则可与写入共用批处理，因此当前实现不保证每种混合负载都更快。更多 key 场景也有小幅吞吐下降。保留 `--read-mode log` 供场景选择；下一步可以进一步研究读确认合批与复制调度。

[完整三模式汇总](../benchmarks/published/local-optimization-20261021/performance/summary.md)、[命令、原始样本与清单](../benchmarks/published/local-optimization-20261021/performance/manifest.json)保留所有轮次。原始工具为 `benchmarks/local_optimization.py`。这些闭环负载没有证明服务端饱和、线性扩展或跨机器吞吐，短时结果也不能替代多日运行。

### 持续故障活动

默认 journal/ReadIndex 运行 **1201.50 秒、1143 轮**，共 **61,722 次尝试、61,113 次模型操作**，全部通过检查。**899 次结果未知的写入**以可选、无完成时间上界保留，没有把超时等同于未写入。其余未纳入模型的请求是明确拒绝或未知读。

| 故障 | 轮数 |
| --- | ---: |
| Leader SIGKILL／重启 | 191 |
| Follower 隔离／恢复 | 191 |
| 有向链路隔离／恢复 | 191 |
| 链路延迟／恢复 | 190 |
| 全节点强杀／重启 | 190 |
| Leader SIGSTOP／SIGCONT | 190 |

每轮三个客户端各做 18 次 PUT/GET/DEL，使用六个新 key，每 key 最多九次操作；完整原始尝试、模型输入、故障时刻及结果保留。结束后再全节点强杀、重启，并检查预先确认的数据。逻辑模拟可按种子重放；真实进程活动的种子只固定场景和操作选择，OS 调度仍不确定。

公开核验会从原始尝试重新构造模型输入，检查未知写入处理规则、每轮计数，并重新执行有界逐 key 校验。[故障汇总](../benchmarks/published/local-optimization-20261021/faults/summary.json)与同目录的 `epochs.jsonl.gz` 保留完整记录，工具为 `scripts/cluster-soak.py`。

普通／sanitizer 各 120 项及磁盘补充 13 项测试、持续故障与性能实验共同为本轮保持一致性和恢复语义提供证据；它们不能证明所有故障、负载或无限运行下都正确。

[公开证据与核验说明](../benchmarks/published/local-optimization-20261021/README.md)包括测试日志、源码归档和二进制哈希：

```bash
python3 scripts/verify-local-optimization.py benchmarks/published/local-optimization-20261021
```

## 复现

```bash
cmake --build build -j2
ctest --test-dir build --output-on-failure -j2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
  ctest --test-dir build-asan --output-on-failure -j2

# 同一二进制，三种模式，四种负载，每种三轮、10 秒；不要并行运行其他压力活动。
python3 benchmarks/local_optimization.py --node build/bin/yewukv-node \
  --output benchmarks/results/local-optimization-new --duration-seconds 10 --repeats 3

# 带六类真实进程／网络故障的有界历史活动，保留每轮输入与结果。
python3 scripts/cluster-soak.py --node build/bin/yewukv-node --client build/bin/yewukv-client \
  --output benchmarks/results/journal-soak-new --duration-seconds 1200
```

手动 CI 工作流 `extended-local-reliability` 支持 60–3600 秒的故障活动并保存历史。它不会定时运行；本轮没有触发远端 CI。

## 尚未覆盖的范围

固定三个 voter、单 Raft Group、回环部署、16 MiB 活跃编码快照和 64 MiB 逻辑状态上限保留。检查点仍全量编码，接近活跃容量上限时的延迟需要另行测量。真实断电、跨主机、多日运行、在线成员变更、认证和跨 key 事务未在本轮验证。有限测试和有界穷举不能替代形式化证明。

v3 journal 目录不能直接交给旧程序或 image 模式打开；升级应整组停机、备份后统一切换，回退使用升级前的整组备份。
