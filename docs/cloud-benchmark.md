# 现有三台 ECS 的跨机压测

工具是 [`benchmarks/remote_matrix.py`](../benchmarks/remote_matrix.py)，只连接已经运行的三个节点，不启动本地集群，不替换服务器程序，不停止或重启节点。先完成[基础正确性演练](cloud-acceptance-20261023.md)，再单独运行性能实验。按本文执行的首组 45 轮结果见[2026-10-24 性能报告](cloud-performance-20261024.md)，原始样本与三台环境已有[公开归档](../benchmarks/published/cloud-performance-20261023/README.md)。

## 测量范围

| 项目 | 第一轮参数 |
| --- | --- |
| 并发连接 | 1、4、8、16、32，每条连接同时只有一个请求 |
| 负载 | 纯 GET、50% PUT / 50% GET、90% PUT / 10% GET |
| key / value | 每个客户端 8 个独立 key，value 固定 128 字节 |
| 发压进程 | 2 个 Python 进程，在各自进程内分配连接线程 |
| 时间 | 每条连接预热 5 秒，每轮计时 30 秒 |
| 重复 | 每种组合 3 次，固定种子打乱每个重复内的组合顺序 |
| 统计 | 成功操作每秒、P50/P95/P99/P99.9/最大延迟，分别保留 GET 和 PUT |

共 45 轮，计时部分 22.5 分钟，加预热至少约 26 分钟，另有预填、校验和调度开销。预热和正式阶段都按固定种子的伪随机请求选择比例执行，结果记录实际 PUT / GET 数量，不假定恰好等于目标比例。

三台 ECS 都运行服务。在其中一个 **Follower** 上发压，建议固定用第三台，并记录它的节点 ID。客户端到 Leader 使用私网地址，Leader 再向其他机器复制。工具先确认本机确实拥有相应节点的地址，并拒绝从 Leader 所在节点发压；不会为了这个检查主动停止 Leader。若检查提示当前第三台是 Leader，先查看状态，再选择另一个 Follower 并调整 `--client-node-id`。

这会让发压进程与一个 Follower 共用 CPU、内存和网络。结果应称为“现有三台 ECS、客户端与 Follower 共置条件下的跨机闭环性能”；不能据此声称已经测出服务独立部署时的极限容量。后续有独立发压机时可设 `--client-node-id 0`，另行记录其环境与网络位置。

## 准备与版本记录

三台节点继续使用已经验收的同一份运行程序。只需在发压机更新工具，无需重新构建或运行安装器：

```bash
cd /root/yewukv
git pull --ff-only
```

`git pull` 更新的是工作目录，系统服务仍使用 `/usr/local/bin/yewukv-node`。工具保存自己和 RPC 编码文件的副本、SHA-256、工具所在 Git 提交、发压机配置、发压机本地节点的程序 SHA-256、配置、服务 PID 和启动 ID，避免混淆工具版本与正在运行的节点版本。

正式压测前，在**三台机器分别**执行下面的环境记录；ID 改为本机 1、2、3，保留文件到后续归档：

```bash
NODE_ID=1
{
  date -Is
  uname -a
  cat /etc/os-release
  lscpu
  free -h
  sha256sum /usr/local/bin/yewukv-node /usr/local/bin/yewukv-client
  cat "/etc/yewukv/node-${NODE_ID}.env"
  systemctl show "yewukv-node@${NODE_ID}.service" \
    --property=MainPID,ActiveState,ExecStart,User,FragmentPath
} > "/root/yewukv-benchmark-node${NODE_ID}-$(date -u +%Y%m%dT%H%M%SZ).txt"
```

核对三个节点二进制 SHA-256 一致，并与被测运行版本的已存档哈希对应。不要在计时阶段编译、跑 CTest、跑故障演练或同时启动另一个发压器。不要把 9101 放到公网，沿用现有三台私网来源规则。

## 先试跑

下面地址都是示例，替换成实际私网地址，按节点 ID 1、2、3 排列。后续命令在**同一台 Follower 发压机**执行。

```bash
cd /root/yewukv
NODES='10.0.0.11:9101,10.0.0.12:9101,10.0.0.13:9101'
python3 benchmarks/remote_matrix.py --nodes "$NODES" --client-node-id 3 \
  --concurrency 1,4 --profiles read-only,balanced \
  --duration-seconds 5 --warmup-seconds 2 --repeats 1 \
  --output "/root/yewukv-bench-smoke-$(date -u +%Y%m%dT%H%M%SZ)"
```

这会完成 4 个短轮次，每轮输出吞吐量、P95/P99 和失败数。短轮次仅验证工具与网络，不作为最终 P99 报告。输出目录每次必须是新路径；不覆盖旧结果。只想先读取状态时增加 `--preflight`，它不会发送 PUT。

若请求失败、读取值不符、节点不收敛、任期或 Leader 变化，实验会失败并保留已经写出的记录。超时 PUT 可能已提交，工具不会重试该请求。保留失败目录和日志，先分析原因，不把它删除后只报告成功轮次。不同机器上的发压器需要人工协调，工具的同机锁只能阻止同一台机器的重叠运行。

## 正式运行并防止终端断线影响

试跑全部成功后使用系统临时服务执行，发压任务独立于 Workbench 会话：

```bash
cd /root/yewukv
NODES='10.0.0.11:9101,10.0.0.12:9101,10.0.0.13:9101'
RUN_NAME="yewukv-cross-bench-$(date -u +%Y%m%dT%H%M%SZ)"
BENCH_OUTPUT="/root/$RUN_NAME"
printf '%s\n' "$BENCH_OUTPUT" > /root/yewukv-cross-bench-last-path.txt
systemd-run --unit "$RUN_NAME" --collect \
  --property=UMask=0077 \
  --property="StandardOutput=append:${BENCH_OUTPUT}.log" \
  --property="StandardError=append:${BENCH_OUTPUT}.log" \
  /usr/bin/python3 /root/yewukv/benchmarks/remote_matrix.py \
  --nodes "$NODES" --client-node-id 3 \
  --concurrency 1,4,8,16,32 --profiles read-only,balanced,write-heavy \
  --duration-seconds 30 --warmup-seconds 5 --repeats 3 --workers 2 \
  --output "$BENCH_OUTPUT"
```

重连后，在发压机读取进度：

```bash
BENCH_OUTPUT=$(cat /root/yewukv-cross-bench-last-path.txt)
tail -n 12 "${BENCH_OUTPUT}.log"
python3 -c 'import json,sys; m=json.load(open(sys.argv[1])); print(m["status"], len(m["runs"]), "/", len(m.get("planned_runs", [])))' \
  "$BENCH_OUTPUT/manifest.json"
```

显示 `complete 45 / 45` 才表示整组完成。若 `failed`，查看清单中的 `error`、日志及失败子目录。临时服务结束后可能已被 systemd 回收，结果是否完成以保存的 `manifest.json` 为准。若主动中止，停止的是命名为 `yewukv-cross-bench-...` 的发压任务，不能误停 `yewukv-node@...` 节点服务。

## 核验、导出与报告

```bash
BENCH_OUTPUT=$(cat /root/yewukv-cross-bench-last-path.txt)
cd /root/yewukv
python3 benchmarks/remote_matrix.py --verify "$BENCH_OUTPUT"
cat "$BENCH_OUTPUT/summary.md"
tar -czf "${BENCH_OUTPUT}.tar.gz" -C /root \
  "$(basename "$BENCH_OUTPUT")" "$(basename "${BENCH_OUTPUT}.log")"
sha256sum "${BENCH_OUTPUT}.tar.gz"
```

下载发压机的结果压缩包和三台的环境记录。压缩包含实际私网地址及配置，公开前另做脱敏副本，保留私有原件。离线核验会重算样本数、成功 PUT/GET 数、吞吐量与延迟分位数，并检查 45 轮是否按计划完成以及只读阶段没有写入日志；不是对远端机器身份的证明。

每个轮次保留工作负载、准备前/预热后/结束后的三节点状态、逐客户端原始延迟与完成时间/操作种类/成功标志、最终值检查结果、发压进程 CPU/RSS 和发压机 CPU tick（含 steal）采样。客户端资源采样覆盖计时及收尾校验，进程 CPU 总量还含准备阶段，不能直接当作纯计时 CPU。其他两个节点的逐秒 CPU/RSS 未由此工具采集；分析瓶颈时应另加监测，不能仅凭客户端 CPU 推断服务端已经饱和。

时间使用发压机单调时钟。吞吐量 = 成功的计时请求数 / 从所有连接释放前到最后一个请求完成的实际耗时；包含结束时在途请求的完成时间，排除建连、预填、预热和最终 GET 校验。延迟为客户端编码、网络、服务处理、解码和校验的总时间。闭环客户端在慢响应期间会减少发出请求，结果不能代表固定外部到达率下的排队延迟。

每个重复单独计算最近秩分位数，汇总表展示三次吞吐量中位数及范围、三次 P95/P99 的中位数；不会把平均 P99 称为合并后的 P99。原始 JSON 另保留 GET/PUT 的 P99.9 和最大值。P99/P99.9 对样本量敏感，引用时同时报告操作数、时长与三次波动。

压测使用唯一的 `cloudbench-...` 前缀，与四个验收 key 分开；最多预填 `最大并发 × 每客户端 key 数` 个 key，所有轮次复用这些 key，开头一次预填完整 keyspace，保持数据规模固定。默认约 32 KiB value 数据，工具保守限制新 keyspace 在 4 MiB 内。既有数据、日志、历史检查点继续保留，工具不清空状态，所以这是已有集群上的热数据实验，不是新库冷启动或大数据集容量实验。对已经存在的四个验收值，结束时再次核对。
