# 2026-10-24 三机工程证据

[报告与图表](../../../docs/engineering-next-20261024.md)解释本轮故障活动、后台快照、负载边界与 CAS 升级。数据共 48 轮负载、684,964 次已发计时请求；其中 42 轮正式对照、637,823 次，另六轮为探索数据。故障流量、结果未知写入、未发出的计划请求、过载拒绝各自计数。

## 离线核验

在仓库根目录使用 Python 3.10+，只读证据，无云端权限要求：

```bash
python3 scripts/verify-engineering.py benchmarks/published/engineering-20261024 > /tmp/engineering.json
python3 tests/test_engineering_evidence.py
python3 tests/test_cloud_load_accounting.py
```

核验器使用当前仓库的样本汇总器和有界历史检查器，不导入／执行此目录内的历史脚本。检查文件清单、两份源码包、程序／环境身份、完整矩阵、请求与计划时刻、成功写入对应日志增长、Leader／任期、恢复结果、故障历史、CAS 探测、原服务保护及清理。清单用于一致性和变更检测，不是外部可信签名。

可选图表重绘需要 matplotlib：

```bash
python3 scripts/plot-engineering.py /tmp/engineering.json --output /tmp/engineering
```

`derived/runs.csv` 与 `derived/summary.json` 为便于浏览的派生数据；逐请求记录和上述核验器是重算入口。

## 内容与口径

| 路径 | 内容 |
| --- | --- |
| `results/faults/` | 18 个跨机故障 epoch、全部尝试与模型历史、少数派读、持久值、快照追赶及重启 |
| `results/snapshot-fixed/` | 正式 sync/background 六轮；Leader 2，客户端在 Follower 3 |
| `results/capacity-fixed/` | 27 轮闭环＋9 轮有界计划到达；Leader 2／客户端 3 |
| `results/snapshot/` | 六轮探索，Leader 与客户端都在 3；不计入正式效果 |
| `results/probes/` | 独立旧目录升级、并发 CAS、ABA、重启及 1,024 个准入响应 |
| `results/executed-load-tools/`、`pilot-tools/` | 正式／探索阶段实际执行工具字节，对应 dispatch SHA |
| `results/tools/`、`nodes/` | 后续工具版本与三节点实验日志 |
| `control/` | 构建／分发、环境、原服务数据保护、清理和下载恢复记录 |
| `source/` | 冻结运行时源码、最终代码／测试／工具源码及逐文件清单 |
| `tests/` | 普通／ASan 143 项、组件 TSan 13 项、相关复核、初次构建失败与重试 |

每轮 `workload.json` 给出配置；`before.json`／`after.json` 给出三节点状态；`result.json` 给出计时边界、各客户端元数据和汇总。`client-N.json.gz` 是逐请求数值样本，格式由当前 `benchmarks/cloud_load.py` 的汇总器解释；`resources.json.gz` 保留进程资源采样，`restart-verification.json` 是快照轮整组重启后的检查。

全部完成负载的已发请求成功不等于扛住全部计划到达：9 轮计划到达共 **148,257 个 missed slots**，没有给它们填写有限延迟。已发请求的 P99 不包含这些未发请求。故障活动另外保留 48 个结果未知写入，模型允许其生效或不生效，客户端不盲目重放。检查为有限单 key PUT／GET／DELETE 模型，不包含 CAS 的通用线性一致性证明。

只有三台 2 vCPU ECS，发压与 Follower 共置，原服务仍运行；不能把吞吐表作为独立服务端生产容量。快照对照强制频繁生成约 8 MiB 快照，默认 `sync` 未改；后台编码仍保留同步磁盘发布及额外所有权／线程开销。百分位为三轮各自百分位的中位数。

## 在独立实验目录复跑

本节供三台 Linux/systemd 实验机操作者复现。所有新实验使用新 tag；保留既有服务／数据目录。节点端口固定 19101，私网控制端口 19190，故障代理使用回环 19201–19203；先确认端口空闲、三台私网互通。代理只改变本实验副本连接，不修改宿主机防火墙。

1. 按[构建说明](../../../README.md#构建与测试)编译节点，记录编译器、选项、源码清单和二进制哈希，同一程序分发到三台。复刻本次 C++ 实现时，在新目录解开 `source/runtime-source.tar.gz` 再构建；最终工具／测试另在 `source/tested-source.tar.gz`。不同工具链未必产生相同程序哈希。
2. 每台存在 `yewukv` 服务用户。选择新的 `TAG`，例如 `yewukv-next-my-run`；建立 `/root/TAG` 和 `/usr/local/libexec/TAG`，将程序放为后者的 `yewukv-node`，将当前仓库 `scripts/peer-proxy.py` 放在同目录，将 `scripts/cloud-node-agent.py` 放入 `/root/TAG`。代理和节点都应可被相应执行用户读取。`TAG` 需以 `yewukv-next-` 开头。
3. 用本地随机生成的临时 token 创建三份 0600 的 agent JSON，仅私网传送；每台将其存为 `/root/TAG/agent.json`。字段如下，替换三个地址、tag、root 与本机 node。`root` 必须是实际 `/root/TAG`，不可使用归档中的脱敏路径。

```json
{
  "tag": "yewukv-next-my-run",
  "root": "/root/yewukv-next-my-run",
  "node": 1,
  "hosts": ["192.0.2.1", "192.0.2.2", "192.0.2.3"],
  "token": "REPLACE_WITH_NEW_RANDOM_PRIVATE_TOKEN"
}
```

4. 每台 root 启动自己的临时代理控制服务，下面替换 `TAG`／`N`。控制 API 只能操作该 tag 的节点、代理、数据路径；没有通用命令执行接口，但仍应仅限实验机私网与可信操作者。控制服务退出会停止自己的节点与故障代理。

```bash
systemd-run --unit TAG-agent-N --collect --property=RuntimeMaxSec=7200 \
  --property=UMask=0077 /usr/bin/python3 /root/TAG/cloud-node-agent.py \
  --config /root/TAG/agent.json
```

5. 节点 3 保留完整仓库工具布局（`benchmarks/` 和 `tests/linearizability.py`），在仓库根目录按顺序运行。下面 config 使用节点 3 的私有 agent JSON，包含 Fleet 所需相同 hosts／token；每个输出目录必须不存在，不能同时运行多个活动。输出目录名称还会构成最多 64 字符的实验 case 名。

```bash
python3 benchmarks/cloud_campaign.py --config /root/TAG/agent.json \
  --output /root/TAG/faults --repeats 3
python3 benchmarks/cloud_load.py --config /root/TAG/agent.json \
  --output /root/TAG/snapshot-fixed --group snapshot --repeats 3
python3 benchmarks/cloud_load.py --config /root/TAG/agent.json \
  --output /root/TAG/capacity-fixed --group capacity --repeats 3
```

worker 会记录失败并停止自己的节点；保留失败目录，调整后改用新输出目录／tag。正式对照会在计时前选择 Leader 2。普通发压、预装、预热、故障活动与各轮重启检查不可混作同一个延迟窗口。

6. `benchmarks/cloud_probes.py --config ... --output ...` 还会在独立 `legacy-upgrade` case 上运行旧程序，**要求** `/usr/local/bin/yewukv-node` 恰为本归档记录的 `6bb46d…` 旧二进制；agent 会核对完整哈希，并拒绝旧程序打开已升级 case。其他机器没有该旧程序时，不能声称复现了这个升级探测；可以用 [CAS 的真实三节点回归](../../../tests/test_cas_snapshot.py)验证当前程序，但应单独记录范围。
7. 结束后逐台停止自己准确的 `TAG-agent-N.service`，确认该 tag 的 node/proxy unit 已停止；采集实验日志、环境、程序与源码哈希，校验全部结果后再归档。不要发布 token JSON、私钥、节点数据或既有业务备份。新机器的真实数据与数值结论须使用新实验目录和自己的清单，不可覆盖本目录。

本次实际编译命令见 `control/build-result.json`，运行参数可从三机日志与 `scripts/cloud-node-agent.py` 复核。历史工具字节不等于已审阅的当前安全工具；离线核验始终使用当前仓库脚本。

## 失败、脱敏与保留

初次受限 ASan 构建遇到 LeakSanitizer 的 ptrace 环境错误，`tests/build-asan-first.log.gz` 与 `tests/build-asan-retry.log` 同时保留；后续 143 项在允许泄漏检查的环境全部通过。收尾下载之后曾因日志字典重复 `node` 字段抛错，见 `control/export-first.log`；独立重新核对远端包与包内清单的记录见 `control/download-recovery.json`，没有替换实验轮次。两个含原始行尾空白的构建日志使用 gzip 保存，解压后保留内容。

诊断 JSON／日志中的节点地址一致映射为文档保留地址 `192.0.2.1–3`，实验私有前缀替换为 `/private-experiment`／`/private-data`。源码和测试日志保留实际代码／日志内容及独立清单；它们可能出现无凭据的构建路径。原始运行程序、节点目录、业务备份和访问材料未包含。旧的既有公开归档保持原样，本目录为新增证据。
