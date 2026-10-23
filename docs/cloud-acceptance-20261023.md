# 2026-10-23：三台 ECS 基础正确性手工验收

源码版本 [`909df769ae2d9b0579a31f9d89745e589da86819`](https://github.com/ContinueForever/yeyu/commit/909df769ae2d9b0579a31f9d89745e589da86819) 在三台 ECS 上各运行一个节点，完成基础读写、两次 Leader 服务停止后的接管与追平、一个 Follower 虚拟机正常重启、少数派保护，以及全部服务正常停止后的恢复。四个已确认的测试值均在最终检查中读回，三个节点的日志、提交与应用位置最终均为 11。

**证据状态：本文根据操作者执行命令后提供的终端截图逐项记录。云端原始日志、环境信息与二进制哈希尚未导入仓库，公开的独立校验归档尚未完成。** 本文没有把截图转写成原始日志，也没有生成不存在的请求历史或样本。后续应以导出的原始记录补齐归档；它与此前已经发布完整样本的同机实验采用不同的证据状态。

## 环境与执行方式

| 项目 | 本次范围 |
| --- | --- |
| 运行环境 | 阿里云 ECS，华东 1，同一 VPC，三台虚拟机各运行一个节点 |
| 每台配置 | 经济型 e，2 vCPU / 4 GiB 内存，40 GiB ESSD Entry 系统盘；来自控制台截图 |
| 操作系统 | Ubuntu 24.04.5 LTS，x86_64；来自登录终端截图 |
| 版本 | 三台部署输出均为 `909df76`；安装后的实际二进制哈希待导出 |
| 完整测试 | 三台各完成同一组 126 项 CTest，均通过；这是云服务器上的普通构建 |
| 节点配置 | 固定 ID 1、2、3，各绑定本机私网 IPv4 的 TCP 9101，使用一致成员列表 |
| 服务与数据 | 系统级 `yewukv-node@ID.service`，数据位于 `/var/lib/yewukv/node-ID` |
| 策略 | 默认 journal、ReadIndex、自适应快照 |
| 操作方式 | 操作者通过 Workbench 手工执行命令，保存实验输出；没有运行跨机并发负载发生器 |

安装及复现步骤见[三台云服务器部署](cloud-deployment.md)。公开记录以节点 ID 表示身份，实际地址保留在待导出的原始材料中。三台 ECS 不能据此推断位于不同物理宿主机或独立可用区。

当日开发机对同一源码版本的普通与 ASan/UBSan 完整回归各 126 项通过，后者启用泄漏检查；IPv4 测试组 5 个用例通过。本次三台 ECS 的截图只展示普通构建，不把开发机的 sanitizer 结果计作云端 sanitizer 结果。

## 已观察的六个场景

| 场景 | 操作与业务结果 | 状态观测 |
| --- | --- | --- |
| 基础读写 | `ping`、`put cloud-check ready`、`get cloud-check` 返回 `PONG / OK / ready`；终端重连后再读返回 `ready` | Leader 为节点 1；三台 term=80，last/commit/applied=3，keys=1 |
| 第一次接管 | 停止节点 1 服务，只向节点 2、3 发起请求；旧值返回 `ready`，写入 `failover-check=survived` 返回 `OK`，读取返回 `survived` | 节点 3 成为 Leader，term=81；节点 1 恢复后为 Follower，三台进度均为 5，keys=2 |
| 恢复副本再次参与多数派 | 停止当时的 Leader 节点 3；只向节点 1、2 发起请求，读取 `survived`，写入 `rejoin-check=caught-up` 返回 `OK`，读取返回 `caught-up` | 节点 1 成为 Leader，term=82；节点 3 恢复后追平，三台进度均为 7，keys=3 |
| Follower 虚拟机重启 | 正常重启第三台机器；第一台检测到节点 3 暂时不可达后，通过节点 1、2 读取 `caught-up`，写入 `reboot-check=alive` 返回 `OK`，读取返回 `alive` | 第三台前后 boot ID 不同；重连后服务 `enabled / active`，term=82，进度为 8，keys=4 |
| 少数派保护 | 停止节点 2、3 服务，直接向节点 1 执行 GET 与 PUT，分别返回读多数派不可用和写多数派超时；恢复服务后四个已确认值均能读取 | 节点 1 仍自报 Leader；last_index 从 8 到 9，commit/applied 仍为 8，keys=4，quorum_timeouts 从 0 到 2 |
| 全部服务停止后恢复 | 全停检查依次确认三个端点均不可达，再逐台启动；四个已确认值均读回 | 最终节点 2 为 Leader；三台 term=96，last/commit/applied=11，keys=4，pending=0 |

表中的“进度均为 N”表示 `last_index = commit_index = applied_index = N`，对应各场景提供的诊断快照。`status` 是本地诊断接口，业务读取结果来自 GET。

### 少数派读写的含义

少数派实验前、后都探测节点 2、3 不可达，节点 1 的诊断接口始终可访问。读取输出为：

```text
read quorum unavailable or leadership changed
```

写入使用已经存在的 `reboot-check=alive`，输出为：

```text
leadership lost or quorum timeout; operation outcome is unknown
```

本地追加到第 9 条日志与“没有在少数派提交”并不矛盾。该阶段 `commit_index`、`applied_index` 均保持 8，没有返回写入成功。超时写入的最终结果仍未知，恢复多数派后可能提交；本次没有据此把它当作确定取消的操作，也没有通过重复发起请求来判定结果。

### 虚拟机重启的观测范围

第一台观察脚本在 `2026-10-23T21:16:07+08:00` 检测到节点 3 的 RPC 暂时不可达，然后仅向节点 1、2 发起业务请求，得到 `caught-up / OK / alive`。第三台重连后的 boot ID 已改变，操作者按检查步骤直接得到 `enabled / active`，随后诊断显示已应用到第 8 条日志。

观察脚本没有在业务请求结束时再次探测第三台是否仍不可达，也没有持续记录整个重启窗口的请求历史。本次只支持“检测到节点暂时不可达后，剩余两台成功完成这些请求，并观察到被重启节点自动启动及追平”，不能据此声称整个重启期间连续无错误服务或某个恢复时间上界。

## 全停恢复后的最终结果

全停检查在 `2026-10-23T21:37:35+08:00` 生成记录，三个端点均不可达。恢复检查记录生成于 `2026-10-23T21:38:45+08:00`，不把这两个记录时间之差当作恢复耗时。

| 节点 | 角色 | term | leader_id | last_index | commit_index | applied_index | keys | pending |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | follower | 96 | 2 | 11 | 11 | 11 | 4 | 0 |
| 2 | leader | 96 | 2 | 11 | 11 | 11 | 4 | 0 |
| 3 | follower | 96 | 2 | 11 | 11 | 11 | 4 | 0 |

| 已确认的 key | 恢复后的 GET 值 |
| --- | --- |
| `cloud-check` | `ready` |
| `failover-check` | `survived` |
| `rejoin-check` | `caught-up` |
| `reboot-check` | `alive` |

最终三台的 `quorum_timeouts` 均为 0。这些指标在进程重启后重新计数，不代表整个实验从未出现多数派超时。任期从 82 到最终 96，也不能直接解释为发生了 14 次已完成的 Leader 切换。

## 待归档的材料

以下文件名已经从操作者截图中核对，文件仍位于对应服务器 `/root/`，尚未导入仓库：

| 所在节点 | 记录 |
| --- | --- |
| 1 | `yewukv-failover-20261023T130640Z.log` |
| 3 | `yewukv-second-failover-20261023T130953Z.log` |
| 1 | `yewukv-reboot-20261023T131528Z.log` |
| 1 | `yewukv-minority-20261023T132652Z.log` |
| 1 | `yewukv-full-stop-node1-20261023T133657Z.log` |
| 1 | `yewukv-all-stopped-20261023T133735Z.log` |
| 1 | `yewukv-full-recovery-20261023T133845Z.log` |

还需收集每台的本项目服务日志、环境信息、Git 提交、节点与客户端二进制 SHA-256、服务配置和最终状态。收到这些原始文件后，应核对版本与输出、保留原始私有材料，再生成适合公开的归档及校验清单。当前没有把这份手工记录列为可运行独立校验器验证的完整证据包。

## 本次结论的边界

- 服务故障通过 `systemctl stop/start` 产生，虚拟机故障通过正常重启产生；没有在云上执行 SIGKILL、强制断电或受控网络分区。
- 使用四个测试 key 和少量顺序请求，没有跨机吞吐、P99、持续并发负载或并发历史模型校验结果。此前[同机性能报告](local-followup-20261022.md)的数字仍只对应其原环境。
- 没有覆盖多日运行、独立故障域、云端备份恢复、真实云盘耗尽、云上大状态快照追赶或物理断电。历史本机测试中的故障注入与本次手工云实验各自保留原范围。
- 固定三个 voter、单 Raft Group 及当前容量限制保持不变；本轮不是生产可用性或容量验收。
