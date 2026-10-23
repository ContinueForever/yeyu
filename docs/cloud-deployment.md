# 三台云服务器的私网部署

当前节点支持 `--listen-host` 指定本机的一个 IPv4 地址，省略时仍只监听 `127.0.0.1`。成员列表是其他节点和客户端实际连接的地址，不能使用 `0.0.0.0`。命令行拒绝通配、组播、广播和非 IPv4 监听地址。

IPv4 回归覆盖同机不同地址、相同端口，以及本机非回环网卡上的复制、故障接管和持久恢复。实际三台 ECS 已按本文执行基础正确性手工演练，结果见 [2026-10-23 记录](cloud-acceptance-20261023.md)；已核对操作者终端截图，原始云端日志尚待归档。

2026-10-23 开发机完整 CTest 普通与 ASan/UBSan 构建各 126 项通过，后者启用泄漏检查。新增的 `cluster_ipv4_deployment` 组内 5 个用例全部通过，非回环网卡用例未跳过。系统服务模板通过 `systemd-analyze verify` 语法检查；三台 ECS 的普通构建各 126 项通过，并观察到系统服务安装、第三台正常重启后的开机自启、两次服务停止接管、少数派保护及全停恢复。云上 SIGKILL、网络分区、持续负载和物理断电不在本次手工结果内。

## 准备三台服务器

每台各运行一个节点。已有一台 Ubuntu 24.04、2 vCPU / 4 GiB 的 ECS 时，再准备两台相同配置即可开始小规模正确性实验。三台选择同一地域、同一 VPC；使用同一交换机便于初次配置，记录各自的**私网 IPv4**。试用抵扣、带宽和磁盘费用以账户当前的试用范围与账单为准。

下文的 `10.0.0.11`、`10.0.0.12`、`10.0.0.13` 都是示例，运行前替换为实际私网地址：

| 节点 ID | 示例私网 IP | RPC 端口 | 服务 |
| --- | --- | --- | --- |
| 1 | 10.0.0.11 | 9101 | yewukv-node@1.service |
| 2 | 10.0.0.12 | 9101 | yewukv-node@2.service |
| 3 | 10.0.0.13 | 9101 | yewukv-node@3.service |

在安全组中允许三台实例的私网地址访问 TCP 9101，例如分别使用三个 `/32` 来源规则；主机启用防火墙时也需允许。同一普通安全组通常默认内网互通，具体规则与隔离设置参见阿里云的[内网互通说明](https://help.aliyun.com/zh/ecs/user-guide/internal-network)。服务没有 TLS 和身份认证，端口应限于这三个受信任的私网来源。SSH 登录沿用已有配置，私钥保存在自己的电脑上。

## 构建同一版本

三台服务器分别执行以下步骤。已经克隆过的第一台在源码目录执行 `git pull --ff-only`，其余两台先克隆；不要重复克隆到已有目录。

```bash
sudo apt-get update
sudo apt-get install -y git g++ cmake ninja-build protobuf-compiler libprotobuf-dev libgtest-dev python3 ca-certificates
git clone --depth 1 --single-branch --branch codex/remote-ci https://github.com/ContinueForever/yeyu.git yewukv
cd yewukv
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j2
ctest --test-dir build --output-on-failure -j2
git rev-parse HEAD
ip -4 address show scope global
```

核对三台的 Git 提交相同，并确认成员列表中分给本机的地址确实显示在本机网卡上。先前试验若使用 `yewukv-cloud-node-{1,2,3}.service` 三个临时服务，在第一台停止它们：

```bash
sudo systemctl stop yewukv-cloud-node-1.service yewukv-cloud-node-2.service yewukv-cloud-node-3.service
```

旧单机目录 `/var/lib/yewukv-cloud/node-*` 保留。新集群使用 `/var/lib/yewukv/node-*`，从空数据开始。**已有目录保存了不可变的成员地址，不能把单机目录直接改成私网成员后继续用。**

## 安装一个节点到每台机器

三台使用完全相同的 `MEMBERS`，只改变 `--id`。以下为节点 1 的命令；第二、三台分别将 ID 改为 2、3。

```bash
MEMBERS='1@10.0.0.11:9101,2@10.0.0.12:9101,3@10.0.0.13:9101'
python3 scripts/install-cloud-node.py --id 1 --members "$MEMBERS" --check
sudo python3 scripts/install-cloud-node.py --id 1 --members "$MEMBERS"
sudo systemctl status yewukv-node@1.service --no-pager
sudo journalctl -u yewukv-node@1.service -n 30 --no-pager
```

`--check` 只检查并展示配置，不修改机器。正式安装还会确认本机拥有该私网地址、已有数据的身份匹配、没有正在运行的 `yewukv-node@` 服务。安装器限用三个不同的 RFC 1918 私网地址；节点 CLI 自身也可显式绑定其他单播 IPv4。

安装器复制节点和客户端到 `/usr/local/bin/`，创建无登录权限的 `yewukv` 用户，在 `/etc/yewukv/node-ID.env` 保存配置，并安装[系统级服务模板](../deploy/systemd/yewukv-node@.service)。节点使用独立用户运行，异常退出后自动重启，随机器开机启动，不依赖浏览器终端。只有一台启动时，服务可以运行，但读写要等至少两个节点组成多数派。

## 验证读写与故障恢复

三台都安装后，在其中任意一台执行：

```bash
NODES='10.0.0.11:9101,10.0.0.12:9101,10.0.0.13:9101'
yewukv-client --nodes "$NODES" --timeout-ms 10000 ping
yewukv-client --nodes "$NODES" --timeout-ms 10000 put cloud-check ready
yewukv-client --nodes "$NODES" --timeout-ms 10000 get cloud-check
for address in 10.0.0.11 10.0.0.12 10.0.0.13; do
  yewukv-client --host "$address" --port 9101 status
done
```

前三个输出依次为 `PONG`、`OK`、`ready`。状态应有一个 Leader，三个节点逐步追平 `commit_index` 和 `applied_index`。

按以下顺序演练，每一步记录时间、命令、状态和读写输出：

1. 在 Leader 所在机器停止对应服务，例如 `sudo systemctl stop yewukv-node@2.service`。从另一个仍运行的节点发起请求，确认原值可读，并成功写入一个新 key。`systemctl stop` 会保持停止，适合观察剩余两台接管。
2. 启动被停止的服务，等待它追赶。再次停止当前 Leader，确认新值仍可读，然后恢复全部节点。
3. 三台都正常时，重启其中一台虚拟机；确认其他两台可服务，被重启机器上的节点自动启动并追赶。重启可能使当前 SSH 会话断开。
4. 停止其中两个服务，仅剩少数派时读写应失败。结果未知的写请求不能当作确定失败后自动重放。恢复全部节点后核对之前成功确认的值。
5. 全部服务停止再启动，确认已经返回成功的值仍可读取。保存每台的节点日志和最终状态，区分进程重启、虚拟机重启与物理断电的验收范围。

## 日常管理与升级

```bash
sudo systemctl stop yewukv-node@1.service
sudo systemctl start yewukv-node@1.service
sudo systemctl restart yewukv-node@1.service
sudo journalctl -u yewukv-node@1.service --since '10 minutes ago' --no-pager
```

把 ID 换成本机 ID。升级前停止三台节点并分别备份各自的整个数据目录，再在每台更新、构建并重新运行安装器；三台版本和成员列表保持一致。安装器不会删除已有数据，也不会转换旧成员地址。现有 `local-cluster.sh backup/restore` 面向同机的另一套用户服务，不能直接拿它备份这三个系统级云节点。

更换私网 IP 属于静态成员配置变化，不能直接复用原目录。本项目没有在线成员变更。实验结束后导出需要的日志和数据，再到云控制台处理资源；仅停止进程不会停止云资源计费。
