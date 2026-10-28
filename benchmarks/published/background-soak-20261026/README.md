# 可选后台磁盘六小时稳定性：2026-10-26

六阶段共360个一分钟计时窗口。所有请求、精确计划日历、未发时刻、状态、实际启动参数、资源帧和阶段恢复记录均保留。计时窗口不含预载、预热和阶段重启，墙钟单列；每窗口的分位数只能作为相关窗口统计量，不能标作合并分位数。

这是可选后台路径的稳定性验收。此前18轮对照的大状态P99.9为旧值51.82%，未达到预设减半门槛，performance_gate仍为false，默认继续sync。目标变更、初始选择及其哈希见control。实际程序和云端工具冻结834bb43，4193bb0只修正旧测试；构建与验证CI分别记录。

从仓库根目录执行当前核验代码，不能运行归档工具来自证：

```sh
python3 scripts/verify-background-soak.py benchmarks/published/background-soak-20261026
python3 tests/test_background_soak_evidence.py
sha256sum --check benchmarks/published/background-soak-20261026/SHA256SUMS
```

每组results/*.partNN按清单顺序拼接为一条tar.gz流。只分割外层归档，每份客户gzip保持原字节；映射前后SHA-256和长度见control/export-map.json。解包严格限制路径、类型、单文件及总字节数。source仅作逐文件核对。

三台现有ECS，发压共置Follower3，各实验节点与发压服务限一个CPU/512MiB；不宣称独立容量。资源采样包含预载、预热与排空，RSS是采样最大值。六次阶段结束整组重启核对冷键、热键、锚值及revision。冻结工具实际读取并验证全部值，但恢复记录只保存计数、revision和状态；无法离线重建每个恢复读到的字节，不将其描述为逐值哈希证据。

原服务512个旧实验键和四个保护值前后核对，只公开集合哈希；PID、程序哈希与亲和性核对，实验服务全部停止。诊断身份脱敏，保留数值，不公开私钥、token、控制配置、业务值、原数据库或可执行程序。不包含真实断电、多日运行或其他负载承诺。

完整结果、失败/未发、尾延迟和解释边界见[报告](../../../docs/background-soak-20261026.md)。
