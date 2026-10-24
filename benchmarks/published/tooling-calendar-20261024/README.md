# 固定速率日历边界修正

初次云端烟测在 32 连接、600 次／秒、2 秒窗口中，每轮排出了 1,201 个时刻，多出的末尾时刻被计为未发。两轮均记录 1,200 个成功请求和一个未发；这是发压器周期取整误差。原始记录保留在 initial-smoke，不作为正式长窗口结果。诊断 JSON 地址与路径已脱敏，逐请求数值保持不变。

本补丁改用全局序号的整数有理计算，样本版本升为 2；旧版核验继续用于保留的烟测。新增 32 连接、600 次／秒回归，要求 400 ms 精确产生 240 个计划时刻，仍单独记录真实未发。普通／ASan 的 C++ 发压器专项各通过一个 CTest，内部包含真实三节点、错值、上限与中断测试。148 项全量回归对应前一提交 c4c02f1，本补丁不冒充另一次完整回归。

从仓库根目录核验初始烟测：

```bash
python3 scripts/verify-cloud-soak.py benchmarks/published/tooling-calendar-20261024/initial-smoke
```

源码修改属于加入本目录的 Git 提交，详见 app/yewukv_bench.cc、benchmarks/cpp_load.py、tests/test_cpp_bench.py 和 docs/long-window.md。修正后的云端烟测与长窗口使用全新目录保留，不覆盖此处的旧记录。publication-manifest.json 为本目录逐文件 SHA-256 清单。
