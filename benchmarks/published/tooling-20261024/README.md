# 分阶段指标、CAS 历史与 C++ 发压器验证

[工具与长窗口说明](../../../docs/long-window.md)。本归档是短任务回归证据，不包含尚未完成的六小时云端结果。

从仓库根目录执行：

```bash
python3 scripts/verify-tooling.py benchmarks/published/tooling-20261024
python3 tests/test_revision_history.py
```

普通与 ASan／UBSan 各 148 项通过，分别用时 64.73／80.21 秒，启用泄漏检测。最终代码与测试在 source.tar.gz，逐文件 SHA-256 在 source-manifest.json。测试环境为 Linux／GCC15，本机 C++ 发压器行为回归不代表跨机性能收益。

`histories/normal` 与 `histories/asan` 保留本轮多次回归产生的全部 CAS 故障历史。每次调用运行 kill／pause／partition 三个场景，含丢失回复、ABA、快照与整组恢复；重复执行次数不当作独立云端实验次数。未知修改以 end=null 归档，当前仓库检查器按无穷完成时间读取。

`tests/ctest-final-*.log` 是最终完整回归。`tests/LastTest-*.log.gz` 保留详细测试输出，包括 C++ 发压器的错误读值、样本上限、中断保存及真实三节点数据交叉检查。没有发布节点数据目录、可执行文件、SSH 凭据或云端控制配置。

文件清单检测意外变更；语义核验使用当前仓库的模型，不执行归档代码，也不构成 CAS 的通用证明。源码和日志可能包含无凭据的开发机路径。旧公开归档不作覆盖。
