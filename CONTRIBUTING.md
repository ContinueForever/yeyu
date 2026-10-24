# 开发与提交前验证

## 环境与构建

项目使用 C++17、CMake、Ninja、Protobuf、GTest、Python 3 和 Git，当前支持 Linux。请通过 Git 检出后运行完整测试，故障活动需要 Git 元数据来归档被测源码。Ubuntu 24.04 的依赖安装与手动构建命令见[首页](README.md#构建与测试)。安装依赖后也可使用：

```bash
scripts/build.sh
scripts/test.sh -j2
```

脚本默认使用系统工具链；若存在 `~/.micromamba/envs/yewu`，会使用该历史 Conda 环境。也可用 `YEWU_TOOLCHAIN=/absolute/path/to/conda/env` 显式选择环境。不要把本机工具链、构建产物或依赖包提交到仓库。

## 改动与验证

修改 Raft、持久化、读路径或快照时，先阅读对应[设计契约](docs/README.md#设计与正确性契约)，再运行完整 CTest。涉及磁盘字节格式时保留兼容性及独立校验测试；涉及读调度时保留旧任期、旧回复、失去多数派和应用位置的回归。

Sanitizer 构建与运行：

```bash
cmake -S . -B build-asan -G Ninja -DCMAKE_BUILD_TYPE=Debug -DYEWUKV_ENABLE_SANITIZERS=ON
cmake --build build-asan -j2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
  ctest --test-dir build-asan --output-on-failure -j2
```

集成测试会创建独立临时集群，需要本机 TCP 与进程控制权限。普通 CTest 已包含短故障活动、ReadIndex、配置应用、快照、备份和 I/O 故障测试。性能测量与构建、回归、其他发压活动分开运行，避免互相干扰。

CI 使用 clang-format 18 检查所有已跟踪的 C++ 文件：

```bash
clang-format-18 --dry-run --Werror $(git ls-files '*.cc' '*.h')
git diff --check
```

提交前核对修改涉及的文档、命令行参数和实际默认值一致。测试失败时保留日志与重现输入，修复后重新运行受影响的验证。

## 实验材料与仓库边界

`benchmarks/results/` 用于本机运行目录；新结果默认忽略，其中少数已跟踪目录属于早期公开基线。新公开证据放在 `benchmarks/published/<experiment>/`，保留命令、环境、版本、源码、原始样本、失败记录和校验清单。节点数据目录、可执行文件、业务备份和密钥不属于公开证据。

已发布证据可离线核验，无需启动服务器：

```bash
python3 scripts/verify-evidence.py benchmarks/published/local-20261021
python3 scripts/verify-local-optimization.py benchmarks/published/local-optimization-20261021
python3 scripts/verify-local-followup.py benchmarks/published/local-followup-20261022
python3 scripts/verify-cloud-evidence.py benchmarks/published/cloud-20261023
python3 scripts/verify-cloud-performance.py benchmarks/published/cloud-performance-20261023
python3 scripts/verify-cloud-diagnosis.py benchmarks/published/cloud-diagnosis-20261024
python3 scripts/verify-engineering.py benchmarks/published/engineering-20261024
python3 scripts/verify-tooling.py benchmarks/published/tooling-20261024
```

不要直接覆盖已发布样本或其清单。新增实验使用新目录，按照[实验复现说明](benchmarks/README.md)记录实际结果，并在[文档索引](docs/README.md#实验与验收)添加入口。旧报告对应归档的历史实现，当前构建可能采用不同默认策略。

云端性能归档另有 `python3 tests/test_cloud_performance_evidence.py`：在临时副本中修改 P99、矩阵轮次、应用进度、节点程序哈希和请求成功标志，并刷新两层文件清单，要求语义核验仍拒绝。原始归档保持不变。

新增 `tests/test_cloud_diagnosis_evidence.py` 与 `tests/test_engineering_evidence.py` 检查追踪关联、计划到达时刻及不可能的历史读值；CI 运行两份公开归档核验和语义负例。最新实验条件、普通／ASan 143 项及组件 TSan 的范围见[工程报告](docs/engineering-next-20261024.md)。

## GitHub 检查

`ci` 在推送与 PR 更新时运行普通构建、ASan／UBSan、格式检查、公开证据核验和单节点 Docker Compose 持久化恢复。`extended-local-reliability` 可手动运行 60–3600 秒故障活动，并保留报告与重现记录。单次检查通过不代表跨机器、真实断电或多日稳定性已完成。
