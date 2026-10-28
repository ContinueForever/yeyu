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

最新云端结果可离线核验，无需启动服务器：

```bash
python3 scripts/verify-cloud-async-disk.py benchmarks/published/cloud-async-disk-20261026
python3 tests/test_async_disk_evidence.py
sha256sum --check benchmarks/published/cloud-async-disk-20261026/SHA256SUMS
python3 scripts/verify-peer-disk.py benchmarks/published/peer-disk-20261026
python3 tests/test_peer_disk_evidence.py
sha256sum --check benchmarks/published/peer-disk-20261026/SHA256SUMS
python3 scripts/verify-background-soak.py benchmarks/published/background-soak-20261026
python3 tests/test_background_soak_evidence.py
sha256sum --check benchmarks/published/background-soak-20261026/SHA256SUMS
```

运行的是当前仓库核验器，不执行归档源码或工具。客户 gzip 保留原字节，身份脱敏映射与前后哈希单列；数值、日历、分位数、资源、任期、队列守恒、恢复和清理按报告范围复算。哈希检查不能替代语义检查：五项首轮对照负例、七项新旧对照负例、七项长实验负例会在副本中改数据并刷新文件清单，核验仍必须拒绝。

所有历史公开实验的核验命令保留在各归档 README，[文档索引](docs/README.md#实验与验收)提供入口；[CI 配置](.github/workflows/ci.yml)列出完整执行集合。不要直接覆盖已发布样本或其清单。新增实验使用新目录，按[实验复现说明](benchmarks/README.md)记录实际结果，并在文档索引添加入口。旧报告对应当时源码和默认策略，测试数量、故障操作与性能样本不能跨版本相加。

交付基线普通／ASan＋UBSan 各 189 项 CTest 通过。部分 CTest 入口包含多个 Python 子场景；离线证据负例不叠加为 CTest 数。TSan 31 项只覆盖有序队列与后台磁盘组件，不包含完整网络服务。强杀、慢盘和 I/O 错误使用测试专用注入库；常规服务不加载它。首次失败、修正和诊断限制按各阶段报告保留，不覆盖旧日志。

当前只修改文档时，核对链接、命令参数、默认值、指标口径和历史迁移内容即可；已有 CI 仍执行完整检查。修改实现时按本页前述要求运行受影响验证与完整 CTest。求职描述与源码／证据对应关系见[简历与面试说明](docs/resume.md)。

## GitHub 检查

`ci` 在推送与 PR 更新时运行普通构建、ASan／UBSan、格式检查、公开证据核验和单节点 Docker Compose 持久化恢复。`extended-local-reliability` 可手动运行 60–3600 秒故障活动，并保留报告与重现记录。单次检查通过不代表跨机器、真实断电或多日稳定性已完成。
