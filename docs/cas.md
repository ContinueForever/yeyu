# 版本条件更新与配置发布

固定三节点服务新增 CAS（Compare-And-Swap）。比较和替换作为一个 Raft 命令，在提交后的状态机应用中原子执行。CLI 的普通 GET／PUT 行为保持兼容；原单节点服务不支持 CAS。

```bash
build/bin/yewukv-client --nodes 127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103 cas feature 0 initial
build/bin/yewukv-client --nodes 127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103 get-meta feature
# 将下面 REVISION 换成 get-meta 返回的整数，再更新
build/bin/yewukv-client --nodes 127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103 cas feature REVISION updated
```

`get-meta` 返回 `revision` 与 `value_hex`，用十六进制保留任意二进制 value。缺失仍退出 2。`cas KEY 0 VALUE` 只创建缺失的键；非零 revision 必须与当前键版本相同。成功返回新的 revision；比较失败返回 `CONFLICT=7`，CLI 退出 3 并报告当前版本，不覆盖当前值。两个发布者使用同一版本更新时至多一个成功。

版本取自成功修改对应的已提交 Raft 日志索引，单调但不要求连续。DELETE 后重新创建会得到更高版本，即使 value 恢复原内容，旧版本也不能再次写入。比较失败、容量拒绝仍是已提交的命令，但不会替换 value。CAS 仍遵循请求期限与队列上限；传输失败／UNAVAILABLE 后不能自动重放，因为写入可能已经生效。

## 配置应用

```bash
python3 examples/config_store.py publish --file examples/config.json --expected-revision 0
python3 examples/config_store.py inspect
# 将 REVISION 换成 inspect 的 storage_revision
python3 examples/config_store.py publish --file examples/config.json --expected-revision REVISION
```

原 `publish` 不带条件时继续使用普通 PUT，`show` 的内容格式保持兼容。配置文档内部的 SHA-256 `revision` 表示内容；`inspect.storage_revision` 是存储的整数并发版本。发布者先读取版本、修改内容，再条件发布；冲突后显式重新读取并决定如何合并。消费者仍轮询，HTTP 缓存的更新可见性没有变成直接线性一致读。尚未实现 Watch、跨 key 事务或租约。

## 快照与旧目录迁移

KV 快照 v2 保存每个键的 revision 和状态机版本位置；v1 的键值编码仍可读取。恢复或安装快照会恢复版本，因此重启与 Leader 变化不会重置已迁移键的版本。Raft journal／检查点外层格式仍为现有格式。

旧 v1 快照没有历史键版本。首次提交 `state_machine_version=2` 的修改命令时，所有副本在同一个日志位置初始化已有键的版本，避免不同副本使用不同旧快照边界而得到不同结果。迁移前 GET 的 revision 是 0；首次对已有旧键执行 `cas ... 0 ...` 会安全冲突，并返回初始化后的非零版本，重新读取后可提交新的条件更新。迁移包含版本元数据的容量开销；达到容量时保留已有值，仍可 DELETE 释放空间。

升级要求整组停机并备份后统一更换程序；不支持新旧命令语义混跑或旧程序打开 v2 KV 快照。回退使用升级前备份。本轮云端新功能在独立实验目录验证，没有迁移原服务的业务目录。

回归覆盖并发发布者冲突、删除／重建的 ABA 保护、版本快照恢复、不同旧快照边界的确定性迁移、容量拒绝、非法版本快照、快照追赶与整组重启。模型故障活动仍检查单 key PUT／GET／DELETE，不把它表述为包含 CAS 的通用模型证明。
