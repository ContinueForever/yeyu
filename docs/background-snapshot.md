# 后台快照编码

新增 `--snapshot-encoder sync|background`，默认 `sync`，用于同一程序的受控对照。它调整状态机的快照编码阶段；Raft 的提交、原子发布与日志回收契约保持一致。

```bash
build/bin/yewukv-node ... --snapshot-encoder background
```

节点线程在已应用的索引处冻结有序 key 和不可变 value 的共享所有权，再将 Protobuf 构建与序列化交给后台任务。后续 PUT 创建新的不可变 value，DELETE 移除活动映射，不会改变冻结任务持有的数据。最多一个在途编码任务，不积累快照队列。

后台完成后，节点线程检查目标是否已被更新的已安装快照覆盖；过期结果丢弃。有效结果在原来的 `Node::Compact` 路径发布，保留冻结位置之后的最新日志后缀。编码异常按节点失败处理，停止服务；停止节点会等待任务结束，避免任务访问已析构对象。

**磁盘检查点发布、CRC、文件同步与目录同步仍由原路径串行完成。** 当前改动没有把持久化确认改为异步，也没有消除全部快照暂停。自动 journal 检查点、快照安装和同步落盘仍可能影响长尾。

`status` 新增：

| 字段 | 含义 |
| --- | --- |
| snapshot_encoder | sync 或 background |
| snapshot_jobs | 启动的后台编码次数 |
| snapshot_discarded | 被更高快照覆盖而丢弃的任务结果 |
| snapshot_encode_ns | 已收取后台结果的编码墙钟时间累计 |
| snapshot_freeze_max_ns | 单次冻结及任务交接的最长节点线程暂停 |
| snapshot_publish_max_ns | 最长发布暂停；sync 模式还包括同步编码 |

原 `snapshot_pause_ns / snapshot_pause_max_ns` 继续记录节点线程在该快照路径中的累计／单段暂停。后台编码本身不计入节点线程暂停；这些时间均不是实际 CPU 指令时间。最大值是进程启动以来的值，包含预装，不应擅自当作计时窗口的最大值。

代价包括额外线程、冻结 key／共享指针数组、旧 value 延长生命周期及后台 Protobuf 缓冲。任务数有界不等于零内存开销，应结合 RSS 与吞吐测量。先使用显式选项复核具体负载，不将频繁强制快照压力场景的收益推广到默认自适应策略下的全部请求。
