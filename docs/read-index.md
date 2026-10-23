# ReadIndex 的一致性边界

默认 `--read-mode read-index`；`--read-mode log` 保留旧读路径用于对照。GET 不再写 Raft 日志，也不触发磁盘持久化。PUT、DEL、PING 仍经多数派提交后应用。`status` 始终只是本地诊断。

## 完成一次读的条件

1. 接受请求时确认本节点为 Leader，并记录任期、截止时间，以及此前已接受／待批处理提案的最低应用位置。读确认可以与这些提案的复制并行；返回结果之前仍必须等待它们应用，保留同连接 PUT 后 GET 的使用习惯。并发流水线中的操作没有额外的跨请求事务保证。
2. 只有本任期已有条目提交时才能开始 ReadIndex。新 Leader 的持久化 no-op 因而必须先被多数派确认；重启或刚选举成功不能跳过这个门槛。
3. 为这批已经收到的读分配唯一 context，记录当前 commit index。运行时先登记读，再持久化同批写提案。`TakeMessages()` 在向传输层交付时才构造 AppendEntries，合并当前可复制条目和 context；已经交给传输层的请求不能改标签。后收到的读不能加入已经开始的轮次。
4. 副本检查 Leader 任期并处理日志后，在回复中回显 context。Leader 检查成员、任期、当前在途 sequence、context 和已匹配的日志位置。固定三个 voter 下，本节点与任意一个有效副本构成多数派。
5. 得到多数派确认后，运行时还必须等待 applied index 达到 ReadIndex 位置与先前提案的位置，再查询 KV map。

context 不使用客户端 request ID，也不会把旧 heartbeat 的确认用作新读的确认。掉包／超时会通过后续复制轮次重试；失去领导权、任期变化或超过截止时间时取消等待。读队列与写提案共享有界等待额度，断开的请求也会清理。

该路径不依赖 Leader lease 或时钟漂移上界。被隔离的旧 Leader 即使仍自认为是 Leader，也无法为新请求取得多数派确认；进程暂停后恢复时，同样不能用一个旧 context 完成之后收到的读。

## 验证

- 核心测试检查当前任期提交门槛、日志保存次数不增加、错误 context／sequence、重复确认、取消、隔离与旧任期。
- 同批尚未交付的写复制可携带新读 context；已经交付的旧复制回复不能确认之后的读，重复旧回复同样无效。运行时流水线测试同时验证先前写入的应用屏障以及实际发生的复制合并。
- 新增 100 个种子 × 1500 步的逻辑读写模拟，混合分区、丢包、乱序、重复回复、重启和快照；原 100 × 1000 步写入模拟保留。
- 真实 RPC 检查连续 100 次 GET 不增加三个副本的 last index、保存次数和写入字节；混合流水线验证读等待前面的 PUT 应用。
- 原有旧 Leader 分区测试、并发逐 key 历史校验，以及加入 Leader SIGSTOP/SIGCONT 的持续故障活动也覆盖此默认路径。

这些是有限实验和有界模型检查，不是形式化证明。模型仅覆盖单 key PUT/GET/DEL；没有跨 key 事务、动态成员变更或 Byzantine 故障保证。

方法参考：[etcd Raft 的安全读选项与当前任期提交门槛](https://github.com/etcd-io/raft/blob/main/raft.go)、[ReadState 与读上下文](https://github.com/etcd-io/raft/blob/main/read_only.go)。本项目使用独立的固定三 voter 实现。
