# Raft core and fixed-three-node integration

`Election` is a deterministic, network-free fixed-three-voter core for term,
vote and majority-election decisions. `RaftLog` handles previous-entry matching,
conflict hints, replacement of uncommitted suffixes, follower commit bounds,
and the current-term-only leader majority rule. Persistence failure poisons
either instance. Tests use both in-memory fault-injection stores and the
disk-backed `FileStore`, which implements `HardStateStore` and `LogStore`.

`FileStore` supports the original atomic image and runtime-default incremental
journal with generation-bound checkpoints; v1/v2 images can be imported.
See [the journal contract](../../docs/journal.md). Deltas are synced before
acknowledgement; checkpoints sync the file, publish by rename and sync the
directory. Reopen validates the checkpoint and journal; an I/O failure poisons
the instance until reopened. See
[`docs/raft-persistence.md`](../../docs/raft-persistence.md) for crash boundaries,
limits and the difference from the KV WAL.

`Node` integrates these cores for three fixed voters. Logical `Tick` calls drive
randomized elections, heartbeats and RPC retries; correlated replies update
per-peer next/match indexes. A new leader durably appends a current-term no-op.
`ProposeBatch` shares one local durable save across up to 32 commands;
requests still complete only after majority commitment and ordered application.
Append/snapshot sends are queued until `TakeMessages()` constructs and hands
them to the transport. Drivers must drain messages after each owner batch;
this lets fresh read contexts share replication without retagging in-flight RPCs.

`cluster::Server` supplies real clocks, bounded work queues, peer transports and
KV application. GET defaults to ReadIndex with a current-term commit gate,
a fresh correlated quorum and an application fence; log reads remain selectable.
See [the read contract](../../docs/read-index.md). Local `status` is diagnostic only. See [`docs/cluster.md`](../../docs/cluster.md) for the protocol,
fault tests, deployment, and current single-host/capacity limitations.

`PersistenceQueue` adds an owned, bounded submission/completion contract for a
single storage stream. Tests drive actual stores and Raft nodes through a
blocking compatibility bridge to check durability gates. The default server
still uses synchronous `FileStore`; opt-in `--disk-executor background` adds
a single disk consumer and nonblocking prepare/advance continuations, including
immediate leadership fencing while I/O is pending. See
[the asynchronous integration](../../docs/background-persistence.md). See [the queue contract](../../docs/persistence-queue.md).

`RaftLog` indexes its retained suffix relative to the durable snapshot boundary.
`Node::Compact` covers only applied committed entries; `InstallSnapshot` transfers
16 KiB chunks, retries interrupted transfers and acknowledges installation only
after atomic durable publication. The runtime restores the KV snapshot before
applying the committed suffix. See [`docs/snapshots.md`](../../docs/snapshots.md).
