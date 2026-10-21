# Raft core and fixed-three-node integration

`Election` is a deterministic, network-free fixed-three-voter core for term,
vote and majority-election decisions. `RaftLog` handles previous-entry matching,
conflict hints, replacement of uncommitted suffixes, follower commit bounds,
and the current-term-only leader majority rule. Persistence failure poisons
either instance. Tests use both in-memory fault-injection stores and the
disk-backed `FileStore`, which implements `HardStateStore` and `LogStore`.

`FileStore` persists the current term, vote, snapshot and retained indexed/termed
log suffix in one checksummed atomic image (v1 images remain readable). It syncs the file before rename and syncs
the directory before success. Reopen validates the complete image; an I/O
failure poisons the instance until reopened. See
[`docs/raft-persistence.md`](../../docs/raft-persistence.md) for crash boundaries,
limits and the difference from the KV WAL.

`Node` integrates these cores for three fixed voters. Logical `Tick` calls drive
randomized elections, heartbeats and RPC retries; correlated replies update
per-peer next/match indexes. A new leader durably appends a current-term no-op.
`ProposeBatch` shares one local durable replacement across up to 32 commands;
requests still complete only after majority commitment and ordered application.

`cluster::Server` supplies real clocks, bounded work queues, peer transports and
KV application. Every read goes through the replicated log; local `status` is
diagnostic only. See [`docs/cluster.md`](../../docs/cluster.md) for the protocol,
fault tests, deployment, and current single-host/capacity limitations.

`RaftLog` indexes its retained suffix relative to the durable snapshot boundary.
`Node::Compact` covers only applied committed entries; `InstallSnapshot` transfers
16 KiB chunks, retries interrupted transfers and acknowledges installation only
after atomic durable publication. The runtime restores the KV snapshot before
applying the committed suffix. See [`docs/snapshots.md`](../../docs/snapshots.md).
