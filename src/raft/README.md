# Raft (G1 in progress)

`Election` is a deterministic, network-free fixed-three-voter core for term,
vote and majority-election decisions. `RaftLog` handles previous-entry matching,
conflict hints, replacement of uncommitted suffixes, follower commit bounds,
and the current-term-only leader majority rule. Persistence failure poisons
either instance. The tests use in-memory fault-injection stores; no disk-backed
Raft store is wired yet.

This is not a Raft cluster: there is no timer, transport, integrated node,
state-machine apply, or linearizable read path. The wire messages in
`proto/raft.proto` are not connected to this core. See
[`docs/roadmap.md`](../../docs/roadmap.md) for acceptance gates.
