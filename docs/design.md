# yewukv design notes

Living document. Records decisions and trade-offs as milestones land, so the
repository tells a real engineering story rather than just containing code.

## M1 — network layer decisions

- **LT epoll (level-triggered)** for the first slice: simpler correctness
  semantics, no mandatory read-until-EAGAIN. The code is structured so an ET
  path (loop read to EAGAIN, drain-on-register) can be A/B benchmarked later;
  the LT/ET trade-off is a deliberate interview "hook".
- **one loop per thread**: acceptor in the base loop, connections round-robin
  to worker loops; cross-thread `Send()` hops via `RunInLoop` + eventfd wakeup.
- **Buffer**: contiguous vector with prepend space and read/write indices,
  `readv` + 64KB stack scatter buffer to avoid premature growth on large reads.
- **Text protocol first, Protobuf RPC next**: the line protocol exists only to
  validate the IO path; the production wire is length-prefixed + protobuf
  (Raft schema drafted in `proto/raft.proto`). The frame decoder must still
  handle TCP fragmentation, coalescing and invalid lengths.
- **Engine interface from day one**: network code depends on `KVEngine`, never
  on a concrete map. WAL sync runs on the storage executor; keeping blocking
  disk work off the I/O loop preserves service to other connections.

## M1 — reliability pass

- **Connection lifetime**: an epoll event holds a connection reference during
  dispatch. Closed connections leave the registry only after the current
  batch of active Channels has been processed. This prevents the disconnect
  use-after-free reproduced under AddressSanitizer.
- **Shutdown**: SIGINT/SIGTERM are blocked before workers start and received
  through signalfd in the base loop. Acceptance stops, queued responses get
  time to drain, and a timer forces remaining connections closed before worker
  threads join. The deadline depends on a responsive event loop; a blocking
  application callback can delay it.
- **Resource bounds**: limits on text requests, connection buffers and total
  connections reject overlarge or stalled clients without affecting healthy
  peers. These are intentionally fixed for the first reliability pass.

## M1 — framed KV RPC

- **Wire format**: four-byte unsigned big-endian payload length followed by a
  serialized `yewukv.rpc.KvRequest` or `KvResponse`. The payload must contain
  1–65536 bytes. A partial header or body remains buffered; zero or oversized
  lengths and malformed Protobuf close the connection.
- **Service mode**: `--protocol rpc` selects the framed endpoint. The default
  `text` mode remains available for the original bring-up client. Both modes
  use the same KVEngine semantics, connection limits and shutdown path.
- **Correlation**: each request carries a nonzero request ID. The client owns
  ID allocation, reuses one connection, and matches responses by ID so multiple
  calls can be in flight. Application results are carried in `KvResponse.code`;
  transport timeout, disconnect and shutdown complete the matching future with
  a local Status. Calls are never retried automatically.
- **Timeouts**: a client timeout stops waiting; it does not prove the server
  did not apply a write. Before automatic retries are added, the protocol needs
  a durable deduplication key and a defined recovery rule.
- **Execution**: M1 initially applied commands on the I/O loop. The bounded
  storage executor described below now keeps engine calls off those loops and
  posts ordered completions back to each connection.

## M1 — idle connection eviction

- A single `CLOCK_MONOTONIC` timerfd on the base loop scans the bounded connection
  registry once per second. `--idle-timeout` defaults to 60 seconds, accepts
  0–86400 seconds, and disables the timer at zero. This avoids one timer fd per
  connection; scanning is O(number of connections), currently capped at 1024.
- A connection records successful socket reads and writes using `steady_clock`.
  The scan schedules a check on its owning loop, where activity is rechecked
  before closing. Shared ownership preserves lifetime across the handoff; the
  existing deferred removal protects the current epoll dispatch batch.
- Expiration force-closes incomplete requests and stalled output. Active byte
  transfers refresh the deadline, so this is inactivity control, not an absolute
  request deadline or protection against peers trickling bytes indefinitely.
  Eviction normally occurs within one scan interval after expiry, but a blocked
  event loop can delay it. During graceful drain the shutdown timer takes over.
- RPC clients do not reconnect automatically. A write without an observed reply
  still has an uncertain outcome after eviction, just as after any disconnect.

## M2 preparation — bounded storage executor

- One storage worker serializes accepted `KVEngine` calls in the order admitted
  under the executor mutex. Its configured capacity (`--storage-queue`, default
  4096, range 1–8192) counts the running call and queued calls. `Submit` never
  waits on storage. Rejected calls return text `-ERR busy` or RPC `BUSY` (4);
  they have not been applied. Each TCP connection reserves a reply sequence
  number when a request is parsed; completion or rejection fills that slot.
  Later replies remain pending until earlier slots are filled, preserving text
  pipeline and RPC response order despite cross-thread completion handoff.
- Completions keep only a weak connection reference while storage runs. When
  the peer is still present, the response is posted to its owning event loop;
  disconnect discards it. The shutdown hook stops new submissions, waits for
  admitted work before stopping I/O workers, and lets existing responses drain
  until the one-second TCP deadline. If an operation blocks beyond that
  deadline, the response may be lost even though the operation later runs.
  Storage draining itself has no deadline: an indefinitely blocked engine
  callback prevents process exit. WAL integration must preserve this explicit
  behavior or add cancellation/operation timeouts.
- This queue bounds the number of accepted storage operations, not a total
  request-byte or memory budget. Existing input/output and connection caps
  remain in force. The storage interface is still backed by a memory map, now applied after the
  WAL reaches stable storage. Durability comes from the WAL, not the map.

## M2 — WAL and recovery

- The server opens `--data-dir` (default `data`), creates it when missing,
  and holds a nonblocking exclusive `flock` on `LOCK`. A second process cannot
  open the same directory for writes. The active WAL is `wal.log`; log
  reclamation atomically replaces it. Open uses `O_NOFOLLOW`, and the WAL must
  be a regular file.
- Normally each Put/Delete is one record: ASCII `YWAL` magic, version byte 1, operation
  byte, two reserved zero bytes, big-endian uint64 sequence, big-endian uint32
  key and value lengths, raw key/value bytes, then big-endian IEEE CRC32 over
  version through payload. Maximum key is 1 MiB, value is 15 MiB, and file is
  capped at 1 GiB. Sequence begins at 1 and must be contiguous within each WAL
  generation; compaction rewrites live keys with fresh sequences. Empty keys,
  unknown operations, invalid reserved bits and invalid lengths are rejected.
- Each normal mutation is fully appended with `pwrite`, then `fdatasync` completes
  before the in-memory map changes and before success can be returned. A Delete
  committed by replacement WAL waits for its file and directory syncs. There
  is no group commit; each mutation pays one sync. If append or sync fails, the
  engine enters a failed state and rejects future reads/writes because the last
  record's durability is uncertain. If applying a synced operation to memory
  fails, the engine also stops serving stale state; restart replays the record.
- Startup replays complete valid records in sequence. A physically incomplete
  final header/body is truncated to the last complete offset and synced before
  accepting work. A complete record with a bad CRC, bad format, or sequence gap
  fails startup rather than silently dropping data. This policy cannot repair
  arbitrary media corruption or prove survival of storage hardware that lies
  about flush completion.
- Newly created directory entries are synced at startup. This depends on Linux
  filesystem and device guarantees for `fsync`/`fdatasync`; no portability claim
  is made. Before an append exceeds 1 GiB, the engine writes a replacement WAL
  with one Put per live key to `wal.compact`, syncs it, renames it over `wal.log`,
  then syncs the directory. A crash before rename leaves the old WAL authoritative;
  a stale `wal.compact` is ignored on startup. The replacement is durable before
  the pending mutation is appended. A Delete that cannot fit is committed by a
  replacement snapshot omitting that key. Writes return BUSY only when the live
  set and pending record cannot fit. This is WAL log reclamation, not an LSM or
  Raft snapshot. Operators must preserve the directory and never manually delete
  `wal.log` to make space.
- A successful Put/Delete response follows `fdatasync`. `Get` reflects the
  in-process map. This does not imply replication or consensus: clients can
  still time out after the record became durable, and no deduplication key
  makes retries safe.

## Open questions (resolve when implementing)

- WAL fsync policy vs group commit batching (measure P99 on cloud SSDs).
- ReadIndex vs LeaseRead: start with ReadIndex (no clock dependence).
- Fixed shard table first; Region split + Epoch only after failover is solid.
