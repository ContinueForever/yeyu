# yewukv

A distributed key-value store built from scratch in modern C++, targeting
strongly-consistent replication with the Multi-Raft protocol.

> Status: **M2 storage slice + early G1 election core** — Reactor network layer,
> framed RPC, bounded storage worker and checksummed WAL recovery run end-to-end.
> Raft election and log rule helpers have deterministic tests; no Raft cluster,
> LSM or sharding is implemented
> (see the [six-month roadmap](docs/roadmap.md)).

## Long-term architecture (target)

```text
Client SDK / CLI / Redis-compatible gateway
            │  TCP, length-prefixed frames + Protobuf RPC
            ▼
Reactor network layer   (epoll, one loop per thread, framing)
            │  key → Raft Group routing
            ▼
Multi-Raft consensus    (election, log replication, snapshot, membership)
            │  apply committed entries
            ▼
Storage engine          (WAL → SkipList MemTable → SSTable + BloomFilter)
```

## Current vertical slice (M1–M2)

- Non-blocking TCP server: `epoll` Reactor, one-loop-per-thread worker pool,
  growable read/write buffers, cross-loop task handoff via `eventfd`.
- Line-based text protocol for bring-up:
  `PUT <k> <v>`, `GET <k>`, `DEL <k>`, `PING` (RESP-ish replies).
- `--protocol rpc`: 4-byte length-prefixed Protobuf KV requests and responses,
  correlation IDs, a reusable asynchronous client with futures and deadlines.
- `KVEngine` with a checksummed WAL and in-memory ordered map.
  A single FIFO storage worker executes KV operations outside I/O loops; startup
  replays the WAL before accepting requests. The WAL can atomically compact to
  the current live map when its 1 GiB file limit is reached.
- Network-free Raft helpers check term/vote, candidate log freshness, log
  matching and majority commit; they are not yet wired to disk or RPC.
- GoogleTest and process-level TCP coverage for the network layer, framing,
  async client, storage queue, request ordering, limits, concurrency and shutdown.
  WAL recovery tests cover torn tails, damaged complete records, exclusive
  directory ownership, and acknowledged writes after SIGKILL and restart.
- Bounded text line (64 KiB), input buffer (128 KiB), output buffer (1 MiB)
  and connection count (1024); `--storage-queue <count>` sets the storage
  operation limit to 1–8192 (default 4096). A full queue returns `-ERR busy`
  or RPC `BUSY` (code 4). SIGINT/SIGTERM drain responses with a one-second
  event-loop deadline.
- Idle TCP connections expire after 60 seconds without a successful read or
  write; `--idle-timeout <seconds>` configures 0–86400 seconds (0 disables it).
  Both protocols share this policy; checks run once per second.

## Build

The server defaults to `--data-dir data`; pass the same directory after restart
for WAL recovery.
The toolchain (g++ 14, CMake, Ninja, Protobuf, GTest) lives in a user-space
micromamba environment at `~/.micromamba/envs/yewu`; no root is required.

```bash
source scripts/env.sh        # activate toolchain
scripts/build.sh             # configure + build (Ninja)
scripts/test.sh              # ctest (includes local TCP integration test)
```

Binaries are emitted under `build/bin/`.

To run under AddressSanitizer and UndefinedBehaviorSanitizer:

```bash
source scripts/env.sh
cmake -S . -B build-asan -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DYEWUKV_ENABLE_SANITIZERS=ON -DCMAKE_PREFIX_PATH="$YEWU_ENV"
cmake --build build-asan -j4
ctest --test-dir build-asan --output-on-failure
```

If LeakSanitizer cannot inspect processes in a restricted container, set
`ASAN_OPTIONS=detect_leaks=0` for the build and test commands. AddressSanitizer
and UndefinedBehaviorSanitizer remain active in that mode.

## Run

```bash
./build/bin/yewukv-server --port 9000 --io-threads 4 --data-dir data
# in another shell (or use nc/python):
python3 - <<'PY'
import socket
s = socket.create_connection(("127.0.0.1", 9000))
s.sendall(b"PING\nPUT foo bar\nGET foo\n")
print(s.recv(4096).decode())
PY
```

The current Compose deployment is one durable node, not a Raft cluster:

```bash
docker compose -f deploy/docker-compose.yml up --build -d
./build/bin/yewukv-client --port 9000 put foo bar
docker compose -f deploy/docker-compose.yml restart node
./build/bin/yewukv-client --port 9000 get foo
```

Compose stores the WAL in the `node-data` volume. The client commands above
require a local build; alternatively run the client inside the container.
For a repeatable write/restart/container-recreation check that cleans up its
own Compose project and volume, run `scripts/ci-compose-smoke.sh`.

To exercise the framed RPC mode with the C++ client:

```bash
./build/bin/yewukv-server --port 9001 --io-threads 4 --protocol rpc
# in another shell:
./build/bin/yewukv-client --port 9001 ping
./build/bin/yewukv-client --port 9001 put foo bar
./build/bin/yewukv-client --port 9001 get foo
```

For example, `--idle-timeout 300` keeps inactive connections for five minutes.
Idle eviction closes the transport, including incomplete requests and stalled
output. Clients should send periodic requests or create a new client after an
idle disconnect; the C++ RPC client does not reconnect automatically.

Text is still the default server protocol. Each listening port serves one
protocol mode. RPC calls are not retried automatically; a timed-out write may
still have been applied. Accepted operations execute in one FIFO storage order,
and replies on each connection preserve request order. Successful PUT/DEL
responses follow a synced WAL record or a synced replacement WAL that includes
the deletion. A timed-out or disconnected client cannot infer
whether an accepted write ran. The storage worker drains accepted work during
shutdown; the one-second TCP response deadline can expire first if a storage
operation blocks.
WAL records use a versioned binary format with a sequence number and CRC32.
Incomplete final records are truncated during recovery; checksum or format
corruption fails startup. The server takes an exclusive lock on the data
directory. The WAL is capped at 1 GiB. When an append would exceed it, the
engine writes a compacted replacement containing live keys and retries; writes
return BUSY if the live set plus the new record cannot fit. Use a stable local
filesystem that honors `fdatasync` and `fsync` for the durability guarantee.

See [the RPC design notes](docs/design.md#m1--framed-kv-rpc) and
[development baseline instructions](benchmarks/README.md). Machine-specific
benchmark reports and raw samples remain in the local development checkout.

## Roadmap

The current M1/M2 single-node slice is implemented, and G1's election and log
rule helpers have started. The next gates are durable Raft storage and an
integrated fixed three-node service, followed by fault validation and
operational evidence. See the
[six-month roadmap](docs/roadmap.md) for acceptance criteria and scope limits.

## Layout

```
src/common/   Status/Result, logging
src/net/      Reactor: EventLoop, Poller(epoll), Channel, Acceptor, TcpServer
src/rpc/      length-prefixed Protobuf RPC and asynchronous client
src/raft/     deterministic election and log rule helpers (G1); cluster pending
src/storage/  KVEngine, WalEngine + recovery, MemEngine
src/kv/       command protocol + server wiring
proto/        Protobuf schemas
app/          server executable
tests/        GoogleTest unit tests
deploy/       single-node durable Docker Compose config (cluster pending)
scripts/      toolchain/build/test helpers
benchmarks/   YCSB/self-built load generators (M6)
docs/         design notes
```

## Toolchain notes

For a user-space toolchain, install dependencies with
[micromamba](https://mamba.readthedocs.io/):

```bash
micromamba create -n yewu -c conda-forge \
  gxx_linux-64=14 cmake ninja libprotobuf gtest clang-tools ccache
```

`scripts/env.sh` points CC/CXX/CMAKE_PREFIX_PATH at that environment.
