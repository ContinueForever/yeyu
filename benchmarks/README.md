# Benchmarks and experiment evidence

The [2026-10-24 three-ECS report](../docs/cloud-performance-20261024.md) records
45 runs and 1,954,767 timed requests across three read/write mixes and five
concurrency levels. [Published samples, environment records and figures](published/cloud-performance-20261023/README.md)
can be checked with `python3 scripts/verify-cloud-performance.py benchmarks/published/cloud-performance-20261023`.
Clients share a Follower VM; this is a bounded closed-loop baseline, with the
high read tail and 152.175 ms worst request retained.

To measure an **existing three-machine deployment**, use
[`remote_matrix.py`](remote_matrix.py) and the [cloud benchmark guide](../docs/cloud-benchmark.md).
It discovers the stable leader, retains checked per-request samples and compares
1/4/8/16/32 connections under three read/write mixes. It does not launch nodes.
The local runners below create their own same-host clusters instead.

The [2026-10-22 follow-up report](../docs/local-followup-20261022.md) adds frozen-binary
read scheduling and same-current-binary fixed/adaptive checkpoint controls,
including all-node crash/recovery checks at 1/8/15 MiB of cold values.

The [2026-10-21 journal / ReadIndex optimization report](../docs/local-optimization-20261021.md)
compares three modes of the same binary. Reproduce it without running another load concurrently:

Use its archived source for the historical implementation. Running the tool
against a current build measures the current code and defaults, including the
adaptive snapshot policy; it does not reproduce the old implementation exactly.

```bash
python3 benchmarks/local_optimization.py --node build/bin/yewukv-node \
  --output benchmarks/results/my-local-optimization --duration-seconds 10 --repeats 3
python3 benchmarks/local_optimization.py --verify benchmarks/results/my-local-optimization
python3 scripts/verify-local-optimization.py benchmarks/published/local-optimization-20261021
```

Modes are image+log reads, journal+log reads and journal+ReadIndex. The matrix
interleaves four profiles and three repetitions, records raw latency/completion
samples, CPU/RSS, and three-node persistence bytes plus encoding/CRC, write and
sync/publication times. Every GET checks the expected value. Pure ReadIndex reads
must produce zero persistent bytes and no log growth. Keep all runs, including
failures. Mode comparisons use fresh directories; journal directories cannot be
opened in image mode.

The follow-up tool compares a frozen prior binary with the current binary,
including a same-current-binary log-read control and a fixed/adaptive checkpoint
control. Freeze both binaries before running; do not rebuild or run another load
until the experiment finishes:

```bash
python3 benchmarks/local_followup.py --baseline /absolute/path/to/prior/yewukv-node \
  --node build/bin/yewukv-node --suite reads --duration-seconds 15 --repeats 3 \
  --output benchmarks/results/my-read-followup
python3 benchmarks/local_followup.py --baseline /absolute/path/to/prior/yewukv-node \
  --node build/bin/yewukv-node --suite snapshots --duration-seconds 15 --repeats 3 \
  --output benchmarks/results/my-checkpoint-followup
python3 benchmarks/local_followup.py --verify benchmarks/results/my-read-followup
```

The snapshot suite preloads 1/8/15 MiB of cold values, then updates small hot
keys. Each run kills all three nodes and checks every final cold/hot value after
restart. Samples retain operation kinds, P99.9 and maximum latency as well as
P50/P95/P99. Prefill and recovery are outside the measurement window. A frozen
baseline without new pause counters reports zero for missing counters; do not
interpret those as zero baseline checkpoint cost.

The [2026-10-21 acceptance report](../docs/local-acceptance-20261021.md)
describes the earlier image/log snapshot cluster. Its [published evidence](published/local-20261021/README.md)
includes raw latency samples, all 948 fault histories and SHA-256 checksums.

M1 single-node RPC development baseline (run the server with `--protocol rpc`):

```bash
python3 benchmarks/rpc_baseline.py --host 127.0.0.1 --port 9000 \
  --clients 8 --ops-per-client 1000 --write-percent 50 --value-size 128 \
  --output /tmp/yewukv-rpc-baseline.json
```

The script reuses one connection per client, checks every response, and reports
throughput plus P50/P95/P99 latency, run parameters, machine details and Git
revision. Client and server share a host, so these figures are development
baselines rather than cluster or production performance claims.

- All numbers must be reproducible; VM-local numbers are development-only.

## Reproducible local matrix

Build first, then use the same Python runtime when comparing runs:

```bash
bash scripts/build.sh
/usr/bin/python3 benchmarks/run_baseline.py \
  --output-dir benchmarks/results/my-rpc-baseline
```

The runner starts a fresh server for each combination of 0/4 I/O threads and
1/8 clients, repeated three times. Each run uses its own WAL data directory
inside the output directory. Defaults: 5,000 measured operations per
client, 100 warmup operations, 128-byte values, 50% PUT / 50% GET. Each client
uses its own single hot key and has one outstanding request. Operations follow
100-operation blocks (PUTs first); counts record the actual mix, including a
partial final block. This is not a large dataset, random workload or YCSB run.

The timed interval begins in the barrier action before releasing clients and
ends when the final worker finishes its requests. It excludes connection setup,
prepopulation, warmup and socket teardown. Latencies include Python encode/decode,
response validation, scheduling and network time. Python threads and the server
share the host; these figures cannot establish maximum C++ server throughput.

Each output directory contains:

- `summary.md` and `manifest.json`: repeated-run summary, exact commands, CPU,
  memory, Python/compiler versions, build type/options, binary SHA-256, Git state.
- Per-run JSON and `*-samples.json`: counts, throughput, nearest-rank P50/P95/P99,
  and every measured latency in nanoseconds (grouped by client, not time ordered).
- Server/client logs and command files, including failures. A failed run leaves
  `status: failed` and does not produce a successful summary.
- `source.tar.gz` and `source-manifest.json`: actual working-tree source snapshot
  and file hashes, including untracked source. Generated and published results are excluded. This
  identifies code more precisely than HEAD when the working tree is dirty.

Choose a new output directory for every run; existing results are never replaced.
The runner requires Linux `/proc`, local TCP sockets and a prebuilt server.
For a remote server use `rpc_baseline.py` directly and separately record the
server hardware/build/options. Optional `--samples-output` preserves raw samples.

The [2026-09-24 report](rpc-baseline-20260924.md) links the initial collected
baseline, which predates the WAL. Compare new results against it only as a
change in the whole server configuration, not as a like-for-like storage result.
The [2026-09-25 WAL report](rpc-wal-baseline-20260925.md) records the first
durable-write run with the same workload and its raw evidence.
The original source archives are omitted from the public repository because
they also contain unrelated documents; the source hash manifests remain for
provenance. New runs can create their own snapshots with `run_baseline.py`.


## Snapshot cluster matrix and fault campaign

`cluster_matrix.py` runs fresh three-node clusters and multiprocess load generators.
The default seven profiles vary concurrency, PUT proportion, active keys and value size;
all GETs check the current value in a disjoint per-client keyspace. Timed work excludes
preload and 20 warmup GETs per client. Raw gzip JSON contains `latency_ns` and
`completed_offset_ns`; `timeline.json` records one-second throughput/latency windows.
CPU, sampled RSS, published file size, persistence bytes and snapshot counters are retained.

```bash
python3 benchmarks/cluster_matrix.py --node build/bin/yewukv-node \
  --duration-seconds 10 --repeats 3 --output benchmarks/results/my-matrix
# Same binary, identical workload, interleaved snapshot threshold comparison.
# The large thresholds suppress snapshots only within this bounded experiment.
python3 benchmarks/cluster_matrix.py --node build/bin/yewukv-node \
  --profiles balanced --duration-seconds 10 --repeats 3 \
  --snapshot-entries 128,1000000000 --snapshot-bytes 67108864 \
  --output benchmarks/results/my-snapshot-ab
python3 scripts/cluster-soak.py --node build/bin/yewukv-node \
  --client build/bin/yewukv-client --duration-seconds 900 \
  --output benchmarks/results/my-soak
```

Outputs must be new directories. Each experiment retains commands, binary/source
identity and raw evidence. A failed run aborts the matrix and remains recorded.
P99 summaries are medians of run P99s, not pooled percentiles. Resource counters
include a short replication drain; the completion latency interval does not.
Partial final timeline windows should not be compared with full one-second windows.
All nodes and clients share a host; no server saturation or cross-host claim follows.

Fault campaigns use fresh per-epoch keys so the exhaustive per-key checker remains
bounded without dropping same-key dependencies. Unknown writes remain optional,
unbounded-completion operations. Real-process seed values reproduce the scenario
choices; OS scheduling remains nondeterministic. Logical core tests are separately
reproducible with fixed message/tick seeds.
