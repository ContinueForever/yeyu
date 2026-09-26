# benchmarks (M6)

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

- `ycsb/` — YCSB workloads A–F against the cluster; record hardware, version,
  parameters, P50/P95/P99 and throughput.
- `e2e_writer.py` — monotonic-sequence writer/verifier used for RPO checks
  across leader kill / partition chaos runs.
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
  and file hashes, including untracked source. Prior results are excluded. This
  identifies code more precisely than HEAD when the working tree is dirty.

Choose a new output directory for every run; existing results are never replaced.
The runner requires Linux `/proc`, local TCP sockets and a prebuilt server.
For a remote server use `rpc_baseline.py` directly and separately record the
server hardware/build/options. Optional `--samples-output` preserves raw samples.

Machine-specific reports and raw artifacts from the initial local runs are
retained in the development checkout and excluded from this public CI branch.
Run the matrix above to collect new evidence on your own machine. Compare
versions only with the same script, workload and runtime; a whole-server
version comparison is not an isolated WAL A/B experiment.
