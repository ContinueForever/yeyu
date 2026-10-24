# Three-ECS performance evidence, 2026-10-23 / 24

The received run started at **2026-10-23 23:38:35** and finished at
**2026-10-24 00:05:35**, Asia/Shanghai. It contains 45 closed-loop runs and
**1,954,767 timed, successful requests**, with 599,523 PUTs and 1,355,244 GETs.
No timed failures were recorded. Setup, warmup and final checks are separate.

- [Chinese report and figures](../../../docs/cloud-performance-20261024.md)
- [15-case CSV summary](summary.csv), [complete derived analysis](analysis.json)
- [Original-format repeated-run summary](experiment/summary.md)
- [Concurrency figure](figures/concurrency.png), [tail latency figure](figures/tails.png)

## Offline verification

From the repository root, with Python 3 standard library only:

```bash
python3 scripts/verify-cloud-performance.py benchmarks/published/cloud-performance-20261023
python3 benchmarks/remote_matrix.py --verify benchmarks/published/cloud-performance-20261023/experiment
```

The first verifier checks the complete publication inventory, runtime source
snapshot, three environment records, configuration and binary hashes, all 45
planned cases, all per-client samples, final-version counts, nearest-rank
percentiles, throughput, GET/PUT splits and CSV/JSON summaries. It independently
reconstructs prefill/warmup/timed PUT accounting against each node's applied
index. Read-only phases have zero persistent-byte and log-index growth. The four
previous acceptance values match before and after load.

The second command is the unmodified benchmark tool's original verifier.
Checks concern consistency of retained data; they do not authenticate remote
machines, prove continuous availability or reproduce cloud performance.

## Scope and versions

Three ECS virtual machines, each 2 vCPU / configured 4 GiB RAM and 40 GiB system
disk, Ubuntu 24.04.5 x86_64, Linux 6.8.0-139. The operating system reports about
3.5 GiB usable memory. Prior deployment records identify the economic e family,
ESSD Entry and the same Hangzhou VPC. This archive adds fresh pre-load hardware,
service, binary and member records for all three nodes.

Runtime commit: `909df769ae2d9b0579a31f9d89745e589da86819`.
The node and client executable hashes match all three environment records and
the earlier deployment archive. The benchmark scripts were recorded at clean
commit `c1b238cb7430a051ea8f21798c71aa064f8cdc41`; their retained copies match that
repository version. Pulling the benchmark tools did not replace the installed
node executable.

One existing voter per VM, journal persistence, ReadIndex GET, adaptive
checkpoints. Leader ID 2 / term 96 agree at retained state checks. The load
generator shares node 3's machine with a Follower. It uses up to two Python
processes, one in-flight request per persistent TCP connection, with TCP_NODELAY.
This is not an isolated-client or open-loop server capacity measurement.

Concurrency 1/4/8/16/32, 0/50/90% requested PUT, 128-byte values, eight disjoint
keys per connection, five-second per-connection warmup, thirty-second timed
window and three repeats. Cases are shuffled within each repeat with the
retained seed. Maximum experiment keyspace is prefilled once and reused;
the previous 36 keys remain, giving 292 final keys. No data directory is cleared.

## Retained records and normalization

`experiment/` retains the complete original-format result tree, including 549
per-client gzip sample files, per-client final-version metadata, initial / post-
warmup / final node states, workloads, process/host samples, the manifest and both
benchmark Python source files. `run.log` contains the 45 console result lines.
`nodes/` contains the three pre-load environment records.

Only actual private addresses and hostnames in imported text/JSON have been
replaced with `10.0.0.11` / `.12` / `.13` and `ecs-node-1` / `-2` / `-3`.
The experiment file inventory is recalculated after this normalization.
**All 549 gzip request-sample files are byte-for-byte identical to the originals.**
Counts, timings, latencies, flags, versions, PIDs, timestamps, binary hashes,
boot ID and ordering are preserved. The runtime source snapshot is the previously
published, unmodified tested Git version; existing source test literals are not
rewritten as if they were machine records.

`runtime-source.tar.gz` and its 154-file manifest are copied from the earlier
[cloud correctness evidence](../cloud-20261023/README.md). This is a code-review
snapshot excluding older benchmark evidence, not a replacement for Git metadata
when running the complete fault test suite.

`publication-manifest.json` identifies the four original user-provided files by
name, size and SHA-256, plus every published file by size and hash. Original
unmodified inputs remain outside the public Git repository.

## Figures and interpretation

Lines are medians of three run statistics; bands show min-to-max across those
three runs. P95/P99/P99.9 columns are medians of run percentiles, not pooled
request percentiles. The tail figure's maximum is the worst observed request
across the three runs. Actual GET/PUT counts and per-operation percentiles remain
in JSON, including the worst 152.175 ms request.

Figure regeneration requires Matplotlib and the Noto Sans CJK SC font. The
published plots were rendered with Matplotlib 3.11.2 / NumPy 2.5.4:

```bash
python3 benchmarks/plot_cloud_performance.py benchmarks/published/cloud-performance-20261023
```

Regeneration can change image bytes with different fonts or renderer versions;
numeric verification does not require plotting dependencies. After a deliberate
regeneration the publication inventory must be updated and reviewed.

This 27-minute small hot-key experiment does not establish production limits,
independent failure domains, multi-day stability, large-dataset behavior or
fault-under-load correctness. CPU/RSS time series cover client processes and
their shared host only; the other two VMs were not continuously sampled. The
observed high-concurrency read tail and write maximum are retained without an
unverified causal attribution.
