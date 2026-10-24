# Same-host snapshot KV load matrix

| Profile | Batch | Snapshot entries | Clients | PUT % | Value B | Keys | Median ops/s | Median run P99 ms | Server cores | Client cores |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single-client | 32 | 128 | 1 | 50 | 128 | 8 | 422.6 | 3.856 | 0.54 | 0.03 |
| read-heavy | 32 | 128 | 8 | 10 | 128 | 64 | 1749.6 | 7.137 | 0.68 | 0.10 |
| balanced | 32 | 128 | 8 | 50 | 128 | 64 | 1695.6 | 7.312 | 0.71 | 0.10 |
| write-heavy | 32 | 128 | 8 | 90 | 128 | 64 | 1650.1 | 7.588 | 0.74 | 0.10 |
| large-value | 32 | 128 | 8 | 50 | 8192 | 64 | 225.1 | 65.539 | 1.68 | 0.02 |
| more-keys | 32 | 128 | 8 | 50 | 128 | 1024 | 809.9 | 15.308 | 1.32 | 0.05 |
| more-clients | 32 | 128 | 16 | 50 | 128 | 128 | 2891.6 | 8.489 | 0.80 | 0.16 |

P99 is the median of run P99s, not a pooled percentile. CPU cores are CPU seconds / measured wall time;
node CPU and disk counters include a short replication drain. RSS is sampled every 250 ms.
Preload and 20 warmup GETs per client are excluded. Every GET verifies the latest value for that client's key.
Client CPU is process CPU across load threads; raw latency_ns/completed_offset_ns are saved as gzip JSON.
timeline.json groups completions by second and retains throughput and latency trends within each run.
Processes share the same host, CPU and filesystem. These closed-loop loads do not establish saturation.
See manifest.json, source snapshot, commands, workload, resource-samples and all per-run results.
