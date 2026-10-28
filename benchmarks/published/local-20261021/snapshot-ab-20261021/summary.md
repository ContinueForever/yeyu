# Same-host snapshot KV load matrix

| Profile | Batch | Snapshot entries | Clients | PUT % | Value B | Keys | Median ops/s | Median run P99 ms | Server cores | Client cores |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| balanced | 32 | 128 | 8 | 50 | 128 | 64 | 1695.7 | 7.418 | 0.71 | 0.10 |
| balanced | 32 | 1000000000 | 8 | 50 | 128 | 64 | 506.2 | 32.997 | 1.59 | 0.04 |

P99 is the median of run P99s, not a pooled percentile. CPU cores are CPU seconds / measured wall time;
node CPU and disk counters include a short replication drain. RSS is sampled every 250 ms.
Preload and 20 warmup GETs per client are excluded. Every GET verifies the latest value for that client's key.
Client CPU is process CPU across load threads; raw latency_ns/completed_offset_ns are saved as gzip JSON.
timeline.json groups completions by second and retains throughput and latency trends within each run.
Processes share the same host, CPU and filesystem. These closed-loop loads do not establish saturation.
See manifest.json, source snapshot, commands, workload, resource-samples and all per-run results.
