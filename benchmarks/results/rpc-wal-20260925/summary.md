# Local RPC development baseline

| I/O threads | Clients | Median ops/s | Min–max ops/s | Median run P99 (ms) |
| --- | --- | --- | --- | --- |
| 0 | 1 | 2530.1 | 2526.1–2541.9 | 0.916 |
| 4 | 1 | 2529.8 | 2523.5–2563.5 | 0.873 |
| 0 | 8 | 3545.9 | 3521.2–3620.8 | 4.494 |
| 4 | 8 | 3637.5 | 3555.3–3738.1 | 4.250 |

Same-host Python threaded closed-loop load; one in-flight RPC and one hot key
per connection. 128-byte values, requested 50% PUT / 50% GET, 100 warmup
operations per connection. Samples exclude preload, warmup and socket teardown.
These results do not establish server saturation, storage or cluster performance.
P99 column is the median of run P99s, not a pooled percentile.

See manifest.json for environment, build, source identity and each run.
source.tar.gz captures the dirty working-tree inputs; source-manifest.json
contains their SHA-256 hashes. Each run retains nanosecond latency samples.
