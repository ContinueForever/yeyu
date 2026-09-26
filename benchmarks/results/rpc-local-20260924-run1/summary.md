# Local RPC development baseline

| I/O threads | Clients | Median ops/s | Min–max ops/s | Median run P99 (ms) |
| --- | --- | --- | --- | --- |
| 0 | 1 | 13595.4 | 13413.1–14927.1 | 0.154 |
| 4 | 1 | 13496.8 | 13441.5–13807.0 | 0.171 |
| 0 | 8 | 7064.5 | 7038.0–8710.6 | 3.369 |
| 4 | 8 | 6915.3 | 6547.7–7391.1 | 3.429 |

Same-host Python threaded closed-loop load; one in-flight RPC and one hot key
per connection. 128-byte values, requested 50% PUT / 50% GET, 100 warmup
operations per connection. Samples exclude preload, warmup and socket teardown.
These results do not establish server saturation, storage or cluster performance.
P99 column is the median of run P99s, not a pooled percentile.

See manifest.json for environment, build, source identity and each run.
source.tar.gz captures the dirty working-tree inputs; source-manifest.json
contains their SHA-256 hashes. Each run retains nanosecond latency samples.
