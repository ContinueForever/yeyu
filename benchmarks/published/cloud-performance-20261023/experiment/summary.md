# Existing-cluster benchmark

Each row summarizes repeated runs; percentile columns are medians of run
percentiles, not percentiles of a pooled distribution. See individual run JSON
for GET/PUT breakdown, P99.9, maximum latency, actual mix and raw samples.

| Profile | Connections | Runs | Median ops/s | Min–max ops/s | P95 ms | P99 ms | Failures |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| balanced | 1 | 3 | 165.3 | 162.3–165.7 | 10.143 | 11.516 | 0 |
| balanced | 4 | 3 | 443.8 | 443.1–450.8 | 13.522 | 17.038 | 0 |
| balanced | 8 | 3 | 743.1 | 743.1–743.9 | 15.013 | 21.330 | 0 |
| balanced | 16 | 3 | 1283.3 | 1276.1–1319.5 | 17.890 | 26.152 | 0 |
| balanced | 32 | 3 | 2336.8 | 2315.4–2337.0 | 22.488 | 27.235 | 0 |
| read-only | 1 | 3 | 268.5 | 264.4–271.2 | 4.964 | 5.613 | 0 |
| read-only | 4 | 3 | 952.5 | 950.5–961.0 | 5.672 | 7.054 | 0 |
| read-only | 8 | 3 | 1768.8 | 1747.1–1798.0 | 5.787 | 36.006 | 0 |
| read-only | 16 | 3 | 3233.8 | 3213.2–3235.1 | 6.514 | 42.763 | 0 |
| read-only | 32 | 3 | 5868.4 | 5821.0–5943.8 | 7.269 | 43.611 | 0 |
| write-heavy | 1 | 3 | 126.5 | 126.3–129.9 | 10.282 | 11.921 | 0 |
| write-heavy | 4 | 3 | 375.3 | 369.2–380.9 | 13.950 | 19.256 | 0 |
| write-heavy | 8 | 3 | 682.2 | 674.8–684.0 | 16.307 | 22.895 | 0 |
| write-heavy | 16 | 3 | 1237.7 | 1234.0–1261.5 | 19.586 | 25.765 | 0 |
| write-heavy | 32 | 3 | 2200.9 | 2163.1–2272.1 | 22.899 | 27.125 | 0 |

Client-side closed-loop latency includes request encoding, networking,
server work and response validation. One request is outstanding per connection.
Setup, preload, warmup and final value checks are excluded from throughput.
The final outstanding requests drain into the measured elapsed interval.
A client sharing an ECS node competes for resources. This is a workload
observation, not an isolated server capacity or open-loop latency claim.
Do not average the per-run P99s and label it a pooled P99.
