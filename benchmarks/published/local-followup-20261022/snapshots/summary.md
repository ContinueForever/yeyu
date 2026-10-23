# Local read scheduling / checkpoint follow-up

Median of independent runs; same host, synchronous journal, fixed three voters.

| Profile | Variant | ops/s | P99 ms | P99.9 ms | Max ms | Written B/op | Checkpoints (3 nodes) | Snapshot pause ms/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cold-1MiB | baseline-fixed | 2670.9 | 15.847 | 21.027 | 29.654 | 12649.5 | 468 | 0.0000 |
| cold-1MiB | current-fixed | 3338.8 | 9.269 | 15.086 | 24.088 | 12625.7 | 582 | 0.0759 |
| cold-1MiB | current-adaptive | 4059.4 | 4.677 | 9.116 | 15.050 | 1344.2 | 61 | 0.0071 |
| cold-8MiB | baseline-fixed | 1057.1 | 168.699 | 186.350 | 195.092 | 96864.2 | 182 | 0.0000 |
| cold-8MiB | current-fixed | 1977.4 | 61.137 | 78.953 | 99.147 | 80575.6 | 282 | 0.2664 |
| cold-8MiB | current-adaptive | 4151.5 | 4.517 | 9.360 | 45.831 | 2345.2 | 15 | 0.0115 |
| cold-15MiB | baseline-fixed | 612.8 | 320.959 | 344.897 | 371.048 | 173845.0 | 102 | 0.0000 |
| cold-15MiB | current-fixed | 1201.2 | 120.361 | 166.988 | 242.652 | 148467.2 | 173 | 0.5655 |
| cold-15MiB | current-adaptive | 4054.6 | 3.959 | 8.513 | 88.435 | 4190.3 | 15 | 0.0192 |

All runs verify GET values. Large-state runs verify every cold/hot final value after all-node SIGKILL/restart.
Raw samples, binary/source identities and exact commands are retained. No saturation or physical power-loss claim.
Snapshot pause is unavailable (reported as zero) in the frozen baseline; it must not be treated as a zero-cost baseline.
P99/P99.9/max are medians of run statistics, not merged-sample statistics. Prefill/recovery are outside the timing window.
