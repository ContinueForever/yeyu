# Local read scheduling / checkpoint follow-up

Median of independent runs; same host, synchronous journal, fixed three voters.

| Profile | Variant | ops/s | P99 ms | P99.9 ms | Max ms | Written B/op | Checkpoints (3 nodes) | Snapshot pause ms/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| read-only | baseline-read-index | 14710.6 | 1.481 | 3.429 | 9.081 | 0.0 | 0 | 0.0000 |
| read-only | current-read-index | 15114.2 | 1.489 | 3.153 | 9.664 | 0.0 | 0 | 0.0000 |
| read-only | current-log | 4001.9 | 3.790 | 5.119 | 10.550 | 397.9 | 1395 | 0.0439 |
| balanced | baseline-read-index | 3403.1 | 5.637 | 8.332 | 13.029 | 422.0 | 594 | 0.0000 |
| balanced | current-read-index | 4073.5 | 4.427 | 10.859 | 26.026 | 416.4 | 711 | 0.0233 |
| balanced | current-log | 3700.0 | 4.643 | 6.677 | 8.644 | 597.9 | 1287 | 0.0453 |
| write-heavy | baseline-read-index | 3308.9 | 5.903 | 8.908 | 15.170 | 732.8 | 1038 | 0.0000 |
| write-heavy | current-read-index | 3672.9 | 5.078 | 7.781 | 12.059 | 726.5 | 1146 | 0.0417 |
| write-heavy | current-log | 3781.0 | 4.233 | 6.504 | 9.496 | 763.2 | 1316 | 0.0450 |
| large-value | baseline-read-index | 2158.3 | 12.048 | 16.351 | 22.325 | 18811.9 | 384 | 0.0000 |
| large-value | current-read-index | 2876.5 | 7.407 | 11.042 | 17.240 | 18702.5 | 513 | 0.0463 |
| large-value | current-log | 2575.7 | 9.130 | 11.398 | 14.862 | 23389.3 | 894 | 0.0656 |
| more-keys | baseline-read-index | 3158.4 | 8.246 | 11.702 | 16.192 | 2117.6 | 553 | 0.0000 |
| more-keys | current-read-index | 3768.7 | 6.238 | 9.418 | 13.456 | 1373.1 | 390 | 0.0229 |
| more-keys | current-log | 3832.9 | 5.037 | 6.642 | 9.804 | 1717.1 | 497 | 0.0270 |

All runs verify GET values. Large-state runs verify every cold/hot final value after all-node SIGKILL/restart.
Raw samples, binary/source identities and exact commands are retained. No saturation or physical power-loss claim.
Snapshot pause is unavailable (reported as zero) in the frozen baseline; it must not be treated as a zero-cost baseline.
P99/P99.9/max are medians of run statistics, not merged-sample statistics. Prefill/recovery are outside the timing window.
