# Local journal / ReadIndex comparison

Same binary, same host, synchronous persistence, fixed three voters. Each cell is the median of run values.

| Profile | Mode | ops/s | P99 ms | Written B/op (3 nodes) | Encode+CRC us/op | Sync+publish us/op | Server cores |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| read-only | image-log | 1793.6 | 6.964 | 11259.8 | 109.8 | 920.0 | 0.67 |
| read-only | journal-log | 3816.4 | 4.916 | 399.7 | 4.5 | 397.6 | 0.74 |
| read-only | journal-read-index | 15730.1 | 1.285 | 0.0 | 0.0 | 0.0 | 1.41 |
| balanced | image-log | 1710.8 | 7.263 | 15437.1 | 145.6 | 927.4 | 0.72 |
| balanced | journal-log | 3693.8 | 4.385 | 602.3 | 6.7 | 408.6 | 0.75 |
| balanced | journal-read-index | 3408.9 | 5.513 | 422.0 | 4.7 | 389.9 | 0.78 |
| large-value | image-log | 253.9 | 56.193 | 754220.8 | 6282.6 | 1787.2 | 1.77 |
| large-value | journal-log | 2116.1 | 12.500 | 24766.4 | 235.3 | 523.3 | 1.02 |
| large-value | journal-read-index | 2176.1 | 12.128 | 18799.9 | 176.6 | 513.9 | 0.95 |
| more-keys | image-log | 838.1 | 14.825 | 147145.4 | 1237.5 | 1068.1 | 1.34 |
| more-keys | journal-log | 3308.4 | 7.061 | 3954.4 | 37.4 | 410.8 | 0.82 |
| more-keys | journal-read-index | 3260.3 | 8.089 | 2121.0 | 19.9 | 389.6 | 0.81 |

Three 10-second runs are the default; use manifest.json for the actual duration and repetitions.
All phases and written bytes sum the three nodes. Written bytes are user-space successful durable writes, not device-sector traffic.
Encode includes serialization and CRC; sync/publish includes file/directory synchronization and checkpoint publication.
Preload and warmup are excluded. Disk/CPU counters include a short replication drain. P99 is a median of run P99s.
Clients use a closed loop and disjoint keys and verify every GET. No saturation or cross-host availability claim is made.
