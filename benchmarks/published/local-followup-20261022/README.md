# 2026-10-22 local follow-up evidence

All nodes and load generators ran on one computer. This retains the frozen
baseline identity/source, interleaved read and large-state checkpoint samples,
normal/sanitizer/disk regression logs, and every bounded fault history.

The large-state matrix kills all nodes and checks every cold/hot final value.
The fault campaign also checks acknowledged values across epochs and after its
final all-node restart. Unknown writes stay optional with an unbounded finish.

Mutable node directories and executable binaries are omitted. Exact commands,
binary SHA-256 identities, source archives and workload parameters are retained.
No cross-host, physical power-loss, production saturation or multi-day claim is
made. See the report in `docs/local-followup-20261022.md` for interpretation.

The first read-suite stdout had a run-count printing variable overwritten by a
latency statistic. Its manifest and raw records contain all 45 runs; corrected
verification is retained in `acceptance/reads-verification.log`.

The top-level JSON files in `acceptance/*-evidence` identify the measured binary.
Their subdirectories retain a node-log cache that may also contain earlier test
runs. Latest regression results are `normal-complete.log`, `asan-final.log` and
`disk-final.log`; the retained sanitizer node logs were separately scanned for
address, undefined-behavior and leak diagnostics.

From the repository root:

```bash
python3 scripts/verify-local-followup.py benchmarks/published/local-followup-20261022
```

The verifier checks every published file and source archive, reconstructs latency
and operation-mix statistics, replays every bounded per-key model, and checks the
cross-epoch durability sequence. Verification reads retained evidence only.
