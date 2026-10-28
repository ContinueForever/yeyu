#!/usr/bin/env python3
"""Export completed local follow-up evidence without mutable node directories.

Keeps raw latency samples, source archives, test logs and every fault attempt.
The destination must be new; nothing in the input directory is removed.
"""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import shutil


def read(path):
    return json.loads(path.read_text())


def copy_tree(source, destination):
    shutil.copytree(source, destination, ignore=shutil.ignore_patterns("node-1", "node-2", "node-3"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root, output = args.input.resolve(), args.output.resolve()
    for name in ("reads", "snapshots", "faults"):
        if read(root / name / "manifest.json")["status"] != "complete":
            parser.error(f"{name} did not complete")
    output.mkdir(parents=True, exist_ok=False)
    for name in ("reads", "snapshots", "acceptance"):
        copy_tree(root / name, output / name)
    compatibility = output / "acceptance/compatibility"
    compatibility.mkdir()
    shutil.copy2(root / "compatibility/result.json", compatibility / "result.json")
    for log in sorted((root / "compatibility/runtime").glob("node-*.log")):
        shutil.copy2(log, compatibility / log.name)
    baseline = output / "baseline"
    baseline.mkdir()
    for name in ("identity.json", "source.tar.gz", "source-manifest.json"):
        shutil.copy2(root / "baseline" / name, baseline / name)
    faults = output / "faults"
    faults.mkdir()
    for name in ("manifest.json", "summary.json", "source.tar.gz", "source-manifest.json", "progress.json"):
        shutil.copy2(root / "faults" / name, faults / name)
    summary = read(faults / "summary.json")
    with gzip.open(faults / "epochs.jsonl.gz", "wt") as stream:
        for epoch in range(summary["epochs"]):
            history = read(root / "faults" / f"epoch-{epoch:05d}.json")
            result = read(root / "faults" / f"epoch-{epoch:05d}-result.json")
            stream.write(json.dumps(dict(history=history, result=result), separators=(",", ":")) + "\n")
    for log in sorted((root / "faults/runtime").glob("node-*.log")):
        with log.open("rb") as source, gzip.open(faults / f"{log.name}.gz", "wb") as target:
            shutil.copyfileobj(source, target)
    for name in ("normal-complete.log", "asan-final.log", "asan-port-conflict.log", "disk-final.log",
                 "reads.log", "snapshots.log", "faults.log", "prior-evidence.log", "build-final.log", "build-asan.log"):
        shutil.copy2(root / name, output / "acceptance" / name)
    shutil.copy2(root / "reads-verification.log", output / "acceptance/reads-verification.log")
    shutil.copy2(root / "compatibility.log", output / "acceptance/compatibility.log")
    (output / "README.md").write_text("""# 2026-10-22 local follow-up evidence

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
""")
    files = {}
    for path in sorted(output.rglob("*")):
        if path.is_file():
            content = path.read_bytes()
            files[path.relative_to(output).as_posix()] = dict(bytes=len(content), sha256=hashlib.sha256(content).hexdigest())
    (output / "publication-manifest.json").write_text(json.dumps(dict(files=files), indent=2) + "\n")
    print(json.dumps(dict(published_files=len(files), output=str(output))))


if __name__ == "__main__":
    main()
