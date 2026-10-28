"""Offline accounting for yewukv-bench's bounded per-connection samples."""

import gzip
import json
from pathlib import Path


def read(path):
    if path.exists():
        return json.loads(path.read_text())
    with gzip.open(path.with_suffix(path.suffix + ".gz"), "rt") as f:
        return json.load(f)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def summarize(directory):
    directory = Path(directory)
    result = read(directory / "result.json")
    require(
        result["schema"] in [1, 2]
        and result["columns"]
        == [
            "latency_ns",
            "scheduled_offset_ns",
            "started_offset_ns",
            "completed_offset_ns",
            "put",
            "code",
        ],
        "unknown sample schema",
    )
    clients, rate = result["clients"], result["offered_rps"]
    duration = result["duration_ms"] * 1000000
    require(
        1 <= clients <= 128 and duration >= 100000000 and rate >= 0,
        "invalid configuration",
    )
    counts = dict(attempts=0, successes=0, failures=0, offered_slots=0, missed_slots=0)
    latency, scheduled_latency = [], []
    completed = duration
    codes = {}
    for client in range(clients):
        record = read(directory / f"client-{client}.json")
        require(record["client"] == client, "client identity differs")
        period = (1000000000 * clients + rate // 2) // rate if rate else 0
        offset = (1000000000 * client + rate // 2) // rate if rate else 0
        offered = (
            max(0, (duration - 1 - offset) // period + 1)
            if period
            else len(record["samples"])
        )
        if rate and result["schema"] == 2:
            total = (duration * rate + 999999999) // 1000000000
            offered = max(0, (total - 1 - client) // clients + 1)
        require(record["offered_slots"] == offered, "offered schedule differs")
        require(
            len(record["samples"]) <= (result["max_samples"] + clients - 1) // clients,
            "sample limit exceeded",
        )
        previous_end, previous_slot = -1, -1
        for row in record["samples"]:
            require(
                len(row) == 6 and all(type(v) is int for v in row), "invalid sample"
            )
            elapsed, due, start, end, put, code = row
            require(
                elapsed == end - start
                and elapsed >= 0
                and 0 <= due <= start < duration
                and start >= previous_end
                and put in [0, 1]
                and -2 <= code <= 7,
                "invalid interval or response",
            )
            if period and result["schema"] == 1:
                require(
                    due >= offset
                    and (due - offset) % period == 0
                    and due > previous_slot,
                    "paced calendar differs",
                )
            elif period:
                index = (due * rate + 999999999) // 1000000000
                require(
                    index % clients == client
                    and due == index * 1000000000 // rate
                    and index < total
                    and due > previous_slot,
                    "exact paced calendar differs",
                )
            previous_end, previous_slot = end, due
            completed = max(completed, end)
            counts["attempts"] += 1
            counts["successes" if code == 0 else "failures"] += 1
            codes[str(code)] = codes.get(str(code), 0) + 1
            if code == 0:
                latency.append(elapsed)
                scheduled_latency.append(end - due)
        require(len(record["samples"]) <= offered, "sent more than offered")
        counts["offered_slots"] += offered
        counts["missed_slots"] += offered - len(record["samples"])
    require(
        all(result[k] == v for k, v in counts.items())
        and result["elapsed_ns"] == completed,
        "summary accounting differs",
    )
    require(
        not result["valid"]
        or (
            not result["setup_failed"]
            and not result["cancelled"]
            and not result["sample_limit_reached"]
            and counts["failures"] == 0
        ),
        "failed run labeled valid",
    )

    def quantile(values, n):
        return (
            sorted(values)[max(0, (len(values) * n + 999) // 1000 - 1)] / 1e6
            if values
            else None
        )

    return dict(
        **counts,
        valid=result["valid"],
        codes=codes,
        successful_ops_s=counts["successes"] / (completed / 1e9),
        p99_ms=quantile(latency, 990),
        p999_ms=quantile(latency, 999),
        max_ms=max(latency) / 1e6 if latency else None,
        scheduled_p99_ms=quantile(scheduled_latency, 990),
    )


def final_values(directory):
    """Reconstruct the known successful run's final keys for independent RPC reads."""
    directory = Path(directory)
    require(summarize(directory)["valid"], "failed run has unknown final values")
    result = read(directory / "result.json")
    size = result["value_size"]
    expected = {}
    for client in range(result["clients"]):
        values = [bytes([ord("a") + (client + key) % 26]) * size for key in range(8)]
        random = 0x9E3779B9 + client
        for sequence, row in enumerate(
            read(directory / f"client-{client}.json")["samples"], 1
        ):
            random ^= (random << 13) & (2**64 - 1)
            random ^= random >> 7
            random ^= (random << 17) & (2**64 - 1)
            require(
                row[4] == int(random % 100 < result["write_percent"]),
                "operation mix differs",
            )
            if row[4]:
                value = bytearray([ord("a") + sequence % 26] * size)
                value[: min(size, 8)] = sequence.to_bytes(8, "little")[: min(size, 8)]
                if size > 8:
                    value[8] = client
                values[(random // 100) % 8] = bytes(value)
        for key, value in enumerate(values):
            expected[f"{result['prefix']}-{client}-{key}"] = value
    return expected
