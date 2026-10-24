"""Bounded per-key oracle for fresh, already revisioned registers.

Checks observed values, CAS outcomes and monotonic key versions. Unknown writes
may be omitted or take effect after timeout. Unobserved versions are existential
intervals, not guessed log indexes. This does not model cross-key transactions,
legacy migration, or every internal Raft log position.
"""

from functools import lru_cache
import math

MAX_REVISION = 2**64 - 1


def revision(value):
    return type(value) is int and 0 <= value <= MAX_REVISION


def check(history):
    successful_versions = set()
    for op in history:
        kind = op.get("kind")
        if kind not in ["put", "get", "del", "cas"] or not isinstance(
            op.get("key"), str
        ):
            raise ValueError("invalid revision operation")
        start, end = op.get("start"), op.get("end")
        if (
            type(start) not in [int, float]
            or not math.isfinite(start)
            or type(end) not in [int, float]
            or math.isnan(end)
            or end <= start
        ):
            raise ValueError("invalid operation interval")
        if kind in ["put", "cas"] and not isinstance(op.get("value"), str):
            raise ValueError("missing mutation value")
        if kind == "cas" and not revision(op.get("expected_revision")):
            raise ValueError("invalid expected revision")
        if op.get("optional"):
            if kind == "get" or end != math.inf or op.get("code") != 6:
                raise ValueError("unknown mutations require unbounded completion")
            continue
        if not math.isfinite(end) or not revision(op.get("revision")):
            raise ValueError("completed response requires a revision")
        code, observed = op.get("code"), op.get("result")
        if kind == "get":
            valid = (
                code == 0 and op["revision"] > 0 and isinstance(observed, str)
            ) or (code == 1 and op["revision"] == 0 and observed is None)
        elif kind == "cas" and code == 7:
            valid = (op["revision"] > 0 and isinstance(observed, str)) or (
                op["revision"] == 0 and observed is None
            )
        else:
            valid = code == 0 and op["revision"] > 0
            if valid:
                if op["revision"] in successful_versions:
                    return False
                successful_versions.add(op["revision"])
        if not valid:
            raise ValueError("invalid revision response")

    for key in {op["key"] for op in history}:
        ops = [op for op in history if op["key"] == key]
        if len(ops) > 22:
            raise ValueError("bounded oracle supports at most 22 operations per key")
        predecessors = [
            sum(
                1 << j
                for j, other in enumerate(ops)
                if i != j and other["end"] <= op["start"]
            )
            for i, op in enumerate(ops)
        ]
        complete = (1 << len(ops)) - 1

        @lru_cache(maxsize=None)
        def search(mask, value, low, high, floor):
            if mask == complete:
                return True
            for i, op in enumerate(ops):
                bit = 1 << i
                if mask & bit or predecessors[i] & ~mask:
                    continue
                next_mask = mask | bit
                kind = op["kind"]
                if op.get("optional"):
                    if search(next_mask, value, low, high, floor):
                        return True
                    expected = op.get("expected_revision", 0)
                    if kind == "cas" and not (
                        (value is None and expected == 0)
                        or (
                            value is not None
                            and expected > 0
                            and low <= expected <= high
                        )
                    ):
                        continue
                    minimum = max(floor, expected if kind == "cas" else 0) + 1
                    if minimum > MAX_REVISION:
                        continue
                    if kind == "del":
                        state = (None, 0, 0, minimum)
                    else:
                        state = (op["value"], minimum, MAX_REVISION, minimum)
                elif kind == "get" or op["code"] == 7:
                    observed = op["revision"]
                    matches = (
                        value is None and observed == 0 and op.get("result") is None
                    ) or (
                        value is not None
                        and op.get("result") == value
                        and low <= observed <= high
                    )
                    if not matches:
                        continue
                    if kind == "cas" and op["expected_revision"] == observed:
                        continue
                    state = (value, observed, observed, max(floor, observed))
                else:
                    observed = op["revision"]
                    if observed <= floor:
                        continue
                    if kind == "cas":
                        expected = op["expected_revision"]
                        if not (
                            (value is None and expected == 0)
                            or (
                                value is not None
                                and expected > 0
                                and low <= expected <= high
                                and expected < observed
                            )
                        ):
                            continue
                    state = (
                        (None, 0, 0, observed)
                        if kind == "del"
                        else (op["value"], observed, observed, observed)
                    )
                if search(next_mask, *state):
                    return True
            return False

        if not search(0, None, 0, 0, 0):
            return False
    return True
