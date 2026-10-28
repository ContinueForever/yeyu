"""Small exhaustive per-key register checker for recorded concurrent histories.

An unavailable write is optional and has no completion bound: a timeout does
not prove absence. Definite pre-append rejections are omitted by the caller.
This is a bounded test oracle, not a general distributed-systems proof.
"""

from functools import lru_cache
import math


def check(history):
    for op in history:
        if op.get("kind") not in ("put", "get", "del") or "key" not in op:
            raise ValueError("invalid register operation")
        begin, end = op.get("start"), op.get("end")
        if (not isinstance(begin, (int, float)) or not math.isfinite(begin) or
                not isinstance(end, (int, float)) or end <= begin or math.isnan(end)):
            raise ValueError("invalid operation interval")
        if op.get("optional"):
            if op["kind"] == "get" or end != math.inf:
                raise ValueError("unknown writes require an unbounded completion time")
        elif not math.isfinite(end):
            raise ValueError("completed operations require a finite completion time")
        if (op["kind"] == "put" and "value" not in op) or (op["kind"] == "get" and "result" not in op):
            raise ValueError("missing register value")
    keys = {op["key"] for op in history}
    for key in keys:
        operations = [op for op in history if op["key"] == key]
        if len(operations) > 22:
            raise ValueError("bounded checker supports at most 22 operations per key")
        predecessors = [sum(1 << j for j, other in enumerate(operations)
                            if j != i and other["end"] <= op["start"])
                        for i, op in enumerate(operations)]
        complete = (1 << len(operations)) - 1

        @lru_cache(maxsize=None)
        def search(mask, value):
            if mask == complete:
                return True
            for i, op in enumerate(operations):
                bit = 1 << i
                if mask & bit or predecessors[i] & ~mask:
                    continue
                if op.get("optional") and search(mask | bit, value):
                    return True
                if op["kind"] == "get":
                    if op["result"] == value and search(mask | bit, value):
                        return True
                elif search(mask | bit, op["value"] if op["kind"] == "put" else None):
                    return True
            return False

        if not search(0, None):
            return False
    return True
