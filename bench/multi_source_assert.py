#!/usr/bin/env python3
"""Per-stream exactness gate for the multi-source drill (#54).

Reads a file of raw InfluxDB line protocol — every `/write` body the agent
delivered to the mock sink — and checks, for each stream, that the set of
`idx` values is exactly 0..N-1: complete, and by-set duplicate-free.

Duplicates in the *delivered bytes* are expected after a crash-resume: the
agent replays an un-acked batch, and a real TimeLakeDB collapses those by
primary key (the deterministic stamper regenerates identical timestamps —
DESIGN.md §3.2, and the L1 drill proves it against the real database). So
this reports total-vs-distinct rather than failing on a replay; the
completeness claim is about the distinct set, which is what "nothing lost"
means. `max == expected-1` per stream is also the cross-contamination guard:
no stream may carry another stream's indices.

  multi_source_assert.py received.lp alpha:6000 beta:4000
"""
import json
import re
import sys

# `stream` is a tag, so it sits in the first (comma-joined) section before the
# space that starts the fields; `idx` is an integer field, rendered `idx=<n>i`.
# Message values are quoted strings from a fixed vocabulary with no "idx=" in
# them, so a plain search for each token is unambiguous for this corpus.
STREAM = re.compile(rb"(?:^|,)stream=([A-Za-z0-9_]+)")
IDX = re.compile(rb"(?:^|[ ,])idx=(\d+)i")


def main() -> int:
    if len(sys.argv) < 3:
        print(
            "usage: multi_source_assert.py <received.lp> <stream:expect> ...",
            file=sys.stderr,
        )
        return 2
    path = sys.argv[1]
    want = {}
    for spec in sys.argv[2:]:
        name, n = spec.rsplit(":", 1)
        want[name] = int(n)

    totals = {name: 0 for name in want}
    sets = {name: set() for name in want}
    with open(path, "rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ms = STREAM.search(line)
            mi = IDX.search(line)
            if not ms or not mi:
                continue
            s = ms.group(1).decode()
            if s not in want:
                continue
            totals[s] += 1
            sets[s].add(int(mi.group(1)))

    ok = True
    report = {}
    for name, expect in want.items():
        st = sets[name]
        distinct = len(st)
        lo = min(st) if st else -1
        hi = max(st) if st else -1
        exact = distinct == expect and lo == 0 and hi == expect - 1
        ok = ok and exact
        report[name] = {
            "expect": expect,
            "distinct": distinct,
            "total_delivered": totals[name],
            "min": lo,
            "max": hi,
            "replayed": totals[name] - distinct,
            "exact": exact,
        }

    print(json.dumps(report, indent=2))
    for name, r in report.items():
        tag = "PASS" if r["exact"] else "FAIL"
        print(
            f"  [{tag}] stream {name}: distinct {r['distinct']}/{r['expect']}, "
            f"min {r['min']}, max {r['max']}, "
            f"replayed-then-deduped {r['replayed']}"
        )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
