#!/usr/bin/env python3
"""The gate: lines written == rows stored, exactly.

Counting alone would pass a run that lost 5,000 lines and duplicated
5,000 others, so this checks the *set* of indices instead:

    n == distinct == expected  and  min == 0  and  max == expected-1

which can only hold if every generated line arrived exactly once.

  verify.py --url http://localhost:1963 --db logs --table app_logs --expect 1000000
"""
import argparse
import json
import sys
import urllib.request


def sql(url: str, db: str, query: str):
    body = json.dumps({"db": db, "sql": query}).encode()
    req = urllib.request.Request(
        f"{url.rstrip('/')}/api/sql",
        data=body,
        headers={"content-type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:1963")
    ap.add_argument("--db", default="logs")
    ap.add_argument("--table", default="app_logs")
    ap.add_argument("--stream", default="app")
    ap.add_argument("--expect", type=int, required=True)
    args = ap.parse_args()

    rows = sql(
        args.url,
        args.db,
        f"SELECT COUNT(*) AS n, COUNT(DISTINCT idx) AS distinct_idx, "
        f"MIN(idx) AS lo, MAX(idx) AS hi FROM {args.table} "
        f"WHERE stream = '{args.stream}'",
    )
    r = rows[0]
    n, distinct = int(r["n"] or 0), int(r["distinct_idx"] or 0)
    lo = int(r["lo"]) if r["lo"] is not None else -1
    hi = int(r["hi"]) if r["hi"] is not None else -1
    want = args.expect

    checks = {
        "rows == expected": (n, want, n == want),
        "distinct == expected": (distinct, want, distinct == want),
        "min idx == 0": (lo, 0, lo == 0),
        "max idx == expected-1": (hi, want - 1, hi == want - 1),
    }

    ok = all(c[2] for c in checks.values())
    print(json.dumps({"expected": want, "rows": n, "distinct": distinct,
                      "min": lo, "max": hi, "exact": ok}, indent=2))
    for name, (got, expect, passed) in checks.items():
        print(f"  {'PASS' if passed else 'FAIL'}  {name}: got {got}, want {expect}")

    if not ok and n < want:
        missing = want - distinct
        print(
            f"\n  {missing} lines are missing. If this is close to "
            f"(1 - 1/lines_per_tick) of the corpus, it is primary-key\n"
            f"  collision (DESIGN.md §1.1): the writes returned 204 and the "
            f"rows were silently overwritten.",
            file=sys.stderr,
        )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
