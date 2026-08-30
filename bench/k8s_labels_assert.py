#!/usr/bin/env python3
"""Pod-label allowlist check for the k8s labels drill (#8 phase 4, #66).

Over the DATA table's delivered line protocol, assert:

  - distinct series == the expected bounded count (labels did not inflate it),
  - every ALLOWLISTED label key is present as a tag (enrichment landed),
  - every FORBIDDEN label key is absent — a label the operator did not name
    (the unbounded `pod-template-hash`) never became a tag,
  - zero tag values are 64-hex container ids (the phase-3 guarantee still holds).

Usage:
  k8s_labels_assert.py <recv.lp> <data-table> <expected-series> <present-keys> <forbidden-keys>
where present/forbidden keys are comma-separated (forbidden may be empty).
"""
import re
import sys

HEXID = re.compile(r"^[0-9a-f]{64}$")


def measurement(line):
    return re.split(r"[ ,]", line, maxsplit=1)[0]


def tags(line):
    head = line.split(" ", 1)[0]
    out = {}
    for p in head.split(",")[1:]:
        if "=" in p:
            k, v = p.split("=", 1)
            out[k] = v
    return out


def main():
    recv, table = sys.argv[1], sys.argv[2]
    expect = int(sys.argv[3])
    present = [k for k in sys.argv[4].split(",") if k]
    forbidden = [k for k in sys.argv[5].split(",") if k] if len(sys.argv) > 5 else []

    lines = [l for l in open(recv, encoding="utf-8", errors="replace").read().splitlines() if l.strip()]
    series = set()
    key_values = {}   # tag key -> set of values seen on data rows
    id_tags = 0
    for ln in lines:
        if measurement(ln) != table:
            continue
        t = tags(ln)
        series.add(frozenset(t.items()))
        for k, v in t.items():
            key_values.setdefault(k, set()).add(v)
            if HEXID.match(v):
                id_tags += 1

    fail = 0
    print(f"  distinct series (full tag-set) = {len(series)} (expected {expect})")
    if len(series) != expect:
        fail = 1
    for k in present:
        vals = key_values.get(k)
        ok = bool(vals)
        print(f"  [{'PASS' if ok else 'FAIL'}] allowlisted label '{k}' is a tag: {sorted(vals) if vals else 'ABSENT'}")
        fail |= 0 if ok else 1
    for k in forbidden:
        n = len(key_values.get(k, set()))
        ok = n == 0
        print(f"  [{'PASS' if ok else 'FAIL'}] non-allowlisted label '{k}' never became a tag (values seen={n})")
        fail |= 0 if ok else 1
    ok = id_tags == 0
    print(f"  [{'PASS' if ok else 'FAIL'}] no 64-hex id tags (values seen={id_tags})")
    fail |= 0 if ok else 1

    print("  RESULT:", "PASS" if fail == 0 else "FAIL")
    sys.exit(fail)


if __name__ == "__main__":
    main()
