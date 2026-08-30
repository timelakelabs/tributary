#!/usr/bin/env python3
"""Cardinality check for the k8s glob drill (#8 phase 3, #65).

The whole reason the tag allowlist exists: distinct SERIES (tag-sets) must track
the BOUNDED identity (pod / namespace / container / node), not the number of
pods or container restarts. The container-id is unbounded and sits right in the
log path, so the failure mode is stamping it — which would make every restart a
new dead series. This asserts the opposite over the DATA table:

  - distinct series == the expected bounded count, NOT the file count,
  - distinct `stream` tags == that same bounded count (the id is stripped),
  - ZERO tag values are 64-hex container ids (nothing leaked the id into a tag),
  - the `node` tag is present and constant (the Downward API stamp landed).

It also reports the watermark table's series count, which must be bounded too —
the completeness rows are tagged by the same stripped `stream` label, so an
id-laden label would blow THAT table up as well.

Usage: k8s_cardinality_assert.py <recv.lp> <data-table> <expected-series> <file-count> <node>
"""
import re
import sys

HEXID = re.compile(r"^[0-9a-f]{64}$")


def measurement(line):
    # `<measurement>,tag=v,... fields ts` or `<measurement> fields ts`
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
    recv, table, expect = sys.argv[1], sys.argv[2], int(sys.argv[3])
    files, node = int(sys.argv[4]), sys.argv[5]
    lines = [l for l in open(recv, encoding="utf-8", errors="replace").read().splitlines() if l.strip()]

    series, pnc, streams, nodes = set(), set(), set(), set()
    wm_streams = set()
    id_tags = 0
    for ln in lines:
        m = measurement(ln)
        t = tags(ln)
        id_tags += sum(1 for v in t.values() if HEXID.match(v))
        if m == table:
            series.add(frozenset(t.items()))
            pnc.add((t.get("pod"), t.get("namespace"), t.get("container")))
            streams.add(t.get("stream"))
            nodes.add(t.get("node"))
        else:  # the watermark (or any meta) table
            wm_streams.add(t.get("stream"))

    print(f"  files on disk (id churn)       = {files}")
    print(f"  distinct series (full tag-set) = {len(series)}")
    print(f"  distinct (pod,ns,container)    = {len(pnc)}")
    print(f"  distinct stream tag values     = {len(streams)}  {sorted(x for x in streams if x)}")
    print(f"  distinct node tag values       = {sorted(x for x in nodes if x)}")
    print(f"  watermark-table series         = {len(wm_streams)} (bounded too, same stripped label)")
    print(f"  tag values that are 64-hex ids = {id_tags}")

    ok = (
        len(series) == expect
        and len(pnc) == expect
        and len(streams) == expect
        and id_tags == 0
        and nodes == {node}
        and len(wm_streams) <= expect
    )
    print(
        f"  [{'PASS' if ok else 'FAIL'}] series={len(series)} tracks bounded identity "
        f"(expected {expect}), not the {files} files; id-tags={id_tags}; nodes={sorted(x for x in nodes if x)}"
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
