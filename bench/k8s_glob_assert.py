#!/usr/bin/env python3
"""Per-pod completeness AND CRI-enrichment check for the k8s glob drill (#64).

For each spec `pod=ns/container:N`, over the delivered line protocol:

  - every record tagged `pod=<pod>` also carries `namespace=<ns>` and
    `container=<ctr>` (phase 1 enrichment landed, and landed the SAME on every
    line from that file), and
  - the distinct `idx` set for that pod is exactly `0..N-1` (nothing lost —
    at-least-once may duplicate, which is why it's the DISTINCT set).

Usage:  k8s_glob_assert.py <recv.lp> pod=ns/container:N [pod=ns/container:N ...]
"""
import re
import sys


def tags(line):
    head = line.split(" ", 1)[0]  # measurement,tag=v,tag=v
    out = {}
    for p in head.split(",")[1:]:
        if "=" in p:
            k, v = p.split("=", 1)
            out[k] = v
    return out


def main():
    recv = sys.argv[1]
    specs = sys.argv[2:]
    lines = open(recv, encoding="utf-8", errors="replace").read().splitlines()
    ok = True
    for spec in specs:
        pod, rest = spec.split("=", 1)
        nsctr, n = rest.rsplit(":", 1)
        n = int(n)
        ns, ctr = nsctr.split("/", 1)
        idxs, bad_enrich, count = set(), 0, 0
        for ln in lines:
            t = tags(ln)
            if t.get("pod") != pod:
                continue
            count += 1
            if t.get("namespace") != ns or t.get("container") != ctr:
                bad_enrich += 1
            m = re.search(r"idx=(\d+)i", ln)
            if m:
                idxs.add(int(m.group(1)))
        want = set(range(n))
        missing, extra = want - idxs, idxs - want
        good = idxs == want and bad_enrich == 0 and count > 0
        ok = ok and good
        print(
            f"  [{'PASS' if good else 'FAIL'}] pod={pod} ns={ns} ctr={ctr}: "
            f"records={count} distinct={len(idxs)}/{n} enrich_bad={bad_enrich} "
            f"missing={len(missing)} extra={len(extra)}"
        )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
