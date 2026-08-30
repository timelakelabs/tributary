#!/usr/bin/env python3
"""CRI text-parser checks for the k8s CRI drill (#71).

Over the delivered line protocol, assert that `parser = "cri"`:

  - stamped each record at the LOG's OWN time (the corpus is dated 2026-01-01;
    if the parser fell back to ingestion time the timestamps would be ~now),
  - pulled `stdout`/`stderr` into the `stream` tag (both appear),
  - reassembled a >16 KB `P`/`F` split into ONE record (the BIGSTART/BIGEND
    markers land in the same record, not three), and
  - still enriched pod/namespace/container from the CRI path (phase 1).

Usage: k8s_cri_assert.py <recv.lp> <data-table> <pod> <namespace> <container>
"""
import datetime
import sys

JAN1 = int(datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc).timestamp() * 1e9)
JAN2 = int(datetime.datetime(2026, 1, 2, tzinfo=datetime.timezone.utc).timestamp() * 1e9)


def tags(line):
    head = line.split(" ", 1)[0]
    out = {}
    for p in head.split(",")[1:]:
        if "=" in p:
            k, v = p.split("=", 1)
            out[k] = v
    return out


def main():
    recv, table, pod, ns, ctr = sys.argv[1:6]
    lines = [l for l in open(recv, encoding="utf-8", errors="replace").read().splitlines() if l.strip()]
    data = [l for l in lines if l.split(",", 1)[0].split(" ")[0] == table]

    fail = 0
    # 1. every record is stamped at the log's own time, not ingestion time.
    out_of_window = 0
    for l in data:
        ts = int(l.rsplit(" ", 1)[1])
        if not (JAN1 <= ts < JAN2):
            out_of_window += 1
    ok = data and out_of_window == 0
    print(f"  [{'PASS' if ok else 'FAIL'}] all {len(data)} records stamped at event time (2026-01-01), not ingest ({out_of_window} out of window)")
    fail |= 0 if ok else 1

    # 2. stdout AND stderr both landed as the stream tag.
    streams = {tags(l).get("stream") for l in data}
    ok = {"stdout", "stderr"} <= streams
    print(f"  [{'PASS' if ok else 'FAIL'}] stream tag carries stdout and stderr: {sorted(x for x in streams if x)}")
    fail |= 0 if ok else 1

    # 3. the split line reassembled to exactly one record.
    starts = [l for l in data if "BIGSTART" in l]
    ok = len(starts) == 1 and "BIGEND" in starts[0]
    print(f"  [{'PASS' if ok else 'FAIL'}] the >16KB P/F split reassembled to ONE record (BIGSTART records={len(starts)}, has BIGEND={bool(starts) and 'BIGEND' in starts[0]})")
    fail |= 0 if ok else 1

    # 4. enrichment still works through the CRI parser.
    enr = all(tags(l).get("pod") == pod and tags(l).get("namespace") == ns and tags(l).get("container") == ctr for l in data)
    print(f"  [{'PASS' if enr else 'FAIL'}] every record enriched pod={pod} ns={ns} container={ctr}")
    fail |= 0 if enr else 1

    print("  RESULT:", "PASS" if fail == 0 else "FAIL")
    sys.exit(fail)


if __name__ == "__main__":
    main()
