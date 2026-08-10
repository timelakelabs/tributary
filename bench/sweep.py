#!/usr/bin/env python3
"""L3 concurrency sweep: how many batches in flight, and where the time goes.

The second half matters as much as the first. The roadmap gates L6 (the
Arrow Flight wire) on a measured breakdown: if encoding plus server-side
parsing is a small share of the ship path, a faster wire is not the
bottleneck and building one would be optimising the wrong thing.

  sweep.py --lines 1000000 --levels 1,2,4,8
"""
import argparse
import json
import re
import subprocess
import time

CONFIG = "bench/l3.toml"


def set_config(inflight: int, table: str) -> None:
    cfg = open(CONFIG).read()
    cfg = re.sub(r"^table = .*$", f'table = "{table}"', cfg, flags=re.M)
    cfg = re.sub(r"^max_inflight = \d+\n", "", cfg, flags=re.M)
    cfg = cfg.replace(
        "queue_max_bytes = 536870912",
        f"queue_max_bytes = 536870912\nmax_inflight = {inflight}",
    )
    open(CONFIG, "w").write(cfg)


def run(inflight: int, table: str, lines: int):
    set_config(inflight, table)
    subprocess.run(["rm", "-rf", "/tmp/tributary-state"], check=False)
    subprocess.run(["mkdir", "-p", "/tmp/tributary-state"], check=False)
    t0 = time.time()
    out = subprocess.run(
        ["./target/release/tributary", "--config", CONFIG,
         "--state-dir", "/tmp/tributary-state", "--once"],
        capture_output=True, text=True,
    )
    elapsed = time.time() - t0
    tail = out.stdout.strip().splitlines()
    if not tail:
        print(f"  inflight={inflight}: no output\n{out.stderr[-400:]}")
        return None
    d = json.loads(tail[-1])
    rate = lines / elapsed
    read_ms, ship_ms, reqs = d["read_ms"], d["ship_ms"], d["requests"]
    # ship_ms is summed across concurrent tasks, so it exceeds wall clock
    # once inflight > 1 — that is the point of the pipeline.
    print(
        f"  inflight={inflight:<2} {elapsed:5.2f}s  {rate:>9,.0f} lines/s   "
        f"read {read_ms:>5}ms   ship {ship_ms:>6}ms (summed)   "
        f"requests {reqs:>4}   read share {read_ms / (read_ms + ship_ms) * 100:4.1f}%"
    )
    return {"inflight": inflight, "elapsed_s": round(elapsed, 2),
            "lines_per_s": round(rate), "read_ms": read_ms,
            "ship_ms": ship_ms, "requests": reqs, "agent": d}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lines", type=int, default=1_000_000)
    ap.add_argument("--levels", default="1,2,4,8")
    args = ap.parse_args()

    subprocess.run(["rm", "-rf", "/tmp/tributary-corpus"], check=False)
    subprocess.run(["mkdir", "-p", "/tmp/tributary-corpus"], check=False)
    subprocess.run(
        ["python3", "bench/gen.py", "--out", "/tmp/tributary-corpus/app.log",
         "--lines", str(args.lines), "--rate", "100000"],
        capture_output=True, check=True,
    )
    print(f"=== L3 concurrency sweep, {args.lines:,} lines per level:")
    results = []
    for i, level in enumerate(int(x) for x in args.levels.split(",")):
        r = run(level, f"l3_sweep{level}", args.lines)
        if r:
            results.append(r)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
