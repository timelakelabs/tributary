#!/usr/bin/env python3
"""Generate a log corpus with a known, checkable shape.

Every line carries a monotonic `idx`, which is what turns the gate from
"did roughly the right number of rows arrive" into "is the set of lines
exactly 0..N-1" — the assertion that catches primary-key collision,
poison-batch loss and checkpoint bugs at once (DESIGN.md §6.1).

  gen.py --out corpus/app.log --lines 1000000 --rate 10000
"""
import argparse
import json
import os
import random
import time

LEVELS = ["debug", "info", "warn", "error"]
SERVICES = ["api", "worker", "scheduler"]
MESSAGES = [
    "request completed",
    "cache miss",
    "retrying upstream call",
    "disk usage above threshold",
    "connection reset by peer",
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--lines", type=int, default=100_000)
    ap.add_argument(
        "--rate",
        type=int,
        default=0,
        help="lines per second of SIMULATED time; 0 means as fast as possible "
        "with timestamps still advancing at this rate",
    )
    ap.add_argument(
        "--realtime",
        action="store_true",
        help="also sleep so the file grows at --rate in wall-clock time",
    )
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument(
        "--binary-at",
        type=int,
        default=-1,
        help="inject an invalid UTF-8 byte sequence at this line index",
    )
    ap.add_argument(
        "--malformed-at",
        type=int,
        default=-1,
        help="inject an unparseable line at this line index",
    )
    args = ap.parse_args()

    rng = random.Random(args.seed)
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    # Millisecond-resolution timestamps, which is the realistic and
    # dangerous case: at --rate 10000 that is ten lines per millisecond,
    # all sharing a primary key unless the agent disambiguates.
    rate = args.rate if args.rate > 0 else 10_000
    start_ms = int(time.time() * 1000) - (args.lines * 1000 // rate) - 1000

    written = 0
    t0 = time.time()
    with open(args.out, "wb") as f:
        for i in range(args.lines):
            ts_ms = start_ms + (i * 1000) // rate
            rec = {
                "ts": ts_ms,
                "level": rng.choice(LEVELS),
                "service": rng.choice(SERVICES),
                "idx": i,
                "message": rng.choice(MESSAGES),
            }
            line = json.dumps(rec).encode("utf-8")
            if i == args.binary_at:
                line = line[:-1] + b"\xff\xfe}"
            if i == args.malformed_at:
                line = b"this is not json at all {{{"
            f.write(line + b"\n")
            written += 1
            if args.realtime and written % rate == 0:
                target = t0 + written / rate
                now = time.time()
                if target > now:
                    time.sleep(target - now)

    elapsed = time.time() - t0
    print(
        json.dumps(
            {
                "path": args.out,
                "lines": written,
                "first_idx": 0,
                "last_idx": written - 1,
                "simulated_rate": rate,
                "bytes": os.path.getsize(args.out),
                "elapsed_s": round(elapsed, 2),
            }
        )
    )


if __name__ == "__main__":
    main()
