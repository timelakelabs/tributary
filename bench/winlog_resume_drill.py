#!/usr/bin/env python3
"""Windows Event Log crash-exact resume drill (#11, acceptance criterion 2).

Proves that a winlog source, stopped and restarted, resumes from its saved
**bookmark** with NO GAP and NO DUPE — the bookmark (an opaque XML token), not
an EventRecordID offset, is the checkpoint.

Unlike journald there is no Linux-container trick for the Windows Event Log
API: it only exists on Windows. But this IS a Windows host with a real System
channel, so the drill runs the cross-compiled `tributary.exe` here, three
times, using the binary's `--winlog-dump` mode (which drives the REAL reader
and persists the bookmark through the SAME `Checkpoint` path production uses):

  1. reference  — fresh state, read the oldest 2N events in one shot   -> R
  2. run 1      — fresh state, read the oldest N events, save bookmark -> A
  3. run 2      — SAME state, resume from the bookmark, read N more     -> C

Then assert A ++ C == R exactly: run 2 picks up precisely where run 1 stopped,
every event once, in order — a split read at a checkpoint reproduces the
un-split read. Reading OLDEST-first makes the window stable: new events append
at the tail, far from the oldest 2N, so all three reads see the same set.

    python bench/winlog_resume_drill.py --exe path\\to\\tributary.exe

Exits 0 on PASS, 1 on FAIL. Deliberately does not depend on writing new events
(that needs elevation); it reads what is already there.
"""
import argparse
import os
import subprocess
import sys
import tempfile
import time


def dump(exe, channel, state_dir, limit, stream="winsys"):
    """Run one --winlog-dump; return (record_ids, resuming_flag).

    stdout is one line per event: `RecordID<TAB>time_created_ns<TAB>mapped`.
    stderr carries the `resuming=` banner.
    """
    p = subprocess.run(
        [exe, "--winlog-dump", "--channel", channel, "--limit", str(limit),
         "--state-dir", state_dir, "--stream", stream],
        capture_output=True, text=True,
    )
    if p.returncode != 0:
        print(f"  dump exited {p.returncode}; stderr:\n{p.stderr}")
        return None, None
    rids, mapped_all = [], True
    for line in p.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) >= 3:
            rids.append(parts[0])
            mapped_all = mapped_all and parts[2].strip() == "true"
    resuming = "resuming=true" in p.stderr
    return (rids, mapped_all), resuming


def check(label, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}{('  ' + detail) if detail else ''}")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--exe", required=True, help="path to the cross-built tributary.exe")
    ap.add_argument("--channel", default="System")
    ap.add_argument("--limit", type=int, default=20, help="N; the reference reads 2N")
    args = ap.parse_args()

    if not os.path.exists(args.exe):
        print(f"exe not found: {args.exe}")
        sys.exit(2)

    n = args.limit
    tmp = tempfile.mkdtemp(prefix="winlog-drill-")
    print(f"=== Windows Event Log crash-exact resume drill "
          f"({time.strftime('%Y-%m-%dT%H:%M:%S')}) ===")
    print(f"exe={args.exe}")
    print(f"channel={args.channel}  N={n}  workdir={tmp}")
    ok = True

    # --- 1. Reference: the oldest 2N events, read in ONE shot. ---
    ref_dir = os.path.join(tmp, "ref")
    (R, ref_mapped), _ = dump(args.exe, args.channel, ref_dir, 2 * n, "ref")
    if R is None:
        print("reference read failed — cannot run the drill")
        sys.exit(1)
    # If the channel holds fewer than 2N events, shrink N so the split is even.
    if len(R) < 2 * n:
        n = len(R) // 2
        R = R[: 2 * n]
        print(f"-- channel has only {len(R)} events available; N reduced to {n} --")
    if n < 1:
        print("not enough events in the channel to drill a resume")
        sys.exit(1)
    print(f"\n-- reference: {len(R)} events (oldest first), RecordIDs "
          f"{R[0]}..{R[-1]} --")
    ok &= check("every reference event mapped to a line", ref_mapped)
    ok &= check("reference RecordIDs are unique", len(set(R)) == len(R))

    # --- 2. Run 1: fresh state, read the oldest N, save the bookmark. ---
    split_dir = os.path.join(tmp, "split")
    (A, _), resuming1 = dump(args.exe, args.channel, split_dir, n, "winsys")
    print(f"\n-- run 1: read {len(A)} events {A[0]}..{A[-1]}, bookmark saved --")
    ok &= check("run 1 started fresh (no bookmark yet)", resuming1 is False,
                f"resuming={resuming1}")
    ok &= check("run 1 read exactly N", len(A) == n, f"got {len(A)}, want {n}")

    # --- 3. Run 2: SAME state, resume from the bookmark, read N more. ---
    (C, _), resuming2 = dump(args.exe, args.channel, split_dir, n, "winsys")
    print(f"-- run 2: resumed, read {len(C)} events "
          f"{C[0] if C else '-'}..{C[-1] if C else '-'} --")
    ok &= check("run 2 resumed from the saved bookmark", resuming2 is True,
                f"resuming={resuming2}")
    ok &= check("run 2 read exactly N", len(C) == n, f"got {len(C)}, want {n}")

    # --- 4. The verdict: no gap, no dupe across the restart. ---
    ok &= check("no event read by BOTH runs (no dupe)",
                len(set(A) & set(C)) == 0,
                f"overlap={sorted(set(A) & set(C))[:5]}")
    ok &= check("run 2 begins exactly where run 1 stopped (no gap)",
                A + C == R,
                "split read != one-shot read" if A + C != R else "")
    ok &= check("union covers the reference set exactly",
                set(A) | set(C) == set(R))

    print(f"\n=== {'PASS' if ok else 'FAIL'}: the bookmark is the checkpoint; "
          f"resume is crash-exact ===")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
