#!/usr/bin/env python
"""Assertion half of the metrics collector drill (#25).

Reads each measurement back from a TimeLakeDB node via /api/sql and checks it
carries Telegraf's field and tag NAMES plus the drill's global_tags /
static_fields. Split from the bash orchestrator so the JSON checks are real
Python, not brittle shell.

    python bench/metrics_collector_assert.py <host> <port> <db>
"""
import json
import sys
import urllib.request

HOST, PORT, DB = sys.argv[1], sys.argv[2], sys.argv[3]

# Telegraf's names, per measurement: tags that must be columns, fields that
# must be columns. Renaming any of these is what blanks a migrated dashboard.
EXPECT = {
    "cpu": (["cpu", "host"], ["usage_idle", "usage_active"]),
    "mem": (["host"], ["total", "available", "used", "free", "used_percent", "available_percent"]),
    "disk": (["device", "path", "fstype", "host"], ["total", "free", "used", "used_percent"]),
    "net": (["interface", "host"], ["bytes_recv", "bytes_sent", "packets_recv", "packets_sent", "err_in", "err_out"]),
    "system": (["host"], ["load1", "load5", "load15", "n_cpus", "uptime"]),
    "swap": (["host"], ["total", "used", "free", "used_percent"]),
}

ok = True


def sql(q):
    body = json.dumps({"db": DB, "sql": q}).encode()
    req = urllib.request.Request(
        f"http://{HOST}:{PORT}/api/sql", data=body,
        headers={"content-type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def check(label, cond, detail=""):
    global ok
    ok = ok and bool(cond)
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}{('  ' + detail) if detail else ''}")


for m, (tags, fields) in EXPECT.items():
    try:
        rows = sql(f"SELECT * FROM {m} LIMIT 1")
    except Exception as e:  # noqa: BLE001 — a missing table is a drill failure, report it
        check(f"{m}: table queryable", False, repr(e))
        continue
    check(f"{m}: table exists with a row", len(rows) >= 1, f"got {len(rows)} rows")
    if not rows:
        continue
    cols = set(rows[0].keys())
    missing_f = [f for f in fields if f not in cols]
    missing_t = [t for t in tags if t not in cols]
    check(f"{m}: all Telegraf fields present", not missing_f, f"missing {missing_f}" if missing_f else "")
    check(f"{m}: all Telegraf tags present", not missing_t, f"missing {missing_t}" if missing_t else "")
    check(f"{m}: global tag region=us-east", rows[0].get("region") == "us-east", str(rows[0].get("region")))
    check(f"{m}: static field deployment=prod", rows[0].get("deployment") == "prod", str(rows[0].get("deployment")))

print()
print(f"=== {'PASS' if ok else 'FAIL'}: metrics land with Telegraf schema + global_tags + static_fields ===")
sys.exit(0 if ok else 1)
