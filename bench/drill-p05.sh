#!/bin/sh
# P0-5 drill: Tributary presents its data-plane token to a TimeLakeDB node
# running TIMELAKE_DATA_AUTH=required.
#
# Proves, end to end against a real required-mode node:
#   1. with the correct token, every line ships (exact count);
#   2. with a WRONG token, nothing ships, nothing is dropped (the queue
#      holds it), and the failure is reported as auth — not transport;
#   3. with NO token, same as wrong;
#   4. the token never appears in Tributary's logs.
#
# Run from a container sharing the node's network, so the node is reachable
# by its compose hostname without a published-port penalty.
#
#   TOKEN=<write secret>  — the token Tributary presents (scope=write)
#   READ_TOKEN=<read secret> — this drill's own verification reads (required
#     mode gates reads too, and the write token deliberately cannot read)
#   NODE=http://timelakedb-da:1963
#
#   TOKEN=... READ_TOKEN=... sh drill-p05.sh
set -e
# Plain logs: with ANSI colour, tracing wraps every field so `key=value` is
# not a literal substring. The drill greps the agent's own log, so turn it off.
export NO_COLOR=1
BENCH=$(cd "$(dirname "$0")" && pwd)
BIN="$BENCH/../target/release/tributary"
NODE=${NODE:-http://timelakedb-da:1963}
LINES=${LINES:-20000}

WORK=/tmp/p05
CORPUS="$WORK/corpus"
rm -rf "$WORK"; mkdir -p "$CORPUS"
# Unique table names per run: the node persists across runs, so a fixed name
# would accumulate rows and break the exact-count check on a re-run.
RUN=$(date +%s)

pass=0; fail=0
chk() { if [ "$1" = "$2" ]; then echo "  PASS  $3"; pass=$((pass+1));
        else echo "  FAIL  $3 (got '$1' want '$2')"; fail=$((fail+1)); fi; }

# A JSON corpus matching the config below: ts (ms), message, idx, level.
gen() {
  python3 - "$1" "$LINES" <<'PY'
import json, sys, time
path, n = sys.argv[1], int(sys.argv[2])
base = int(time.time() * 1000) - n
with open(path, "w") as f:
    for i in range(n):
        f.write(json.dumps({"ts": base + i, "message": f"event {i}",
                            "idx": i, "level": "info"}) + "\n")
PY
}

cfg() {  # $1 = table
  cat > "$WORK/tributary.toml" <<EOF
[output]
url = "$NODE"
database = "logs"
batch_lines = 5000
gzip = true

[[source]]
name = "p05"
path = "$CORPUS/app.log"
table = "$1"
parser = "json"
timestamp = { field = "ts", format = "unix_ms", resolution = "ms" }
tags = ["level"]

[source.fields]
message = "string"
idx = "integer"
EOF
}

rows() {  # $1 = table — reads need a token too in required mode. A table
          # that was never written does not exist, so the query errors; that
          # is 0 rows for our purposes.
  curl -s -X POST "$NODE/api/sql" -H 'content-type: application/json' \
    -H "authorization: Bearer $READ_TOKEN" \
    -d "{\"db\":\"logs\",\"sql\":\"SELECT COUNT(*) AS n FROM $1\"}" 2>/dev/null \
    | python3 -c "import sys,json
try:
    d=json.load(sys.stdin); print(d[0]['n'] if isinstance(d,list) and d else 0)
except Exception:
    print(0)"
}

run() {  # $1 = table, $2 = state suffix ; token via TRIBUTARY_TOKEN in env
  rm -rf "$WORK/state-$2"; mkdir -p "$WORK/state-$2"
  "$BIN" --config "$WORK/tributary.toml" --state-dir "$WORK/state-$2" --once \
    > "$WORK/log-$2.txt" 2>&1 || true
}

echo "== P0-5: Tributary -> required-mode TimeLakeDB =="
gen "$CORPUS/app.log"

# --- 1. correct token: exact count ---
cfg "p05_ok_$RUN"
TRIBUTARY_TOKEN="$TOKEN" run "p05_ok_$RUN" ok
sleep 1
chk "$(rows "p05_ok_$RUN")" "$LINES" "correct token: every line landed in the table (exact count)"
chk "$(grep -c '"read":'"$LINES" "$WORK/log-ok.txt" 2>/dev/null || echo 0)" "1" "agent read exactly the corpus"
chk "$(grep -c 'authenticated=true' "$WORK/log-ok.txt")" "1" "startup logged authenticated=true"

# --- 2. wrong token: nothing ships, nothing dropped, auth error surfaced ---
cfg "p05_wrong_$RUN"
TRIBUTARY_TOKEN="tldb_definitely_not_valid" run "p05_wrong_$RUN" wrong
sleep 1
chk "$(rows "p05_wrong_$RUN")" "0" "wrong token: zero rows reached the node"
# The counter is on the agent's final logfmt "done" line (unauthorized=N).
UNAUTH=$(grep -o 'unauthorized=[0-9]*' "$WORK/log-wrong.txt" | tail -1 | sed 's/.*=//')
[ -z "$UNAUTH" ] && UNAUTH=0
chk "$([ "$UNAUTH" -ge 1 ] && echo yes || echo no)" "yes" "wrong token: unauthorized counter fired ($UNAUTH)"
# One "rejected the token" per failed batch (>= 1); the point is it is
# reported as auth, not a generic transport error.
chk "$([ "$(grep -c 'rejected the token' "$WORK/log-wrong.txt")" -ge 1 ] && echo yes || echo no)" \
    "yes" "wrong token: reported as auth, not transport"
# data preserved: the durable queue holds the spooled batch (*.lp segments)
QSEG=$(find "$WORK/state-wrong/queue" -name '*.lp' 2>/dev/null | wc -l | tr -d ' ')
chk "$([ "$QSEG" -ge 1 ] && echo yes || echo no)" "yes" "wrong token: data spooled to the durable queue, not dropped ($QSEG segments)"

# --- 3. no token at all ---
cfg "p05_none_$RUN"
run "p05_none_$RUN" none  # no TRIBUTARY_TOKEN in env
sleep 1
chk "$(rows "p05_none_$RUN")" "0" "no token: zero rows reached the node"
chk "$(grep -c 'authenticated=false' "$WORK/log-none.txt")" "1" "startup logged authenticated=false"

# --- 4. the token never leaks into a log ---
LEAKS=$(cat "$WORK"/log-*.txt | grep -c "$TOKEN" || true)
chk "$LEAKS" "0" "the token value never appears in any agent log"

echo "== P0-5 verdict =="
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ] && echo "P0-5: PASS" || echo "P0-5: FAIL"
