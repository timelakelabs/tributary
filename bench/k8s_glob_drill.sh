#!/bin/bash
# Kubernetes glob end-to-end drill (#8 phase 2 / #64) — the evidence that ONE
# source tails a whole directory of container logs as independent per-file
# pipelines, each carrying its own pod/namespace/container (phase 1, #63).
#
# Four legs, each gated on bench/k8s_glob_assert.py (per-pod: the delivered idx
# set is exactly 0..N-1 AND every line carries that file's pod/ns/container):
#
#   1. DISCOVER   two CRI-named files under one glob, tailed to exact per-pod
#                 counts, each enriched with ITS pod/ns/container. Two files ->
#                 two checkpoints, two queues on disk.
#   2. APPEAR     a third container log CREATED mid-run (a pod starting) is
#                 picked up on the next rescan with no restart, and ships exact.
#   3. RESUME     a hard SIGKILL mid-stream, then restart: each file resumes
#                 from ITS OWN checkpoint (bounded replay), all end exact.
#   4. RETIRE     a file DELETED (a pod dying) stops that child and retires its
#                 checkpoint + queue, while the others keep flowing.
#
# Self-contained: a mock TimeLakeDB records every /write body, so this needs
# only rust + python3. The replay after the crash is left visible (total vs
# distinct) — a real DB collapses it by primary key, which the L1 drill proves
# against the real server.
#
#   docker run --rm -v "$PWD:/w" -w /w \
#     -v rk-cargo-registry:/usr/local/cargo/registry \
#     rust:1-slim bash -c 'apt-get update -qq && apt-get install -y -qq python3 \
#       >/dev/null; bench/k8s_glob_drill.sh'
set -u
export NO_COLOR=1
FAIL=0
check() { if [ "$2" = "$3" ]; then echo "  [PASS] $1 ($2)"; else echo "  [FAIL] $1 (got $2, want $3)"; FAIL=1; fi; }

BENCH=$(cd "$(dirname "$0")" && pwd)
cd "$BENCH/.."
WORK=$(mktemp -d)
DIR="$WORK/containers"; mkdir -p "$DIR"

# CRI symlink names: <pod>_<ns>_<container>-<64hex id>.log. The ids are fixed so
# the per-file stream identity (and thus the checkpoint/queue names) are stable
# across the restart in leg 3.
IDA="1111111111111111111111111111111111111111111111111111111111111111"
IDB="2222222222222222222222222222222222222222222222222222222222222222"
IDC="3333333333333333333333333333333333333333333333333333333333333333"
FA="$DIR/web-7d9c8b_shop_server-$IDA.log"        # pod web-7d9c8b  ns shop     ctr server
FB="$DIR/api-6c98bc_billing_worker-$IDB.log"     # pod api-6c98bc  ns billing  ctr worker
FC="$DIR/cache-abc123_shop_redis-$IDC.log"       # pod cache-abc123 ns shop    ctr redis
# The stable per-file stream names the agent derives (source name "k8s" + stem):
SA="k8s.web-7d9c8b_shop_server-$IDA"
SB="k8s.api-6c98bc_billing_worker-$IDB"

NA=6000; NB=4000; NC=3000

echo "=== k8s glob end-to-end drill ($(date -u +%Y-%m-%dT%H:%M:%SZ)) ==="
echo "workdir=$WORK  glob=$DIR/*.log  web=$NA api=$NB cache=$NC"

echo "-- build --"
cargo build -p tributary 2>&1 | tail -1
BIN=target/debug/tributary

cat > "$WORK/mock.py" <<'PYEOF'
import http.server, sys
LOG = sys.argv[1]
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get('content-length', 0))
        body = self.rfile.read(n)
        with open(LOG, 'ab') as f:
            f.write(body)
            if not body.endswith(b'\n'):
                f.write(b'\n')
        self.send_response(204); self.end_headers()
    def log_message(self, *a):
        pass
http.server.HTTPServer(('127.0.0.1', 8899), H).serve_forever()
PYEOF

MOCK=""
start_mock() { RECV="$1"; : > "$RECV"; python3 "$WORK/mock.py" "$RECV" & MOCK=$!; sleep 0.5; }
stop_mock() { [ -n "$MOCK" ] && kill "$MOCK" 2>/dev/null; wait "$MOCK" 2>/dev/null; MOCK=""; }

# distinct idx delivered for a pod (the CRI tag, so files are told apart by the
# thing phase 1 stamps, not by a stream name).
distinct_pod() { grep "pod=$1[ ,]" "$RECV" 2>/dev/null | grep -oE 'idx=[0-9]+i' | sort -u | wc -l | tr -d ' '; }
total_pod()    { grep -c "pod=$1[ ,]" "$RECV" 2>/dev/null || echo 0; }
await_pod() { local pod="$1" target="$2" i=0; while [ $i -lt 200 ]; do [ "$(distinct_pod "$pod")" -ge "$target" ] && break; sleep 0.5; i=$((i+1)); done; distinct_pod "$pod"; }

dribble() { local src="$1" log="$2" gap="$3" d; d=$(mktemp -d); split -l 300 -d "$src" "$d/c."; for p in "$d"/c.*; do cat "$p" >> "$log"; sleep "$gap"; done; rm -rf "$d"; }

# One glob source over the whole directory, kubernetes mode on.
cat > "$WORK/k8s.toml" <<EOF
[output]
url = "http://127.0.0.1:8899"
database = "logs"
gzip = false
batch_lines = 500

[[source]]
name = "k8s"
path = "$DIR/*.log"
table = "kube"
parser = "json"
timestamp = { field = "ts", format = "unix_ms", resolution = "ms" }
tags = ["level", "service"]

[source.fields]
idx = "integer"
message = "string"

[source.kubernetes]
EOF

python3 bench/gen.py --out "$WORK/web.full"   --lines "$NA" --rate 10000 >/dev/null
python3 bench/gen.py --out "$WORK/api.full"   --lines "$NB" --rate 10000 >/dev/null
python3 bench/gen.py --out "$WORK/cache.full" --lines "$NC" --rate 10000 >/dev/null

# ---------------------------------------------------------------------------
echo
echo "### LEG 1 — DISCOVER: two container logs, each enriched, exact ###"
start_mock "$WORK/recv1.lp"
cat "$WORK/web.full" > "$FA"
cat "$WORK/api.full" > "$FB"
STATE1="$WORK/state1"; mkdir -p "$STATE1"
"$BIN" --config "$WORK/k8s.toml" --state-dir "$STATE1" --once >"$WORK/leg1.log" 2>&1 || true
a=$(await_pod web-7d9c8b "$NA"); b=$(await_pod api-6c98bc "$NB")
echo "  delivered distinct: web=$a api=$b"
echo "  per-file state on disk: $(ls "$STATE1" | tr '\n' ' ')"
check "leg1 two checkpoints (one per file)" "$(ls "$STATE1"/*.checkpoint 2>/dev/null | wc -l | tr -d ' ')" "2"
check "leg1 two queue dirs (one per file)"  "$(ls -d "$STATE1"/queue-* 2>/dev/null | wc -l | tr -d ' ')" "2"
BEFORE=$FAIL
python3 bench/k8s_glob_assert.py "$WORK/recv1.lp" "web-7d9c8b=shop/server:$NA" "api-6c98bc=billing/worker:$NB" || FAIL=1
check "leg1 both pods complete and correctly enriched" "$FAIL" "$BEFORE"
stop_mock

# ---------------------------------------------------------------------------
echo
echo "### LEG 2 — APPEAR: a third container log created mid-run is picked up ###"
start_mock "$WORK/recv2.lp"
rm -f "$DIR"/*.log
STATE2="$WORK/state2"; mkdir -p "$STATE2"
cat "$WORK/web.full" > "$FA"                 # start with just web
"$BIN" --config "$WORK/k8s.toml" --state-dir "$STATE2" >"$WORK/leg2.log" 2>&1 &
AGENT=$!
a=$(await_pod web-7d9c8b "$NA")
echo "  [t0] web running, delivered=$a ; cache not created yet (delivered=$(distinct_pod cache-abc123))"
check "leg2 cache silent before its file exists" "$(distinct_pod cache-abc123)" "0"
# A pod starts: its container log appears. The 5 s rescan should adopt it.
cat "$WORK/cache.full" > "$FC"
echo "  [t1] created $FC — waiting for the rescan to discover it"
c=$(await_pod cache-abc123 "$NC")
echo "  cache delivered after discovery: $c"
kill "$AGENT" 2>/dev/null; wait "$AGENT" 2>/dev/null
BEFORE=$FAIL
python3 bench/k8s_glob_assert.py "$WORK/recv2.lp" "web-7d9c8b=shop/server:$NA" "cache-abc123=shop/redis:$NC" || FAIL=1
check "leg2 a pod that appeared mid-run is discovered and exact" "$FAIL" "$BEFORE"
stop_mock

# ---------------------------------------------------------------------------
echo
echo "### LEG 3 — RESUME: SIGKILL mid-stream, each file resumes from its own checkpoint ###"
start_mock "$WORK/recv3.lp"
rm -f "$DIR"/*.log
STATE3="$WORK/state3"; mkdir -p "$STATE3"
HALFA=$((NA/2)); HALFB=$((NB/2))
head -n "$HALFA" "$WORK/web.full" > "$FA"
head -n "$HALFB" "$WORK/api.full" > "$FB"
"$BIN" --config "$WORK/k8s.toml" --state-dir "$STATE3" >"$WORK/leg3a.log" 2>&1 &
AGENT=$!
await_pod web-7d9c8b "$HALFA" >/dev/null; await_pod api-6c98bc "$HALFB" >/dev/null
sleep 1.5                        # let the idle checkpoint fire per file
ca=$([ -s "$STATE3/$SA.checkpoint" ] && echo yes || echo no)
cb=$([ -s "$STATE3/$SB.checkpoint" ] && echo yes || echo no)
echo "  burst 1 quiesced; checkpoints: web=$ca api=$cb"
check "leg3 web checkpoint written before the crash" "$ca" "yes"
check "leg3 api checkpoint written before the crash" "$cb" "yes"
tail -n +"$((HALFA+1))" "$WORK/web.full" > "$WORK/web.b2"
tail -n +"$((HALFB+1))" "$WORK/api.full" > "$WORK/api.b2"
dribble "$WORK/web.b2" "$FA" 0.08 & WA=$!
dribble "$WORK/api.b2" "$FB" 0.10 & WB=$!
sleep 0.6
kill -9 "$AGENT" 2>/dev/null; wait "$AGENT" 2>/dev/null
ak=$(distinct_pod web-7d9c8b)
echo "  SIGKILLed mid burst-2: web=$ak/$NA distinct shipped before the kill"
check "leg3 web crashed past its checkpoint but before EOF" \
  "$([ "$ak" -gt "$HALFA" ] && [ "$ak" -lt "$NA" ] && echo yes || echo no)" "yes"
wait $WA $WB 2>/dev/null
"$BIN" --config "$WORK/k8s.toml" --state-dir "$STATE3" --once >"$WORK/leg3b.log" 2>&1 || true
a=$(await_pod web-7d9c8b "$NA"); b=$(await_pod api-6c98bc "$NB")
ra=$(( $(total_pod web-7d9c8b) - a )); rb=$(( $(total_pod api-6c98bc) - b ))
echo "  after resume distinct: web=$a api=$b ; replayed: web=$ra api=$rb"
check "leg3 web resumed from its checkpoint (replayed $ra < half $HALFA)" "$([ "$ra" -lt "$HALFA" ] && echo yes || echo no)" "yes"
check "leg3 api resumed from its checkpoint (replayed $rb < half $HALFB)" "$([ "$rb" -lt "$HALFB" ] && echo yes || echo no)" "yes"
BEFORE=$FAIL
python3 bench/k8s_glob_assert.py "$WORK/recv3.lp" "web-7d9c8b=shop/server:$NA" "api-6c98bc=billing/worker:$NB" || FAIL=1
check "leg3 both files exact after resume" "$FAIL" "$BEFORE"
stop_mock

# ---------------------------------------------------------------------------
echo
echo "### LEG 4 — RETIRE: a deleted container log stops its tail and retires its state ###"
start_mock "$WORK/recv4.lp"
rm -f "$DIR"/*.log
STATE4="$WORK/state4"; mkdir -p "$STATE4"
cat "$WORK/web.full" > "$FA"
cat "$WORK/api.full" > "$FB"
"$BIN" --config "$WORK/k8s.toml" --state-dir "$STATE4" >"$WORK/leg4.log" 2>&1 &
AGENT=$!
await_pod web-7d9c8b "$NA" >/dev/null; await_pod api-6c98bc "$NB" >/dev/null
sleep 1.5
have_a=$([ -e "$STATE4/$SA.checkpoint" ] && echo yes || echo no)
echo "  both shipped and quiesced; web state present=$have_a"
check "leg4 web checkpoint present before the pod dies" "$have_a" "yes"
# A pod dies: its container log is removed.
rm -f "$FA"
echo "  [t1] deleted $FA — waiting for the rescan (5 s) to retire it"
i=0; while [ $i -lt 30 ]; do [ -e "$STATE4/$SA.checkpoint" ] || break; sleep 0.5; i=$((i+1)); done
gone_cp=$([ -e "$STATE4/$SA.checkpoint" ] && echo no || echo yes)
gone_q=$([ -d "$STATE4/queue-$SA" ] && echo no || echo yes)
kept_b=$([ -e "$STATE4/$SB.checkpoint" ] && echo yes || echo no)
echo "  after retire: web checkpoint gone=$gone_cp  web queue gone=$gone_q  api checkpoint kept=$kept_b"
check "leg4 retired web's checkpoint" "$gone_cp" "yes"
check "leg4 retired web's queue dir"  "$gone_q" "yes"
check "leg4 api untouched by web's retirement" "$kept_b" "yes"
kill "$AGENT" 2>/dev/null; wait "$AGENT" 2>/dev/null
# The surviving sibling must be untouched: still complete, not retired.
BEFORE=$FAIL
python3 bench/k8s_glob_assert.py "$WORK/recv4.lp" "api-6c98bc=billing/worker:$NB" || FAIL=1
check "leg4 api still complete after the sibling retired" "$FAIL" "$BEFORE"
stop_mock

echo
if [ "$FAIL" = 0 ]; then echo "=== ALL PASS ==="; else echo "=== FAILURES ==="; fi
rm -rf "$WORK"
exit "$FAIL"
