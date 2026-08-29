#!/bin/bash
# Multi-source end-to-end drill (#49 phase 4 / #54) — the evidence that one
# agent tails several [[source]] files as independent pipelines.
#
# Three legs, each gated on the per-stream exactness check (bench/
# multi_source_assert.py: the delivered idx set is exactly 0..N-1, per stream):
#
#   1. STEADY   two sources tailed to exact per-stream counts, no crossover.
#   2. RESUME   a hard SIGKILL mid-stream, then restart: each source resumes
#               from ITS OWN checkpoint and both end exactly complete. The kill
#               is process-wide by nature — what it proves is that the two
#               checkpoints are independent, not that one task can be killed
#               alone (that is what leg 3's SIGHUP-remove shows).
#   3. RECONFIG a live SIGHUP that ADDS a source (it starts tailing only then)
#               and one that REMOVES a source (it stops cleanly, its queue and
#               checkpoint preserved on disk) while the other source keeps
#               flowing across both reconfigurations.
#
# Self-contained: a mock TimeLakeDB records every /write body, so the drill
# needs only rust + python3, no running database. Assertions are on the
# delivered line protocol. A real DB would collapse the post-crash replay by
# primary key — exactly what the deterministic stamper guarantees and what the
# L1 drill already proves against the real server; here the replay is left
# visible (total vs distinct) as evidence the kill genuinely hit in-flight data.
#
# The corpus is fed in incrementally (split into chunks appended over time) so
# a kill lands mid-stream deterministically, whatever the sink's speed — a
# fixed sleep against a fast mock would otherwise race the agent to EOF.
#
#   docker run --rm -v "$PWD:/w" -w /w \
#     -v rk-cargo-registry:/usr/local/cargo/registry \
#     rust:1-slim bash -c 'apt-get update -qq && apt-get install -y -qq python3 \
#       >/dev/null; bench/multi_source_drill.sh'
set -u
export NO_COLOR=1   # keep the agent's own log free of ANSI in the transcript
FAIL=0
check() { if [ "$2" = "$3" ]; then echo "  [PASS] $1 ($2)"; else echo "  [FAIL] $1 (got $2, want $3)"; FAIL=1; fi; }

BENCH=$(cd "$(dirname "$0")" && pwd)
cd "$BENCH/.."
WORK=$(mktemp -d)
NA=6000    # alpha corpus size
NB=4000    # beta corpus size (deliberately different, so the two checkpoints
           # sit at different offsets — the point of the independence claim)
EXTRA=1500 # extra beta lines fed AFTER alpha is removed in leg 3

echo "=== multi-source end-to-end drill ($(date -u +%Y-%m-%dT%H:%M:%SZ)) ==="
echo "workdir=$WORK  alpha=$NA beta=$NB extra=$EXTRA"

echo "-- build --"
cargo build -p tributary 2>&1 | tail -1
BIN=target/debug/tributary

# Mock TimeLakeDB: append every /write body so we can read exactly what was
# delivered. gzip is off in the configs, so the bodies are plain line protocol.
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

# distinct idx delivered for a stream in the current RECV. `stream=<n>[ ,]`
# pins the tag so alpha does not match a hypothetical alpha2.
distinct() { grep "stream=$1[ ,]" "$RECV" 2>/dev/null | grep -oE 'idx=[0-9]+i' | sort -u | wc -l | tr -d ' '; }
# total records delivered for a stream (with replays) — total minus distinct is
# how many lines the agent re-shipped, the tell for where a resume started.
total_lines() { grep -c "stream=$1[ ,]" "$RECV" 2>/dev/null || echo 0; }

# Wait until a stream reaches a target distinct count, or give up (so a slow
# run reads as slow, not as loss). Echoes the count reached.
await_stream() {
  local stream="$1" target="$2" i=0
  while [ $i -lt 160 ]; do
    [ "$(distinct "$stream")" -ge "$target" ] && break
    sleep 0.5; i=$((i + 1))
  done
  distinct "$stream"
}

# Append a file to a live log in ~300-line chunks over wall-clock time, so the
# agent tails a growing file and a mid-stream kill is deterministic.
dribble() { # source-file  log-file  seconds-between-chunks
  local src="$1" log="$2" gap="$3" d
  d=$(mktemp -d)
  split -l 300 -d "$src" "$d/c."
  for p in "$d"/c.*; do cat "$p" >> "$log"; sleep "$gap"; done
  rm -rf "$d"
}

src_block() { # name path
  cat <<EOF

[[source]]
name = "$1"
path = "$2"
table = "multi"
parser = "json"
timestamp = { field = "ts", format = "unix_ms", resolution = "ms" }
tags = ["level", "service"]

[source.fields]
idx = "integer"
message = "string"
EOF
}

write_config() { # file, then source names
  local f="$1"; shift
  cat > "$f" <<EOF
[output]
url = "http://127.0.0.1:8899"
database = "logs"
gzip = false
batch_lines = 500
EOF
  for name in "$@"; do src_block "$name" "$WORK/$name.log" >> "$f"; done
}

write_config "$WORK/both.toml"       alpha beta
write_config "$WORK/alpha_only.toml" alpha
write_config "$WORK/beta_only.toml"  beta

# Staging corpora (generated once; fed into the live logs per leg).
python3 bench/gen.py --out "$WORK/alpha.full" --lines "$NA" --rate 10000 >/dev/null
python3 bench/gen.py --out "$WORK/beta.full"  --lines "$((NB + EXTRA))" --rate 10000 >/dev/null

# ---------------------------------------------------------------------------
echo
echo "### LEG 1 — STEADY: two sources, exact per-stream counts ###"
start_mock "$WORK/recv1.lp"
: > "$WORK/alpha.log"; : > "$WORK/beta.log"
cat "$WORK/alpha.full" > "$WORK/alpha.log"
head -n "$NB" "$WORK/beta.full" > "$WORK/beta.log"
STATE1="$WORK/state1"; mkdir -p "$STATE1"
"$BIN" --config "$WORK/both.toml" --state-dir "$STATE1" --once >"$WORK/leg1.log" 2>&1 || true
a=$(await_stream alpha "$NA"); b=$(await_stream beta "$NB")
echo "  delivered distinct: alpha=$a beta=$b"
echo "  per-source state on disk: $(ls "$STATE1" | tr '\n' ' ')"
BEFORE=$FAIL
python3 bench/multi_source_assert.py "$WORK/recv1.lp" "alpha:$NA" "beta:$NB" || FAIL=1
check "leg1 both streams exact" "$FAIL" "$BEFORE"
stop_mock

# ---------------------------------------------------------------------------
echo
echo "### LEG 2 — RESUME: SIGKILL mid-stream, resume from each source's own checkpoint ###"
start_mock "$WORK/recv2.lp"
: > "$WORK/alpha.log"; : > "$WORK/beta.log"
STATE2="$WORK/state2"; mkdir -p "$STATE2"
HALFA=$((NA / 2)); HALFB=$((NB / 2))

# Burst 1: the first half of each corpus. Let the agent ship it and go quiet —
# the periodic checkpoint only fires when the framer has no pending frame (see
# main.rs, the `!framer.has_pending()` guard), i.e. between bursts, not while a
# file is actively growing. So this is what puts a checkpoint at ~half on disk.
head -n "$HALFA" "$WORK/alpha.full" > "$WORK/alpha.log"
head -n "$HALFB" "$WORK/beta.full"  > "$WORK/beta.log"
"$BIN" --config "$WORK/both.toml" --state-dir "$STATE2" >"$WORK/leg2a.log" 2>&1 &
AGENT=$!
await_stream alpha "$HALFA" >/dev/null; await_stream beta "$HALFB" >/dev/null
sleep 1.2                       # let the 500 ms idle checkpoint fire
ca=$([ -s "$STATE2/alpha.checkpoint" ] && echo yes || echo no)
cb=$([ -s "$STATE2/beta.checkpoint" ] && echo yes || echo no)
echo "  burst 1 shipped and quiesced; checkpoints on disk: alpha=$ca ($(wc -c <"$STATE2"/alpha.checkpoint 2>/dev/null | tr -d ' ')B) beta=$cb ($(wc -c <"$STATE2"/beta.checkpoint 2>/dev/null | tr -d ' ')B)"
check "leg2 alpha checkpoint written before the crash" "$ca" "yes"
check "leg2 beta checkpoint written before the crash" "$cb" "yes"

# Burst 2: dribble the second half in, then SIGKILL while it is still streaming
# (so the kill lands past each checkpoint but before EOF).
tail -n +"$((HALFA + 1))" "$WORK/alpha.full" > "$WORK/alpha.b2"
tail -n +"$((HALFB + 1))" "$WORK/beta.full" | head -n "$((NB - HALFB))" > "$WORK/beta.b2"
dribble "$WORK/alpha.b2" "$WORK/alpha.log" 0.08 &  WA=$!
dribble "$WORK/beta.b2"  "$WORK/beta.log"  0.10 &  WB=$!
sleep 0.6
kill -9 $AGENT 2>/dev/null; wait $AGENT 2>/dev/null
ak=$(distinct alpha); bk=$(distinct beta)
echo "  SIGKILLed mid burst-2: alpha=$ak/$NA beta=$bk/$NB distinct shipped before the kill"
check "leg2 alpha crashed past its checkpoint but before EOF (>$HALFA, <$NA)" \
  "$([ "$ak" -gt "$HALFA" ] && [ "$ak" -lt "$NA" ] && echo yes || echo no)" "yes"
wait $WA $WB 2>/dev/null         # let the full second half land on disk

# Restart: each source must resume from ITS OWN checkpoint (~half), not replay
# from zero. Proof: the replay count is far below the pre-kill count — only the
# un-acked tail past the checkpoint is re-shipped, not everything shipped so far.
"$BIN" --config "$WORK/both.toml" --state-dir "$STATE2" --once >"$WORK/leg2b.log" 2>&1 || true
a=$(await_stream alpha "$NA"); b=$(await_stream beta "$NB")
ra=$(( $(total_lines alpha) - a )); rb=$(( $(total_lines beta) - b ))
echo "  after resume distinct: alpha=$a beta=$b ; replayed: alpha=$ra beta=$rb"
echo "  (a replay far below the pre-kill count means the restart resumed from the checkpoint, not from zero)"
check "leg2 alpha resumed from its checkpoint (replayed $ra < half $HALFA)" "$([ "$ra" -lt "$HALFA" ] && echo yes || echo no)" "yes"
check "leg2 beta resumed from its checkpoint (replayed $rb < half $HALFB)"  "$([ "$rb" -lt "$HALFB" ] && echo yes || echo no)" "yes"
BEFORE=$FAIL
python3 bench/multi_source_assert.py "$WORK/recv2.lp" "alpha:$NA" "beta:$NB" || FAIL=1
check "leg2 both streams exact after resume" "$FAIL" "$BEFORE"
stop_mock

# ---------------------------------------------------------------------------
echo
echo "### LEG 3 — RECONFIG: live SIGHUP add + remove, the other keeps flowing ###"
start_mock "$WORK/recv3.lp"
: > "$WORK/alpha.log"; : > "$WORK/beta.log"
STATE3="$WORK/state3"; mkdir -p "$STATE3"
cat "$WORK/alpha.full" > "$WORK/alpha.log"      # alpha's whole corpus available

# start alpha-only
cp "$WORK/alpha_only.toml" "$WORK/live.toml"
"$BIN" --config "$WORK/live.toml" --state-dir "$STATE3" >"$WORK/leg3.log" 2>&1 &
AGENT=$!
a=$(await_stream alpha "$NA")
echo "  [t0] alpha-only running; alpha delivered=$a  beta delivered=$(distinct beta) (beta not configured yet)"
check "leg3 beta silent before it is added" "$(distinct beta)" "0"

# SIGHUP: ADD beta. It should start tailing only now. Append beta's first NB.
head -n "$NB" "$WORK/beta.full" >> "$WORK/beta.log"
cp "$WORK/both.toml" "$WORK/live.toml"
kill -HUP $AGENT
b=$(await_stream beta "$NB")
echo "  [t1] SIGHUP add beta; beta delivered=$b  alpha still=$(distinct alpha)"
grep -q "source added" "$WORK/leg3.log" && echo "    log: $(grep 'source added' "$WORK/leg3.log" | tail -1 | sed 's/.*tributary: //')"

# SIGHUP: REMOVE alpha. beta must keep flowing; alpha's state must survive.
cp "$WORK/beta_only.toml" "$WORK/live.toml"
kill -HUP $AGENT
sleep 1
alive=$(kill -0 $AGENT 2>/dev/null && echo yes || echo no)
echo "  [t2] SIGHUP remove alpha; agent alive=$alive"
grep -q "source removed" "$WORK/leg3.log" && echo "    log: $(grep 'source removed' "$WORK/leg3.log" | tail -1 | sed 's/.*tributary: //')"
cp_ok=$([ -f "$STATE3/alpha.checkpoint" ] && echo yes || echo no)
q_ok=$([ -d "$STATE3/queue-alpha" ] && echo yes || echo no)
echo "    alpha state preserved: checkpoint=$cp_ok queue=$q_ok"
check "leg3 agent survived remove" "$alive" "yes"
check "leg3 removed source's checkpoint preserved" "$cp_ok" "yes"
check "leg3 removed source's queue not orphaned" "$q_ok" "yes"

# the OTHER keeps flowing: append the remaining beta after alpha is gone.
tail -n +"$((NB + 1))" "$WORK/beta.full" >> "$WORK/beta.log"
b=$(await_stream beta "$((NB + EXTRA))")
echo "  [t3] beta kept flowing after the remove; beta delivered=$b (target $((NB + EXTRA)))"
kill -TERM $AGENT 2>/dev/null; wait $AGENT 2>/dev/null

BEFORE=$FAIL
python3 bench/multi_source_assert.py "$WORK/recv3.lp" "alpha:$NA" "beta:$((NB + EXTRA))" || FAIL=1
check "leg3 alpha exact up to removal, beta exact across the whole run" "$FAIL" "$BEFORE"
stop_mock

# ---------------------------------------------------------------------------
echo
if [ "$FAIL" = 0 ]; then
  echo "=== PASS: one agent tails many sources — exact per stream, independent"
  echo "          checkpoints across a crash, and a live add/remove that leaves"
  echo "          the other sources flowing. ==="
else
  echo "=== FAIL ==="
fi
rm -rf "$WORK"
exit $FAIL
