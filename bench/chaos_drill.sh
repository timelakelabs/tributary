#!/bin/bash
# Chaos engineering drills for tributary (#61) — adversarial fault injection.
#
#   chaos_drill.sh flaky     a misbehaving sink: reset / 5xx / latency / ambiguous-ack
#   (multisource | fuzz | backpressure land in later phases of #61)
#
# Every existing drill injects a clean, scripted fault (the L2 outage is a 60 s
# stop, on then off). These inject the nasty randomized failures a real database
# hands a shipper, and assert the promise holds throughout: no acked line lost,
# the at-least-once duplicates a real primary-key store collapses, no crash, and
# tributary's own accounting balances.
#
# Self-contained: bench/chaos_sink.py (a fault-injecting mock TimeLakeDB) stands
# in for the database, so it needs only the binary + python3 + curl. The gate is
# bench/multi_source_assert.py (the delivered idx set is exactly 0..N-1). Run in
# a container:
#   docker run --rm -v "$PWD:/w" -w /w \
#     -v rk-cargo-registry:/usr/local/cargo/registry -v rk-rustup:/usr/local/rustup \
#     rust:1-slim bash -c 'apt-get update -qq && apt-get install -y -qq python3 curl \
#       >/dev/null; cargo build -p tributary --bin tributary && \
#       BIN=target/debug/tributary bench/chaos_drill.sh flaky'
set -u
export NO_COLOR=1
BENCH=$(cd "$(dirname "$0")" && pwd)
cd "$BENCH/.."
BIN=${BIN:-./target/debug/tributary}
WORK=$(mktemp -d)
pass=0
fail=0
chk() { if [ "$1" = "$2" ]; then echo "  [PASS] $3 ($1)"; pass=$((pass + 1)); else echo "  [FAIL] $3 (got '$1' want '$2')"; fail=$((fail + 1)); fi; }

RECV=""
distinct() { grep "stream=$1[ ,]" "$RECV" 2>/dev/null | grep -oE 'idx=[0-9]+i' | sort -u | wc -l | tr -d ' '; }
total_lines() { grep -c "stream=$1[ ,]" "$RECV" 2>/dev/null || echo 0; }
metric() { curl -fs "http://127.0.0.1:9899/metrics" 2>/dev/null | grep -E "^$1[ {]" | awk '{print $NF}' | head -1; }

# Append a file to a live log in ~300-line chunks over wall-clock time, so a
# fault (rotation, SIGHUP, SIGKILL) lands mid-stream against a growing file.
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
table = "chaos"
parser = "json"
timestamp = { field = "ts", format = "unix_ms", resolution = "ms" }

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
batch_lines = 400
gzip = false

[telemetry]
addr = "127.0.0.1:9899"
EOF
  local name
  for name in "$@"; do src_block "$name" "$WORK/$name.log" >> "$f"; done
}

# ---------------------------------------------------------------------------
flaky() {
  local N=${N:-30000}
  echo "### CHAOS: flaky sink — reset / 5xx / latency / ambiguous-ack (N=$N) ###"
  python3 bench/gen.py --out "$WORK/app.log" --lines "$N" --rate 10000 >/dev/null
  RECV="$WORK/recv.lp"
  : > "$RECV"
  CHAOS_SEED=7 python3 bench/chaos_sink.py "$RECV" 127.0.0.1:8899 &
  local SINK=$!
  sleep 0.5

  cat > "$WORK/c.toml" <<TOML
[output]
url = "http://127.0.0.1:8899"
batch_lines = 500
gzip = false

[telemetry]
addr = "127.0.0.1:9899"

[[source]]
name = "app"
path = "$WORK/app.log"
table = "chaos"
parser = "json"
timestamp = { field = "ts", format = "unix_ms", resolution = "ms" }

[source.fields]
idx = "integer"
message = "string"
TOML

  "$BIN" --config "$WORK/c.toml" --state-dir "$WORK/state" > "$WORK/agent.log" 2>&1 &
  local AGENT=$!

  # The sink is hostile, so batches fail and retry from the durable queue. Wait
  # until tributary's queue has fully drained — every batch finally acked — and
  # the full distinct set has landed.
  local qb="?" d=0
  for _ in $(seq 1 240); do
    qb=$(metric tributary_queue_bytes)
    d=$(distinct app)
    [ "${qb:-1}" = "0" ] && [ "${d:-0}" -ge "$N" ] && break
    sleep 1
  done
  echo "  after the storm: queue_bytes=$qb distinct=$(distinct app) delivered=$(total_lines app)"

  # 1) nothing lost: the delivered idx set is exactly 0..N-1.
  local BEFORE=$fail
  python3 bench/multi_source_assert.py "$RECV" "app:$N" || fail=1
  chk "$fail" "$BEFORE" "delivered set is exactly complete under chaos"

  # 2) the chaos genuinely exercised the duplicate path (ambiguous acks), so
  #    the "no loss" above is a real result and not a quiet outage.
  local dupes=$(( $(total_lines app) - $(distinct app) ))
  chk "$([ "$dupes" -gt 0 ] && echo yes || echo no)" "yes" \
    "at-least-once duplicates were delivered ($dupes, deduped by idx)"

  # 3) the agent never crashed under the storm.
  chk "$(kill -0 "$AGENT" 2>/dev/null && echo yes || echo no)" "yes" \
    "agent still running after the storm"

  # 4) tributary's own accounting balances: it read every line, acked each
  #    exactly once (no over-count on a retry), and the queue is empty.
  local read shipped
  read=$(metric tributary_lines_read_total)
  shipped=$(metric tributary_lines_shipped_total)
  echo "  metrics: read=$read shipped=$shipped queue_bytes=$qb"
  chk "${read:-0}" "$N" "read == N"
  chk "${shipped:-0}" "${read:-0}" "shipped == read (acked once each, retries did not over-count)"
  chk "${qb:-1}" "0" "queue fully drained"

  kill -9 "$AGENT" "$SINK" 2>/dev/null
}

# ---------------------------------------------------------------------------
multisource() {
  local NA=${NA:-9000} NB=${NB:-9000} NG=${NG:-9000}
  echo "### CHAOS: multi-source — rotation + SIGHUP add/remove + SIGKILL, concurrent (a=$NA b=$NB g=$NG) ###"
  RECV="$WORK/recv.lp"
  : > "$RECV"
  # A reliable recording sink: here the chaos is on the SOURCE side, the config,
  # and a crash — not the sink. Isolation is the property: a rotation on beta,
  # gamma churning in and out of the config, and a SIGKILL of the whole agent
  # must not lose or cross a single line on any stream.
  CHAOS_5XX=0 CHAOS_RESET=0 CHAOS_LATENCY=0 CHAOS_AMBIGUOUS=0 \
    python3 bench/chaos_sink.py "$RECV" 127.0.0.1:8899 &
  local SINK=$!
  sleep 0.5

  python3 bench/gen.py --out "$WORK/alpha.full" --lines "$NA" --rate 10000 >/dev/null
  python3 bench/gen.py --out "$WORK/beta.full" --lines "$NB" --rate 10000 >/dev/null
  python3 bench/gen.py --out "$WORK/gamma.full" --lines "$NG" --rate 10000 >/dev/null
  : > "$WORK/alpha.log"; : > "$WORK/beta.log"; : > "$WORK/gamma.log"
  write_config "$WORK/all.toml" alpha beta gamma
  write_config "$WORK/no_g.toml" alpha beta

  cp "$WORK/all.toml" "$WORK/live.toml"
  "$BIN" --config "$WORK/live.toml" --state-dir "$WORK/state" > "$WORK/agent.log" 2>&1 &
  local AGENT=$!
  sleep 1

  # dribble all three concurrently over ~10s so every fault lands mid-stream
  dribble "$WORK/alpha.full" "$WORK/alpha.log" 0.35 &
  local DA=$!
  dribble "$WORK/beta.full" "$WORK/beta.log" 0.35 &
  local DB=$!
  dribble "$WORK/gamma.full" "$WORK/gamma.log" 0.35 &
  local DG=$!

  sleep 2
  echo "  [fault] rotate beta mid-stream (the dribble recreates beta.log)"
  mv "$WORK/beta.log" "$WORK/beta.rotated"
  sleep 2
  echo "  [fault] SIGHUP: remove gamma while it is streaming"
  cp "$WORK/no_g.toml" "$WORK/live.toml"; kill -HUP "$AGENT"
  sleep 2
  echo "  [fault] SIGHUP: re-add gamma"
  cp "$WORK/all.toml" "$WORK/live.toml"; kill -HUP "$AGENT"
  sleep 2
  echo "  [fault] SIGKILL the whole agent; restart"
  kill -9 "$AGENT" 2>/dev/null; wait "$AGENT" 2>/dev/null
  "$BIN" --config "$WORK/live.toml" --state-dir "$WORK/state" > "$WORK/agent2.log" 2>&1 &
  AGENT=$!

  wait "$DA" "$DB" "$DG" 2>/dev/null
  for _ in $(seq 1 120); do
    [ "$(distinct alpha)" -ge "$NA" ] && [ "$(distinct beta)" -ge "$NB" ] && [ "$(distinct gamma)" -ge "$NG" ] && break
    sleep 1
  done
  echo "  distinct: alpha=$(distinct alpha) beta=$(distinct beta) gamma=$(distinct gamma)"

  local BEFORE=$fail
  python3 bench/multi_source_assert.py "$RECV" "alpha:$NA" "beta:$NB" "gamma:$NG" || fail=1
  chk "$fail" "$BEFORE" "every stream exactly complete despite rotation + reconfig + crash"
  chk "$([ -f "$WORK/state/alpha.checkpoint" ] && [ -f "$WORK/state/beta.checkpoint" ] && [ -f "$WORK/state/gamma.checkpoint" ] && echo yes || echo no)" \
    "yes" "each source kept its own isolated checkpoint on disk"
  chk "$(kill -0 "$AGENT" 2>/dev/null && echo yes || echo no)" "yes" "agent survived the storm and is running"

  kill -9 "$AGENT" "$SINK" 2>/dev/null
}

# ---------------------------------------------------------------------------
fuzz() {
  local N=${N:-12000} KILLS=${KILLS:-12}
  echo "### CHAOS: crash-resume fuzzer — $KILLS random SIGKILLs across one run (N=$N) ###"
  RECV="$WORK/recv.lp"
  : > "$RECV"
  CHAOS_5XX=0 CHAOS_RESET=0 CHAOS_LATENCY=0 CHAOS_AMBIGUOUS=0 \
    python3 bench/chaos_sink.py "$RECV" 127.0.0.1:8899 &
  local SINK=$!
  sleep 0.5
  python3 bench/gen.py --out "$WORK/app.full" --lines "$N" --rate 10000 >/dev/null
  : > "$WORK/app.log"
  write_config "$WORK/c.toml" app
  # dribble over ~12s so the kills keep landing on a growing, in-flight stream
  dribble "$WORK/app.full" "$WORK/app.log" 0.30 &
  local DR=$!
  RANDOM=42
  local k
  for k in $(seq 1 "$KILLS"); do
    "$BIN" --config "$WORK/c.toml" --state-dir "$WORK/state" > "$WORK/agent.$k.log" 2>&1 &
    local A=$!
    sleep "0.$(((RANDOM % 7) + 2))" # 0.2 - 0.8 s, then a hard kill
    kill -9 "$A" 2>/dev/null
    wait "$A" 2>/dev/null
  done
  echo "  survived $KILLS random kills; final drain"
  "$BIN" --config "$WORK/c.toml" --state-dir "$WORK/state" > "$WORK/agent.final.log" 2>&1 &
  local AGENT=$!
  wait "$DR" 2>/dev/null
  for _ in $(seq 1 120); do [ "$(distinct app)" -ge "$N" ] && break; sleep 1; done
  echo "  distinct=$(distinct app) delivered=$(total_lines app)"

  local BEFORE=$fail
  python3 bench/multi_source_assert.py "$RECV" "app:$N" || fail=1
  chk "$fail" "$BEFORE" "exactly complete across $KILLS random crashes (no gap; dupes collapse by idx)"
  chk "$([ "$(($(total_lines app) - $(distinct app)))" -gt 0 ] && echo yes || echo no)" "yes" \
    "the kills hit in-flight batches (replays present)"
  chk "$(kill -0 "$AGENT" 2>/dev/null && echo yes || echo no)" "yes" "the final agent is running"
  kill -9 "$AGENT" "$SINK" 2>/dev/null
}

# ---------------------------------------------------------------------------
backpressure() {
  local N=${N:-80000} QMAX=${QMAX:-2097152}
  echo "### CHAOS: backpressure — a blocked sink fills the ${QMAX}-byte queue (N=$N) ###"
  RECV="$WORK/recv.lp"
  : > "$RECV"
  # phase 1: a sink that always 503s — nothing ships, so the queue fills
  CHAOS_5XX=1 CHAOS_RESET=0 CHAOS_LATENCY=0 CHAOS_AMBIGUOUS=0 \
    python3 bench/chaos_sink.py "$RECV" 127.0.0.1:8899 &
  local SINK=$!
  sleep 0.5
  python3 bench/gen.py --out "$WORK/app.log" --lines "$N" --rate 10000 >/dev/null
  cat > "$WORK/c.toml" <<TOML
[output]
url = "http://127.0.0.1:8899"
batch_lines = 400
gzip = false
queue_max_bytes = $QMAX

[telemetry]
addr = "127.0.0.1:9899"

[[source]]
name = "app"
path = "$WORK/app.log"
table = "chaos"
parser = "json"
timestamp = { field = "ts", format = "unix_ms", resolution = "ms" }

[source.fields]
idx = "integer"
message = "string"
TOML
  "$BIN" --config "$WORK/c.toml" --state-dir "$WORK/state" > "$WORK/agent.log" 2>&1 &
  local AGENT=$!

  local qf=0 qb=0
  for _ in $(seq 1 60); do
    qf=$(metric tributary_queue_full)
    qb=$(metric tributary_queue_bytes)
    [ "${qf:-0}" = "1" ] && break
    sleep 0.5
  done
  local readd
  readd=$(metric tributary_lines_read_total)
  echo "  under the block: queue_full=$qf queue_bytes=$qb read=$readd (of $N)"
  chk "${qf:-0}" "1" "backpressure engaged (queue_full=1)"
  # The queue force-spools a failed in-flight batch past the soft cap rather
  # than dropping it (the #61 fix), so it overshoots by the bounded in-flight
  # set — but never grows without bound. Allow the cap plus a generous margin.
  local BOUND=$((QMAX + 1048576))
  chk "$([ "${qb:-0}" -le "$BOUND" ] && echo yes || echo no)" "yes" \
    "the queue stayed bounded (soft cap + in-flight margin, no unbounded growth)"
  chk "$([ "${readd:-0}" -lt "$N" ] && echo yes || echo no)" "yes" "reading stalled under backpressure (read < N, not dropping)"
  chk "$(kill -0 "$AGENT" 2>/dev/null && echo yes || echo no)" "yes" "agent alive and holding under backpressure"

  # phase 2: recover — a reliable sink on the same port
  kill -9 "$SINK" 2>/dev/null
  wait "$SINK" 2>/dev/null
  CHAOS_5XX=0 CHAOS_RESET=0 CHAOS_LATENCY=0 CHAOS_AMBIGUOUS=0 \
    python3 bench/chaos_sink.py "$RECV" 127.0.0.1:8899 &
  SINK=$!
  echo "  sink recovered; draining and resuming the stalled read…"
  for _ in $(seq 1 240); do
    [ "$(distinct app)" -ge "$N" ] && [ "$(metric tributary_queue_bytes)" = "0" ] && break
    sleep 1
  done
  echo "  after recovery: distinct=$(distinct app) read=$(metric tributary_lines_read_total) shipped=$(metric tributary_lines_shipped_total) queue_bytes=$(metric tributary_queue_bytes)"
  local BEFORE=$fail
  python3 bench/multi_source_assert.py "$RECV" "app:$N" || fail=1
  chk "$fail" "$BEFORE" "no loss: everything the block held is delivered once the sink returns"
  chk "$(metric tributary_lines_read_total)" "$N" "reading resumed to completion (read == N)"
  chk "$(metric tributary_queue_bytes)" "0" "queue drained after recovery"
  kill -9 "$AGENT" "$SINK" 2>/dev/null
}

# ---------------------------------------------------------------------------
case "${1:-}" in
  flaky) flaky ;;
  multisource) multisource ;;
  fuzz) fuzz ;;
  backpressure) backpressure ;;
  *)
    echo "usage: chaos_drill.sh flaky|multisource|fuzz|backpressure" >&2
    rm -rf "$WORK"
    exit 2
    ;;
esac

echo
echo "=== chaos '$1': $pass passed, $fail failed ==="
if [ "$fail" -eq 0 ]; then echo "=== PASS ==="; else echo "=== FAIL ==="; fi
rm -rf "$WORK"
[ "$fail" -eq 0 ]
