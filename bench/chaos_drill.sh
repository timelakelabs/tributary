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
case "${1:-}" in
  flaky) flaky ;;
  *)
    echo "usage: chaos_drill.sh flaky" >&2
    rm -rf "$WORK"
    exit 2
    ;;
esac

echo
echo "=== chaos '$1': $pass passed, $fail failed ==="
if [ "$fail" -eq 0 ]; then echo "=== PASS ==="; else echo "=== FAIL ==="; fi
rm -rf "$WORK"
[ "$fail" -eq 0 ]
