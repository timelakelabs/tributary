#!/usr/bin/env bash
# Host-metrics collector drill (#25).
#
# Proof the collector is what it claims: run the REAL tributary binary with a
# [metrics] section against a live TimeLakeDB node, then read the six
# measurements back and assert they carry Telegraf's field and tag NAMES plus
# the configured global_tags / static_fields. A renamed field (used_pct for
# used_percent, hostname for host) is exactly what silently blanks a migrated
# dashboard, so the check is on the names, not just "rows exist".
#
# The collector runs inside the rust:1-slim container (sysinfo reads the
# container's /proc — real metrics, real schema) and ships to the node on the
# host via host.docker.internal. A fresh db per run means a re-run cannot read
# back a previous run's rows.
#
#   NODE_HOST=localhost PORT=1963 bench/metrics_collector_drill.sh
set -u

REPO="$(cd "$(dirname "$0")/.." && pwd)"
NODE_HOST="${NODE_HOST:-localhost}"     # node as seen from THIS host
PORT="${PORT:-1963}"
REG="${CARGO_REG_VOLUME:-rk-cargo-registry}"
RUN_SECS="${RUN_SECS:-16}"              # >=2 ticks at 2s so cpu (skips tick 0) appears
SUFFIX="$(date +%s)"
DB="metrics_drill_$SUFFIX"

echo "=== host-metrics collector drill ($(date '+%Y-%m-%d %H:%M:%S')) ==="
echo "node=$NODE_HOST:$PORT  db=$DB  run=${RUN_SECS}s"

# --- run the real collector against the node -------------------------------
MSYS_NO_PATHCONV=1 docker run --rm --add-host host.docker.internal:host-gateway \
  -v "$REPO:/w" -v "$REG:/usr/local/cargo/registry" -w /w rust:1-slim bash -c '
  set -e
  echo "-- building tributary --"
  cargo build -q --bin tributary
  mkdir -p /tmp/mstate
  cat > /tmp/m.toml <<CFG
[output]
url = "http://host.docker.internal:'"$PORT"'"
database = "'"$DB"'"
batch_lines = 50

[metrics]
interval = "2s"
collectors = ["cpu","mem","disk","net","system","swap"]

[metrics.global_tags]
region = "us-east"

[metrics.static_fields]
deployment = "prod"
CFG
  echo "-- collecting for '"$RUN_SECS"'s --"
  timeout '"$RUN_SECS"' ./target/debug/tributary --config /tmp/m.toml --state-dir /tmp/mstate 2>&1 | tail -6 || true
  echo "-- collector stopped --"
'

echo "-- querying the node back --"
sleep 1
python "$REPO/bench/metrics_collector_assert.py" "$NODE_HOST" "$PORT" "$DB"
