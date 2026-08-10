#!/bin/sh
# L2 drills: outage absorption and poison-line isolation.
#   drill-l2.sh outage | poison
set -e
BENCH=$(cd "$(dirname "$0")" && pwd)
cd "$BENCH/.."

CORPUS=/tmp/tributary-corpus
STATE=/tmp/tributary-state
BIN=./target/debug/tributary
TABLE=${TABLE:-l2_outage}
LINES=${LINES:-400000}
RATE=${RATE:-10000}

rows() {
  curl -s -X POST http://localhost:1963/api/sql -H 'content-type: application/json' \
    -d "{\"db\":\"logs\",\"sql\":\"SELECT COUNT(*) AS n FROM $TABLE\"}" 2>/dev/null \
    | sed 's/.*"n"://; s/[^0-9].*//' || echo 0
}

await_quiescence() {
  last=-1; stable=0; i=0
  while [ $i -lt 180 ]; do
    n=$(rows); n=${n:-0}
    if [ "$n" = "$last" ]; then stable=$((stable+1)); [ $stable -ge 3 ] && break
    else stable=0; fi
    last=$n; i=$((i+1)); sleep 1
  done
  echo "  quiesced at $last rows"
}

rm -rf "$CORPUS" "$STATE"; mkdir -p "$CORPUS" "$STATE"

case "$1" in
outage)
  # The database goes away for 60 s while the source keeps growing. The
  # queue must absorb it, nothing may be lost, and the agent must not
  # hammer a server that is not there.
  python3 bench/gen.py --out "$CORPUS/app.log" --lines "$LINES" --rate "$RATE" >/dev/null
  $BIN --config bench/l2.toml --state-dir "$STATE" >/tmp/agent.log 2>&1 &
  AGENT=$!
  sleep 2
  echo "  before outage: $(rows) rows"
  echo "  --- stopping TimeLakeDB for 60s ---"
  ;;
poison)
  # A batch containing an unparseable line and a binary blob: the batch
  # is atomic, so without bisect all 5,000 good lines would be lost.
  python3 bench/gen.py --out "$CORPUS/app.log" --lines 20000 --rate "$RATE" \
    --malformed-at 7777 --binary-at 12345 >/dev/null
  $BIN --config bench/l2.toml --state-dir "$STATE" --once >/tmp/agent.log 2>&1 || true
  await_quiescence
  echo "  agent: $(tail -1 /tmp/agent.log)"
  echo "  dead-letter lines: $(wc -l < "$STATE/dead-letter.lp" 2>/dev/null || echo 0)"
  ;;
esac
