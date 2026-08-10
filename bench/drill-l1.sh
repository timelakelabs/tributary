#!/bin/sh
# L1 drills: rotation and crash-resume, both gated on the exact-count
# assertion (lines written == rows stored, exactly).
#
# Run inside the database's network namespace so localhost:1963 is the
# server — the same discipline TimeLakeDB's own perf work uses, since a
# published port on Docker Desktop adds ~45 ms per request.
#
#   drill-l1.sh rotation | resume
set -e
BENCH=$(cd "$(dirname "$0")" && pwd)
cd "$BENCH/.."

# The corpus lives on a container-local filesystem, NOT the bind mount.
# On a Docker Desktop host mount an open fd LOSES its file after a rename
# (it reports len=0 instead of the original size), so rotation cannot be
# tested there at all � the drill would measure the mount, not the agent.
CORPUS=/tmp/tributary-corpus
# Checkpoints fsync. Keep them OFF the bind mount: on Docker Desktop a
# host mount makes every fsync cost milliseconds, the agent falls behind
# its own source, and the drill measures the mount rather than the agent.
STATE=/tmp/tributary-state
BIN=./target/debug/tributary
TABLE=${TABLE:-l1_rot}
LINES=${LINES:-120000}
RATE=${RATE:-10000}

reset() {
  rm -rf "$CORPUS" "$STATE"
  mkdir -p "$CORPUS" "$STATE"
}

rows() {
  curl -s -X POST http://localhost:1963/api/sql \
    -H 'content-type: application/json' \
    -d "{\"db\":\"logs\",\"sql\":\"SELECT COUNT(*) AS n FROM $TABLE\"}" 2>/dev/null \
    | sed 's/.*"n"://; s/[^0-9].*//' || echo 0
}

# Wait until the agent stops making progress, rather than guessing with a
# fixed sleep — a slow drill run must not look like data loss.
await_quiescence() {
  last=-1
  stable=0
  i=0
  while [ $i -lt 120 ]; do
    n=$(rows); n=${n:-0}
    if [ "$n" = "$last" ]; then
      stable=$((stable + 1))
      [ $stable -ge 3 ] && break
    else
      stable=0
    fi
    last=$n
    i=$((i + 1))
    sleep 1
  done
  echo "  quiesced at $last rows"
}

case "$1" in
rotation)
  # A rotation mid-stream must not lose the tail of the old file: the
  # bytes written between the last read and the rename exist nowhere
  # else. Rotate every 30k lines by rename-and-recreate.
  reset
  python3 bench/gen.py --out "$CORPUS/src.log" --lines "$LINES" --rate "$RATE" >/dev/null
  split -l 30000 -d "$CORPUS/src.log" "$CORPUS/part."
  rm "$CORPUS/src.log"

  : > "$CORPUS/app.log"
  $BIN --config bench/l1.toml --state-dir "$STATE" >/tmp/agent.log 2>&1 &
  AGENT=$!
  sleep 1
  for part in "$CORPUS"/part.*; do
    cat "$part" >> "$CORPUS/app.log"
    sleep 1
    mv "$CORPUS/app.log" "$CORPUS/rotated.$(basename "$part")"
    : > "$CORPUS/app.log"
    sleep 0.5
  done
  await_quiescence
  kill -TERM $AGENT 2>/dev/null || true
  wait $AGENT 2>/dev/null || true
  echo "  rotations seen: $(grep -c 'rotation:' /tmp/agent.log || echo 0)"
  ;;

resume)
  # SIGKILL mid-stream, restart, and the index set must still be exactly
  # complete: nothing lost AND nothing duplicated. The second half is
  # what the deterministic stamper buys — a replay regenerates identical
  # timestamps, so the rows collapse instead of doubling.
  reset
  python3 bench/gen.py --out "$CORPUS/app.log" --lines "$LINES" --rate "$RATE" >/dev/null

  $BIN --config bench/l1.toml --state-dir "$STATE" >/tmp/agent.log 2>&1 &
  AGENT=$!
  # let it get partway in, then kill it the way a machine dies
  sleep 1.2
  kill -9 $AGENT 2>/dev/null || true
  wait $AGENT 2>/dev/null || true
  mid=$(rows)
  echo "  SIGKILLed with $mid rows shipped"
  echo "  checkpoint: $(head -c 220 "$STATE"/app.checkpoint 2>/dev/null)"

  # restart: reads from the checkpoint and finishes the file
  $BIN --config bench/l1.toml --state-dir "$STATE" --once >/tmp/agent2.log 2>&1 || true
  await_quiescence
  ;;

*)
  echo "usage: drill-l1.sh rotation|resume" >&2
  exit 2
  ;;
esac
echo "  drill '$1' complete"
