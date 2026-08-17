#!/bin/sh
# P1-7: measure the RPO, rather than assert it.
#
# The queue is node-local durability, not replication. ROADMAP §5 open
# question 2 says to MEASURE that before recommending anything, so this
# drill puts a number on it under two failure models that are usually
# conflated:
#
#   RESTART  the process dies, the node and its disk come back. The
#            checkpoint and the queue survive, so nothing should be lost —
#            this is L1's property, re-checked here because P1-7's whole
#            claim rests on the two cases being different.
#
#   NODE LOSS  the node vanishes: spot eviction, terminated container with
#            an emptyDir. Everything unacked goes with it — the batch being
#            assembled, the batches in flight, the queue, AND the source
#            log file itself. That loss is the RPO.
#
# Both are measured at the default profile and at a low-RPO profile, so the
# trade is a table rather than an adjective.
#
#   TimeLakeDB: docker compose -f deploy/compose/timelakedb.yml up -d
#   then:       sh bench/drill-p17.sh

set -eu

# The agent's log fields are ANSI-coloured by default, which puts escape
# codes between `pending_lines` and `=` and makes the peak-exposure parse
# below silently find nothing (it reported 0 until this was added). It also
# makes the evidence file unreadable.
NO_COLOR=1
export NO_COLOR

HOST=${HOST:-timelakedb}
PORT=${PORT:-1963}
DB=${DB:-p17drill}
RATE=${RATE:-2000}        # lines per second written to the log
SECONDS_OF_LOAD=${SECONDS_OF_LOAD:-10}
BASE="http://$HOST:$PORT"
# A fresh table per run, for the same reason drill-l4.sh needs one: the
# corpus is deterministic, so a re-run writes rows with identical keys, and
# cross-file duplicates collapse only at compaction. Without this the counts
# are cumulative across runs and the arithmetic goes negative, which is at
# least loud — a smaller overlap would just quietly overstate delivery.
RUN=${RUN:-$(date +%s)}

sql() {
    curl -sS -H 'content-type: application/json' \
        -d "{\"db\":\"$DB\",\"sql\":\"$1\"}" "$BASE/api/sql"
}
count_rows() { # count_rows <table>
    sql "SELECT count(*) AS n FROM $1" | sed -n 's/.*"n":\([0-9]*\).*/\1/p'
}

# Feed a file at a steady rate in the background, so the agent is genuinely
# behind rather than draining a static file it has already caught up with.
# A static corpus would measure nothing: the interesting state is the
# backlog that exists while data is arriving.
feed() { # feed <file> <lines> <rate> <table-tag>
    f=$1; n=$2; rate=$3; tag=$4
    i=0
    per_tick=$((rate / 10))
    [ "$per_tick" -lt 1 ] && per_tick=1
    while [ $i -lt "$n" ]; do
        j=0
        while [ $j -lt $per_tick ] && [ $i -lt "$n" ]; do
            echo "{\"ts\":$((1786000000000 + i)),\"level\":\"INFO\",\"service\":\"$tag\",\"message\":\"seq=$i\",\"idx\":$i}" >> "$f"
            i=$((i + 1)); j=$((j + 1))
        done
        sleep 0.1
    done
}

write_conf() { # write_conf <path> <logfile> <table> <batch> <inflight> <queue_bytes>
    cat > "$1" <<EOF
[output]
url = "$BASE"
database = "$DB"
batch_lines = $4
max_inflight = $5
queue_max_bytes = $6
rpo_report_secs = 1

[[source]]
name = "p17"
path = "$2"
table = "$3"
parser = "json"
timestamp = { field = "ts", format = "unix_ms", resolution = "ms" }
tags = ["level", "service"]

[source.fields]
message = "string"
idx = "integer"
EOF
}

# ---------------------------------------------------------------------------
# One measurement: run under load, kill -9 mid-stream, and compare what the
# server acked against what was written by that moment.
#
# kill -9 with no restart and no drain IS the node-loss model: the agent gets
# no chance to flush, exactly as it would not on an evicted instance.
# ---------------------------------------------------------------------------
measure_node_loss() { # measure_node_loss <label> <table> <batch> <inflight> <rate>
    label=$1; table=$2; batch=$3; inflight=$4; rate=${5:-$RATE}
    log=/tmp/p17-$table.log
    conf=/tmp/p17-$table.toml
    state=/tmp/p17-state-$table
    rm -rf "$state" "$log"; mkdir -p "$state"; : > "$log"
    write_conf "$conf" "$log" "$table" "$batch" "$inflight" 536870912

    total=$((rate * SECONDS_OF_LOAD))
    feed "$log" "$total" "$rate" "$table" &
    FEEDER=$!

    tributary --config "$conf" --state-dir "$state" > "/tmp/p17-agent-$table.log" 2>&1 &
    AGENT=$!

    # Let it reach steady state, then pull the plug mid-stream.
    sleep $((SECONDS_OF_LOAD / 2))
    written_at_kill=$(wc -l < "$log" | tr -d ' ')
    kill -9 $AGENT 2>/dev/null || true
    kill $FEEDER 2>/dev/null || true
    wait $AGENT 2>/dev/null || true
    wait $FEEDER 2>/dev/null || true

    # Everything already acked is durable server-side; give the server a
    # moment to make the last accepted batch visible.
    sleep 3
    acked=$(count_rows "$table"); [ -n "$acked" ] || acked=0
    lost=$((written_at_kill - acked))
    [ "$lost" -lt 0 ] && lost=0
    rpo_ms=$((lost * 1000 / rate))

    # The CONFIG BOUND: the most that can be unacked at any instant.
    #   pending (<= batch_lines) + in-flight (max_inflight * batch_lines)
    # The queue adds to this whenever the server is refusing writes; it is
    # zero here because the server stayed up, which is the point — the queue
    # is the outage buffer, not the steady-state exposure.
    bound=$((batch + inflight * batch))
    bound_ms=$((bound * 1000 / rate))

    # The PEAK OBSERVED exposure, read back out of the agent's own "at risk"
    # lines. This is the better statistic: the kill lands at one arbitrary
    # point on a sawtooth, so a single sample says as much about timing as
    # about configuration.
    peak=$(awk -v b="$batch" '
        /at risk if this node is lost now/ {
            p = 0; f = 0
            for (i = 1; i <= NF; i++) {
                if ($i ~ /pending_lines=/)     { split($i, a, "="); p = a[2] + 0 }
                if ($i ~ /inflight_batches=/)  { split($i, a, "="); f = a[2] + 0 }
            }
            e = p + f * b
            if (e > max) max = e
        }
        END { print max + 0 }' "/tmp/p17-agent-$table.log")
    peak_ms=$((peak * 1000 / rate))

    echo "  $label"
    echo "    written at kill : $written_at_kill"
    echo "    acked (durable) : $acked"
    echo "    lost, this kill : $lost lines (~${rpo_ms} ms) — ONE sample of a sawtooth"
    echo "    peak observed   : $peak lines (~${peak_ms} ms) — highest at-risk seen during the run"
    echo "    CONFIG BOUND    : $bound lines (~${bound_ms} ms) — batch*(1+inflight), the number to quote"
    RESULT_LOST=$lost
    RESULT_RPO_MS=$rpo_ms
    RESULT_PEAK=$peak
    RESULT_BOUND=$bound
}

# ---------------------------------------------------------------------------
# The other failure model: the process dies but the node does not. The
# checkpoint and queue are still on disk, so a restart must lose nothing.
# ---------------------------------------------------------------------------
measure_restart() { # measure_restart <table>
    table=$1
    log=/tmp/p17-$table.log
    conf=/tmp/p17-$table.toml
    state=/tmp/p17-state-$table
    rm -rf "$state" "$log"; mkdir -p "$state"; : > "$log"
    write_conf "$conf" "$log" "$table" 2000 4 536870912

    total=$((RATE * 4))
    feed "$log" "$total" "$RATE" "$table" &
    FEEDER=$!
    tributary --config "$conf" --state-dir "$state" > "/tmp/p17-agent-$table.log" 2>&1 &
    AGENT=$!
    sleep 2
    kill -9 $AGENT 2>/dev/null || true
    wait $AGENT 2>/dev/null || true
    wait $FEEDER 2>/dev/null || true

    # Same node, same disk: restart and let it drain to completion.
    written=$(wc -l < "$log" | tr -d ' ')
    tributary --config "$conf" --state-dir "$state" --once >> "/tmp/p17-agent-$table.log" 2>&1 || true
    sleep 3
    acked=$(count_rows "$table"); [ -n "$acked" ] || acked=0
    echo "  restart on a surviving disk"
    echo "    written         : $written"
    echo "    acked after restart: $acked"
    if [ "$acked" -eq "$written" ]; then
        echo "    RPO             : 0 lines — the checkpoint resumed exactly"
        RESTART_OK=1
    else
        echo "    RPO             : $((written - acked)) lines — EXPECTED 0"
        RESTART_OK=0
    fi
}

echo "=== P1-7: measuring the queue's real RPO ==="
echo "rate=${RATE}/s, load=${SECONDS_OF_LOAD}s, server=$BASE"
echo

echo "--- failure model 1: the node comes back (durable disk) ---"
measure_restart "p17_restart_$RUN"
echo

echo "--- failure model 2: the node is lost (nothing comes back) ---"
echo
echo "  At the FULL rate (${RATE}/s):"
measure_node_loss "default profile (batch 2000, inflight 4)"     "p17_default_$RUN" 2000 4 "$RATE"
D_LOST=$RESULT_LOST; D_PEAK=$RESULT_PEAK; D_BOUND=$RESULT_BOUND
echo
measure_node_loss "small-batch profile (batch 100, inflight 1)"     "p17_small_$RUN" 100 1 "$RATE"
S_LOST=$RESULT_LOST; S_PEAK=$RESULT_PEAK; S_BOUND=$RESULT_BOUND

# The same two profiles at a rate the small-batch one can actually sustain.
# This is the control that turns a surprising number into an explanation:
# if small batches only lose more because they cannot keep up, then at a
# rate they CAN keep up with they must lose less.
SLOW=$((RATE / 10))
echo
echo "  At a rate the small-batch profile can sustain (${SLOW}/s):"
measure_node_loss "default profile (batch 2000, inflight 4)"     "p17_default_slow_$RUN" 2000 4 "$SLOW"
DS_LOST=$RESULT_LOST; DS_PEAK=$RESULT_PEAK; DS_BOUND=$RESULT_BOUND
echo
measure_node_loss "small-batch profile (batch 100, inflight 1)"     "p17_small_slow_$RUN" 100 1 "$SLOW"
SS_LOST=$RESULT_LOST; SS_PEAK=$RESULT_PEAK; SS_BOUND=$RESULT_BOUND

echo
echo "=== the trade, measured ==="
printf '  %-36s %7s %8s %8s %8s
' "profile" "rate/s" "sample" "peak" "BOUND"
printf '  %-36s %7s %8s %8s %8s
' "default     (batch 2000, inflight 4)" "$RATE" "$D_LOST" "$D_PEAK" "$D_BOUND"
printf '  %-36s %7s %8s %8s %8s
' "small-batch (batch 100,  inflight 1)" "$RATE" "$S_LOST" "$S_PEAK" "$S_BOUND"
printf '  %-36s %7s %8s %8s %8s
' "default     (batch 2000, inflight 4)" "$SLOW" "$DS_LOST" "$DS_PEAK" "$DS_BOUND"
printf '  %-36s %7s %8s %8s %8s
' "small-batch (batch 100,  inflight 1)" "$SLOW" "$SS_LOST" "$SS_PEAK" "$SS_BOUND"
echo
echo "  sample = lines lost by ONE kill. It lands at an arbitrary point on the"
echo "           flush sawtooth, so it is an anecdote, not the RPO."
echo "  peak   = highest at-risk total the agent itself reported during the run."
echo "  BOUND  = batch_lines * (1 + max_inflight): the most that can EVER be"
echo "           unacked in steady state. This is the number to put in an SLO."
echo
[ "${RESTART_OK:-0}" -eq 1 ] || { echo "FAIL: restart lost data — L1's property regressed"; exit 1; }
echo "restart on a surviving disk: RPO 0 (L1 property holds)"
echo "node loss: RPO <= batch_lines * (1 + max_inflight), plus the queue if the"
echo "server was refusing writes. The knobs set that bound; the sample does not."
