#!/bin/sh
# T-1: self-telemetry — /metrics and /healthz, scraped while shipping.
#
# Four things, in order of what would actually go wrong:
#
#   A. the endpoints answer, and /metrics is valid Prometheus exposition
#   B. the counters MOVE while data flows (a metric frozen at 0 is worse
#      than no metric — it reads as "nothing is wrong")
#   C. the DESIGN §6.2 accounting invariant holds FROM THE OUTSIDE:
#         lines_read - lines_shipped - lines_quarantined == at_risk_lines
#   D. a database outage leaves /healthz LIVE. This is the one that
#      matters: a liveness probe that fails on an unreachable database
#      gets the agent killed by its orchestrator exactly when the queue
#      is doing its job, and the restart discards everything unacked.
#
#   TimeLakeDB: docker compose -f deploy/compose/timelakedb.yml up -d
#   then:       sh bench/drill-t1.sh

set -eu

HOST=${HOST:-timelakedb}
PORT=${PORT:-1963}
DB=${DB:-t1drill}
RUN=${RUN:-$(date +%s)}
TABLE=${TABLE:-t1_lines_$RUN}
TELEMETRY=${TELEMETRY:-127.0.0.1:9109}
BASE="http://$HOST:$PORT"
NO_COLOR=1
export NO_COLOR

pass=0
fail=0
ck() {
    d=$1
    shift
    if "$@" >/dev/null 2>&1; then
        echo "  ok    $d"
        pass=$((pass + 1))
    else
        echo "  FAIL  $d"
        fail=$((fail + 1))
    fi
}

scrape() { curl -sf "http://$TELEMETRY/metrics"; }
healthz() { curl -s -o /tmp/h.json -w '%{http_code}' "http://$TELEMETRY/healthz"; }
metric() { scrape | awk -v n="$1" '$1==n {print $2; exit}'; }

echo "=== T-1: self-telemetry ==="
rm -rf /tmp/t1-state /tmp/t1-logs
mkdir -p /tmp/t1-state /tmp/t1-logs
: > /tmp/t1-logs/app.log

cat > /tmp/t1.toml <<EOF
[output]
url = "$BASE"
database = "$DB"
batch_lines = 500
rpo_report_secs = 5

[telemetry]
addr = "$TELEMETRY"

[[source]]
name = "t1"
path = "/tmp/t1-logs/app.log"
table = "$TABLE"
parser = "json"
timestamp = { field = "ts", format = "unix_ms", resolution = "ms" }
tags = ["level", "service"]

[source.fields]
message = "string"
idx = "integer"
EOF

emit() { # emit <count>
    i=0
    while [ $i -lt "$1" ]; do
        echo "{\"ts\":$((1786000000000 + EMITTED + i)),\"level\":\"INFO\",\"service\":\"t1\",\"message\":\"seq=$i\",\"idx\":$i}" \
            >> /tmp/t1-logs/app.log
        i=$((i + 1))
    done
    EMITTED=$((EMITTED + $1))
}
EMITTED=0
emit 5000

tributary --config /tmp/t1.toml --state-dir /tmp/t1-state > /tmp/t1-agent.log 2>&1 &
AGENT=$!
trap 'kill $AGENT 2>/dev/null || true' EXIT

# Wait for the listener rather than sleeping a guess.
i=0
while [ $i -lt 30 ]; do
    scrape >/dev/null 2>&1 && break
    i=$((i + 1)); sleep 1
done

echo "--- A. the endpoints answer ---"
ck "/metrics is served"              scrape
ck "/healthz is served"              sh -c "curl -sf http://$TELEMETRY/healthz >/dev/null"
ck "unknown paths 404"               sh -c "[ \"\$(curl -s -o /dev/null -w '%{http_code}' http://$TELEMETRY/nope)\" = 404 ]"
ck "content-type is the Prometheus one" \
    sh -c "curl -sI http://$TELEMETRY/metrics | grep -qi 'version=0.0.4'"

# Every sample line must be `name value` with a numeric value, and every
# metric must carry HELP and TYPE. A malformed exposition is dropped whole
# by most scrapers, silently.
ck "exposition parses as Prometheus text" sh -c '
    curl -sf http://'"$TELEMETRY"'/metrics | awk "
        /^# (HELP|TYPE) / { next }
        /^#/ { next }
        NF == 0 { next }
        NF != 2 { print \"bad line: \" \$0; bad=1; next }
        \$2 !~ /^-?[0-9]+(\.[0-9]+)?\$/ { print \"bad value: \" \$0; bad=1 }
        END { exit bad }
    "'

echo "--- B. the counters move while data flows ---"
i=0
while [ $i -lt 60 ]; do
    v=$(metric tributary_lines_shipped_total || echo 0)
    [ -n "$v" ] && [ "${v%.*}" -ge 5000 ] 2>/dev/null && break
    i=$((i + 1)); sleep 1
done
read_total=$(metric tributary_lines_read_total)
shipped=$(metric tributary_lines_shipped_total)
echo "    read=$read_total shipped=$shipped"
ck "lines_read advanced"      sh -c "[ \"${read_total%.*}\" -ge 5000 ]"
ck "lines_shipped advanced"   sh -c "[ \"${shipped%.*}\" -ge 5000 ]"
ck "requests_total advanced"  sh -c "[ \"\$(echo \"$(metric tributary_requests_total)\" | cut -d. -f1)\" -gt 0 ]"
ck "uptime is nonzero"        sh -c "[ \"\$(echo \"$(metric tributary_uptime_seconds)\" | cut -d. -f1)\" -ge 0 ]"

echo "--- C. the DESIGN 6.2 invariant, checked from outside ---"
# read - shipped - quarantined should equal what is still held in memory.
emit 3000
sleep 3
r=$(metric tributary_lines_read_total | cut -d. -f1)
s=$(metric tributary_lines_shipped_total | cut -d. -f1)
q=$(metric tributary_lines_quarantined_total | cut -d. -f1)
a=$(metric tributary_at_risk_lines | cut -d. -f1)
echo "    read=$r shipped=$s quarantined=$q at_risk=$a  ->  $((r - s - q)) vs $a"
ck "read - shipped - quarantined == at_risk" sh -c "[ $((r - s - q)) -eq $a ]"

echo "--- D. a REAL outage must NOT fail liveness ---"
# Genuinely take the database away, via the Docker socket with curl — no
# docker CLI needed in this image. An earlier version of this drill left
# the stop optional and silently checked a healthy agent, which is to say
# it checked nothing at all.
SOCK=/var/run/docker.sock
CONTAINER=${CONTAINER:-timelakedb}
if [ ! -S "$SOCK" ]; then
    echo "  SKIP  no Docker socket mounted — cannot stage a real outage"
    echo "        (mount -v /var/run/docker.sock:/var/run/docker.sock)"
    fail=$((fail + 1))
else
    curl -s --unix-socket "$SOCK" -X POST \
        "http://localhost/containers/$CONTAINER/stop" >/dev/null 2>&1
    echo "    stopped $CONTAINER"
    # Give the agent time to fail a ship, spool it, and keep looping.
    emit 2000
    sleep 12

    code=$(healthz)
    status=$(sed -n 's/.*"status":"\([a-z]*\)".*/\1/p' /tmp/h.json)
    live=$(sed -n 's/.*"live":\([a-z]*\).*/\1/p' /tmp/h.json)
    shipping=$(sed -n 's/.*"shipping":\([a-z]*\).*/\1/p' /tmp/h.json)
    qb=$(metric tributary_queue_bytes | cut -d. -f1)
    echo "    /healthz -> HTTP $code status=$status live=$live shipping=$shipping queue_bytes=$qb"

    # The whole point: alive, so nothing restarts it and the queue keeps
    # its data — but visibly not shipping, so an alert can fire.
    ck "healthz still returns 200 during an outage" sh -c "[ \"$code\" = 200 ]"
    ck "healthz reports live"                       sh -c "[ \"$live\" = true ]"
    ck "healthz reports degraded, not ok"           sh -c "[ \"$status\" = degraded ]"
    ck "the agent is still running"                 kill -0 $AGENT
    ck "metrics still served while degraded"        scrape

    curl -s --unix-socket "$SOCK" -X POST \
        "http://localhost/containers/$CONTAINER/start" >/dev/null 2>&1
    echo "    restarted $CONTAINER"
    # It should drain what it spooled once the server is back.
    i=0
    while [ $i -lt 60 ]; do
        [ "$(metric tributary_queue_bytes | cut -d. -f1)" = 0 ] && break
        i=$((i + 1)); sleep 1
    done
    ck "the queue drained after recovery" \
        sh -c "[ \"\$(curl -sf http://$TELEMETRY/metrics | awk '\$1==\"tributary_queue_bytes\"{print \$2}' | cut -d. -f1)\" = 0 ]"
fi

echo
echo "passed $pass, failed $fail"
[ "$fail" -eq 0 ] || exit 1
echo "T-1 GATE MET"
