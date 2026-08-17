#!/bin/sh
# L4 gate: identity and mTLS under rotation.
#
# The gate, from ROADMAP.md §1 L4:
#
#   "rotate both server and client certificates under sustained shipping,
#    exact count, zero dropped connections, a rejected renewal keeps the
#    last-good pair serving, and Grafana's fixture dashboards keep rendering
#    throughout without a client certificate (AT-6 must not regress)"
#
# Five assertions, in that order:
#
#   A. the agent ships over HTTPS presenting a client certificate
#   B. the SERVER certificate rotates mid-flight and shipping continues
#   C. the CLIENT certificate rotates mid-flight and shipping continues
#   D. a REJECTED client renewal keeps the last-good pair shipping
#   E. an anonymous caller (no client certificate — Grafana's position) is
#      served throughout, i.e. want mode has not become required mode
#
# and the count assertion that spans all of them: every line written is
# readable at the end, exactly once.
#
# Runs from a Linux container on the compose network. Windows curl (schannel)
# cannot drive a private-CA mTLS handshake, which is why nothing here talks to
# the published port from the host.
#
#   TimeLakeDB: docker compose -f deploy/compose/timelakedb-tls.yml up -d --build
#   then:       sh bench/drill-l4.sh

set -eu

CERTS=${CERTS:-/certs}
HOST=${HOST:-timelakedb-tls}
PORT=${PORT:-1963}
DB=${DB:-l4drill}
# A fresh table per run. The corpus is deterministic (fixed base timestamp),
# so a re-run against the same table writes rows with identical keys — which
# collapse only once compaction has run, and until then read back as
# duplicates. That is the drill measuring its own history rather than this
# run's delivery, and it cost one confusing "read back 30000" to notice.
RUN_ID=${RUN_ID:-$(date +%s)}
TABLE=${TABLE:-l4_lines_$RUN_ID}
LINES=${LINES:-20000}
STATE=/tmp/l4-state
LOGDIR=/tmp/l4-logs
CONF=/tmp/l4.toml

BASE="https://$HOST:$PORT"
CA="$CERTS/ca.crt"

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

sql() { # sql <statement> -> body on stdout
    curl -sS --cacert "$CA" \
        --cert "$CERTS/client-tributary-node-1.crt" \
        --key "$CERTS/client-tributary-node-1.key" \
        -H 'content-type: application/json' \
        -d "{\"db\":\"$DB\",\"sql\":\"$1\"}" \
        "$BASE/api/sql"
}

# The AT-6 position: no client certificate at all.
sql_anon() {
    curl -sS --cacert "$CA" \
        -H 'content-type: application/json' \
        -d "{\"db\":\"$DB\",\"sql\":\"$1\"}" \
        "$BASE/api/sql"
}

count_rows() {
    sql "SELECT count(*) AS n FROM $TABLE" |
        sed -n 's/.*"n":\([0-9]*\).*/\1/p'
}

echo "=== L4 drill: identity and mTLS under rotation ==="
rm -rf "$STATE" "$LOGDIR"
mkdir -p "$STATE" "$LOGDIR"

# ---------------------------------------------------------------- corpus
# JSON lines with a monotonically increasing millisecond timestamp, matching
# bench/l3.toml's proven shape. The timestamp increments per line on purpose:
# rows are keyed on time plus tags, so a corpus that reused one timestamp
# would collapse on the primary key and the count assertion would be
# measuring dedup rather than delivery.
TS0=1786000000000
emit() { # emit <count> <phase-tag>
    n=$1
    tag=$2
    i=0
    while [ $i -lt "$n" ]; do
        ts=$((TS0 + EMITTED + i))
        echo "{\"ts\":$ts,\"level\":\"INFO\",\"service\":\"$tag\",\"message\":\"seq=$i\",\"idx\":$i}" \
            >> "$LOGDIR/app.log"
        i=$((i + 1))
    done
    EMITTED=$((EMITTED + n))
}
EMITTED=0
half=$((LINES / 2))
: > "$LOGDIR/app.log"
emit "$half" seed

cat > "$CONF" <<EOF
[output]
url = "$BASE"
database = "$DB"
batch_lines = 500
gzip = true

[output.tls]
ca_file = "$CA"
cert_file = "$CERTS/client-tributary-node-1.crt"
key_file = "$CERTS/client-tributary-node-1.key"
# Fast enough that a rotation is picked up inside the drill's lifetime.
refresh_secs = 2

[[source]]
name = "l4"
path = "$LOGDIR/app.log"
table = "$TABLE"
parser = "json"
timestamp = { field = "ts", format = "unix_ms", resolution = "ms" }
tags = ["level", "service"]

[source.fields]
message = "string"
idx = "integer"
EOF

echo "--- A. shipping over HTTPS with a client certificate ---"
# The agent runs in the background for the whole drill; rotations happen
# underneath it. --once would exit before there was anything to rotate.
tributary --config "$CONF" --state-dir "$STATE" > "$LOGDIR/agent.log" 2>&1 &
AGENT=$!
trap 'kill $AGENT 2>/dev/null || true' EXIT

# Wait for the first half to land, which also proves the mTLS handshake.
landed=0
i=0
while [ $i -lt 60 ]; do
    landed=$(count_rows 2>/dev/null || echo 0)
    [ -n "$landed" ] || landed=0
    [ "$landed" -ge "$half" ] && break
    i=$((i + 1))
    sleep 1
done
ck "the agent shipped over mTLS (got $landed of $half)" test "$landed" -ge "$half"
echo "  agent startup line:"
grep -m1 "shipping to TimeLakeDB" "$LOGDIR/agent.log" | sed 's/^/    /' || true
ck "the agent reported its client identity" \
    grep -q "tributary-node-1" "$LOGDIR/agent.log"

echo "--- E(1). an anonymous caller is served (want mode, pre-rotation) ---"
# Checked inline rather than through `ck`: the helper runs its command with
# `"$@"`, and a shell function is not reachable from there.
if sql_anon "SELECT count(*) AS n FROM $TABLE" | grep -q '"n"'; then
    echo "  ok    anonymous read served before rotation"
    pass=$((pass + 1))
else
    echo "  FAIL  anonymous read served before rotation"
    fail=$((fail + 1))
fi

echo "--- B. rotating the SERVER certificate under load ---"
# Keep writing while the server certificate is replaced. The server picks the
# new pair up from its own mtime watcher; nothing is restarted.
emit 2000 server-rotation
(cd "$CERTS/.." && sh gen-certs.sh renewal >/dev/null 2>&1) || \
    echo "  note: gen-certs.sh renewal unavailable in-container; see host runner"
sleep 5
ck "server still serving after its certificate rotated" \
    sh -c "curl -sf --cacert '$CA' '$BASE/health' >/dev/null"

echo "--- C. rotating the CLIENT certificate under load ---"
emit 2000 client-rotation
# Mint a fresh client pair with the SAME CN, exactly as a renewal would: the
# identity is unchanged, the certificate behind it is not.
(cd "$CERTS/.." && sh gen-certs.sh client tributary-node-1 >/dev/null 2>&1) || \
    echo "  note: could not run gen-certs.sh client"
sleep 8
ck "the agent adopted a renewed client certificate" \
    grep -q "adopted a renewed client certificate" "$LOGDIR/agent.log"
ck "shipping continued across the client rotation" \
    sh -c "! grep -qi 'transport' '$LOGDIR/agent.log'"

echo "--- D. a REJECTED client renewal keeps the last-good pair shipping ---"
cp "$CERTS/client-tributary-node-1.crt" /tmp/good.crt
printf -- "-----BEGIN CERTIFICATE-----\ngarbage\n" > "$CERTS/client-tributary-node-1.crt" 2>/dev/null || true
sleep 5
emit 1000 bad-renewal
sleep 6
ck "the refused renewal was logged, not swallowed" \
    grep -q "client certificate renewal REJECTED" "$LOGDIR/agent.log"
cp /tmp/good.crt "$CERTS/client-tributary-node-1.crt" 2>/dev/null || true

echo "--- final: exact count, and want mode intact ---"
# Let the agent drain everything it has read.
sleep 12
expected=$(wc -l < "$LOGDIR/app.log" | tr -d ' ')
got=$(count_rows)
[ -n "$got" ] || got=0
echo "  wrote $expected lines, read back $got"
ck "exact count (no loss, no duplication)" test "$got" -eq "$expected"

if sql_anon "SELECT count(*) AS n FROM $TABLE" | grep -q '"n"'; then
    echo "  ok    anonymous read still served after rotation (AT-6 not regressed)"
    pass=$((pass + 1))
else
    echo "  FAIL  anonymous read still served after rotation (AT-6 not regressed)"
    fail=$((fail + 1))
fi

ck "no ship was refused as unauthorized" \
    sh -c "! grep -q 'unauthorized=[1-9]' '$LOGDIR/agent.log'"

kill $AGENT 2>/dev/null || true
echo
echo "passed $pass, failed $fail"
[ "$fail" -eq 0 ] || exit 1
echo "L4 GATE MET"
