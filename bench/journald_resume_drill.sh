#!/bin/bash
# journald crash-exact resume drill (#23, acceptance criterion 2).
#
# Proves that a journald source, hard-SIGKILLed mid-run and restarted, resumes
# from its saved cursor with NO GAP and NO DUPE — the cursor, not a byte
# offset, is the checkpoint. Runs INSIDE a container that has libsystemd,
# systemd-journald and python3; standalone journald needs no --privileged:
#
#   docker run --rm -v "$PWD:/w" -w /w -v rk-cargo-registry:/usr/local/cargo/registry \
#     rust:1-slim bash -c 'apt-get update -qq && apt-get install -y -qq \
#       libsystemd-dev systemd python3 >/dev/null; bench/journald_resume_drill.sh'
set -u
FAIL=0
check(){ if [ "$2" = "$3" ]; then echo "  [PASS] $1 ($2)"; else echo "  [FAIL] $1 (got $2, want $3)"; FAIL=1; fi; }

WORK=$(mktemp -d)
echo "=== journald crash-exact resume drill ($(date -u +%Y-%m-%dT%H:%M:%SZ)) ==="
echo "workdir=$WORK"

echo "-- build --features journald --"
cargo build -p tributary --features journald 2>&1 | tail -1
BIN=target/debug/tributary

# Standalone journald (no full systemd, no --privileged).
mkdir -p /run/systemd/journal /run/log/journal /var/log/journal
/usr/lib/systemd/systemd-journald & JDPID=$!
sleep 2
if kill -0 $JDPID 2>/dev/null; then echo "journald running (pid $JDPID)"; else echo "journald FAILED to start"; exit 1; fi

# A mock TimeLakeDB that records every /write body, so we can see exactly what
# was delivered (gzip off in the config so the body is plain line protocol).
cat > "$WORK/mock.py" <<'PYEOF'
import http.server, sys
LOG = sys.argv[1]
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get('content-length', 0))
        body = self.rfile.read(n)
        with open(LOG, 'ab') as f:
            f.write(body + b'\n')
        self.send_response(204); self.end_headers()
    def log_message(self, *a):
        pass
http.server.HTTPServer(('127.0.0.1', 8899), H).serve_forever()
PYEOF
RECV="$WORK/received.lp"; : > "$RECV"
python3 "$WORK/mock.py" "$RECV" & MOCKPID=$!
sleep 1

cat > "$WORK/trib.toml" <<EOF
[output]
url = "http://127.0.0.1:8899"
database = "t"
gzip = false
batch_lines = 10

[[source]]
name = "journald"
table = "t"
parser = "journald"
fields = { MESSAGE = "string" }
EOF
STATE="$WORK/state"; mkdir -p "$STATE"

distinct(){ grep -oE 'drill-[0-9]+' "$RECV" 2>/dev/null | sort -u | wc -l; }
total(){ grep -oE 'drill-[0-9]+' "$RECV" 2>/dev/null | wc -l; }

echo "-- writing entries drill-1 .. drill-100 into the journal --"
for i in $(seq 1 100); do echo "drill-$i" | systemd-cat -t drilltest; done
sleep 1

echo "-- run 1: agent reads + ships, then a hard kill -9 --"
$BIN --config "$WORK/trib.toml" --state-dir "$STATE" >/dev/null 2>&1 & AGENT=$!
for _ in $(seq 1 40); do [ "$(distinct)" -ge 100 ] && break; sleep 0.5; done
D1=$(distinct); echo "  run 1 delivered $D1 distinct drill messages before the kill"
kill -9 $AGENT 2>/dev/null; wait $AGENT 2>/dev/null

echo "-- writing entries drill-101 .. drill-200 WHILE the agent is dead --"
for i in $(seq 101 200); do echo "drill-$i" | systemd-cat -t drilltest; done
sleep 1

echo "-- run 2: restart from the saved cursor --"
$BIN --config "$WORK/trib.toml" --state-dir "$STATE" >/dev/null 2>&1 & AGENT2=$!
for _ in $(seq 1 40); do [ "$(distinct)" -ge 200 ] && break; sleep 0.5; done
kill -9 $AGENT2 2>/dev/null; wait $AGENT2 2>/dev/null

DISTINCT=$(distinct); TOTAL=$(total)
MISSING=0
for i in $(seq 1 200); do grep -q "drill-$i\"" "$RECV" || MISSING=$((MISSING+1)); done
RESUMED=$([ "$D1" -lt 200 ] && [ "$D1" -ge 1 ] && echo yes || echo no)

echo "-- verdict --"
check "every entry delivered across the crash (200 distinct)" "$DISTINCT" "200"
check "no entry missing (no gap)" "$MISSING" "0"
check "no duplicate delivered (total == distinct)" "$TOTAL" "$DISTINCT"
check "run 2 resumed past run 1 rather than replaying from head" "$RESUMED" "yes"

kill $MOCKPID $JDPID 2>/dev/null
if [ "$FAIL" = 0 ]; then
  echo "=== PASS: journald resumes crash-exact from its cursor — no gap, no dupe ==="
else
  echo "=== FAIL ==="
fi
exit $FAIL
