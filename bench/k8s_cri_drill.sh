#!/bin/bash
# CRI text-parser end-to-end drill (#71). Feeds real containerd-format container
# logs through a glob + kubernetes + parser="cri" source — the actual DaemonSet
# config — and proves the parser does what `parser = "plain"` could not:
#
#   * records are stamped at the LOG's own time, not ingestion time,
#   * stdout/stderr become the `stream` tag,
#   * a >16 KB line the kubelet split into P/P/F reassembles to ONE record,
#   * pod/namespace/container enrichment (phase 1) still works.
#
# The corpus is dated 2026-01-01, so an ingestion-time fallback would stamp it
# ~now and the first assertion would fail — that's the discriminator.
#
#   docker run --rm -v "$PWD:/w" -w /w \
#     -v rk-cargo-registry:/usr/local/cargo/registry \
#     rust:1-slim bash -c 'apt-get update -qq && apt-get install -y -qq python3 \
#       >/dev/null; bench/k8s_cri_drill.sh'
set -u
export NO_COLOR=1
FAIL=0
check() { if [ "$2" = "$3" ]; then echo "  [PASS] $1 ($2)"; else echo "  [FAIL] $1 (got $2, want $3)"; FAIL=1; fi; }

BENCH=$(cd "$(dirname "$0")" && pwd)
cd "$BENCH/.."
WORK=$(mktemp -d)
DIR="$WORK/containers"; mkdir -p "$DIR"
ID="1111111111111111111111111111111111111111111111111111111111111111"
FILE="$DIR/web-7d9c8b_shop_server-$ID.log"   # pod web-7d9c8b ns shop container server
N=200

echo "=== CRI text-parser drill ($(date -u +%Y-%m-%dT%H:%M:%SZ)) ==="
echo "workdir=$WORK  file=$FILE  lines=$N + one 40KB split line"

echo "-- build --"
cargo build -p tributary 2>&1 | tail -1
BIN=target/debug/tributary

# Generate a real CRI-format container log: N small lines (alternating
# stdout/stderr, each a distinct 2026-01-01 second) and one 40 KB line split
# into ~16 KB P entries terminated by an F, bracketed with markers so the assert
# can tell "reassembled to one record" from "shredded into three".
python3 - "$FILE" "$N" <<'PY'
import datetime, sys
out, n = sys.argv[1], int(sys.argv[2])
base = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
def t(i): return (base + datetime.timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%S.000000000Z")
lines = []
for i in range(n):
    lines.append(f"{t(i)} {'stdout' if i % 2 == 0 else 'stderr'} F msg-{i}")
big = "BIGSTART" + "x" * 40000 + "BIGEND"
chunks = [big[j:j+16000] for j in range(0, len(big), 16000)]
tb = t(n)
for c in chunks[:-1]:
    lines.append(f"{tb} stdout P {c}")
lines.append(f"{tb} stdout F {chunks[-1]}")
open(out, "w").write("\n".join(lines) + "\n")
PY
echo "  wrote $(wc -l < "$FILE") physical lines ($(( $(ls -la "$FILE" | awk '{print $5}') )) bytes)"

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
RECV="$WORK/recv.lp"; : > "$RECV"
python3 "$WORK/mock.py" "$RECV" & MOCK=$!; sleep 0.5

# The DaemonSet config, essentially: glob + kubernetes + parser=cri.
cat > "$WORK/k8s.toml" <<EOF
[output]
url = "http://127.0.0.1:8899"
database = "logs"
gzip = false
batch_lines = 500

[[source]]
name = "k8s"
path = "$DIR/*.log"
table = "kube"
parser = "cri"
timestamp = { field = "time", format = "rfc3339", resolution = "ns" }
tags = ["stream"]

[source.fields]
log = "string"

[source.kubernetes]
EOF

STATE="$WORK/state"; mkdir -p "$STATE"
echo "-- run (parser=cri) --"
"$BIN" --config "$WORK/k8s.toml" --state-dir "$STATE" --once >"$WORK/run.log" 2>&1 || true
sleep 1
got=$(grep -c "^kube," "$RECV" 2>/dev/null || echo 0)
echo "  delivered $got kube records (expected $((N + 1)): $N small + 1 reassembled)"
check "record count" "$got" "$((N + 1))"

echo
echo "### CRI PARSER ASSERTION ###"
BEFORE=$FAIL
python3 bench/k8s_cri_assert.py "$RECV" kube web-7d9c8b shop server || FAIL=1
check "cri: event-time, stream tag, P/F reassembly, enrichment" "$FAIL" "$BEFORE"

kill "$MOCK" 2>/dev/null; wait "$MOCK" 2>/dev/null
echo
if [ "$FAIL" = 0 ]; then echo "=== ALL PASS ==="; else echo "=== FAILURES ==="; fi
rm -rf "$WORK"
exit "$FAIL"
