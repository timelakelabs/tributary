#!/bin/bash
# K8s cardinality drill (#8 phase 3 / #65) — the proof that the allowlist holds
# under Kubernetes churn: a container that restarts R times leaves R log files
# (R container-ids) but must produce ONE series, not R dead ones.
#
# The trap it exists to catch: the container-id is unbounded and sits in the
# log path, so a naive "stamp everything from the path" would tag it and blow
# cardinality up exactly the way pod labels would. The per-file STATE key keeps
# the id (so each restart resumes on its own); the `stream` TAG strips it.
#
# Two "deployments", each a FIXED pod/namespace/container churning through R
# container-ids => 2R files, but only 2 bounded identities. The assert
# (bench/k8s_cardinality_assert.py) demands distinct series == 2, not 2R, and
# that no tag value is ever a 64-hex id. NODE_NAME is set so the run also proves
# the Downward API node stamp (${NODE_NAME}) expands end to end.
#
#   docker run --rm -v "$PWD:/w" -w /w \
#     -v rk-cargo-registry:/usr/local/cargo/registry \
#     rust:1-slim bash -c 'apt-get update -qq && apt-get install -y -qq python3 \
#       >/dev/null; bench/k8s_cardinality_drill.sh'
set -u
export NO_COLOR=1
FAIL=0
check() { if [ "$2" = "$3" ]; then echo "  [PASS] $1 ($2)"; else echo "  [FAIL] $1 (got $2, want $3)"; FAIL=1; fi; }

BENCH=$(cd "$(dirname "$0")" && pwd)
cd "$BENCH/.."
WORK=$(mktemp -d)
DIR="$WORK/containers"; mkdir -p "$DIR"
R=50                 # restarts (container-ids) per deployment
K=200                # lines per revision
NODE="node-1"

echo "=== k8s cardinality drill ($(date -u +%Y-%m-%dT%H:%M:%SZ)) ==="
echo "workdir=$WORK  deployments=2  restarts_each=$R  files=$((2*R))  lines/rev=$K  node=$NODE"

echo "-- build --"
cargo build -p tributary 2>&1 | tail -1
BIN=target/debug/tributary

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

# One corpus per deployment, replayed into every revision file.
python3 bench/gen.py --out "$WORK/web.rev"  --lines "$K" --rate 10000 >/dev/null
python3 bench/gen.py --out "$WORK/api.rev"  --lines "$K" --rate 10000 >/dev/null

# A rolling restart: the SAME pod/ns/container, a fresh 64-hex container-id each
# time. `printf %064x` gives a distinct, well-formed id per revision.
for i in $(seq 1 "$R"); do
  id=$(printf '%064x' "$i")
  cp "$WORK/web.rev" "$DIR/web-59f8c_shop_server-$id.log"
  cp "$WORK/api.rev" "$DIR/api-7b3d2_billing_worker-$id.log"
done
echo "  created $(ls "$DIR"/*.log | wc -l | tr -d ' ') CRI files across $R restarts of 2 deployments"

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
parser = "json"
timestamp = { field = "ts", format = "unix_ms", resolution = "ms" }

[source.fields]
idx = "integer"
message = "string"

[source.tags_static]
node = "\${NODE_NAME}"

[source.kubernetes]
EOF

STATE="$WORK/state"; mkdir -p "$STATE"
echo "-- run (NODE_NAME=$NODE) --"
NODE_NAME="$NODE" "$BIN" --config "$WORK/k8s.toml" --state-dir "$STATE" --once >"$WORK/run.log" 2>&1 || true
# --once tails every file to EOF and exits; give the sink a moment to flush.
sleep 1

# The state dir SHOULD carry one checkpoint per file (each restart resumes on
# its own) — that's the churn the tags must NOT mirror.
sc=$(ls "$STATE"/*.checkpoint 2>/dev/null | wc -l | tr -d ' ')
echo "  per-file state on disk: $sc checkpoints (one per container instance)"
check "one checkpoint per file (state is per-instance, deliberately)" "$sc" "$((2*R))"

echo
echo "### CARDINALITY ASSERTION ###"
BEFORE=$FAIL
python3 bench/k8s_cardinality_assert.py "$RECV" kube 2 "$((2*R))" "$NODE" || FAIL=1
check "series tracks bounded identity, not the file count" "$FAIL" "$BEFORE"

kill "$MOCK" 2>/dev/null; wait "$MOCK" 2>/dev/null
echo
if [ "$FAIL" = 0 ]; then echo "=== ALL PASS ==="; else echo "=== FAILURES ==="; fi
rm -rf "$WORK"
exit "$FAIL"
