#!/bin/bash
# K8s pod-label allowlist drill (#8 phase 4 / #66) — the phase-3 cardinality
# drill re-run with a labels allowlist. It proves the last acceptance item:
# allowlisted labels become tags, and a label the operator did NOT name — the
# unbounded `pod-template-hash` — never becomes a tag, so the series count stays
# bounded to the allowlisted set under the same 100-file id churn.
#
# The labels come from a static label_file (the only way an offline drill can
# supply them; in a cluster it is the API server). Each pod's metadata carries
# app + team (allowlisted) AND pod-template-hash (NOT allowlisted). The assert
# demands app/team land as tags, pod-template-hash never does, and 100 files
# still collapse to 2 series.
#
#   docker run --rm -v "$PWD:/w" -w /w \
#     -v rk-cargo-registry:/usr/local/cargo/registry \
#     rust:1-slim bash -c 'apt-get update -qq && apt-get install -y -qq python3 \
#       >/dev/null; bench/k8s_labels_drill.sh'
set -u
export NO_COLOR=1
FAIL=0
check() { if [ "$2" = "$3" ]; then echo "  [PASS] $1 ($2)"; else echo "  [FAIL] $1 (got $2, want $3)"; FAIL=1; fi; }

BENCH=$(cd "$(dirname "$0")" && pwd)
cd "$BENCH/.."
WORK=$(mktemp -d)
DIR="$WORK/containers"; mkdir -p "$DIR"
R=50
K=200
NODE="node-1"

echo "=== k8s pod-label allowlist drill ($(date -u +%Y-%m-%dT%H:%M:%SZ)) ==="
echo "workdir=$WORK  deployments=2  restarts_each=$R  files=$((2*R))  allowlist=[app,team]  node=$NODE"

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

python3 bench/gen.py --out "$WORK/web.rev" --lines "$K" --rate 10000 >/dev/null
python3 bench/gen.py --out "$WORK/api.rev" --lines "$K" --rate 10000 >/dev/null

# One fixed pod per deployment, R container-ids each (the phase-3 model).
for i in $(seq 1 "$R"); do
  id=$(printf '%064x' "$i")
  cp "$WORK/web.rev" "$DIR/web-59f8c_shop_server-$id.log"
  cp "$WORK/api.rev" "$DIR/api-7b3d2_billing_worker-$id.log"
done
echo "  created $(ls "$DIR"/*.log | wc -l | tr -d ' ') CRI files"

# Each pod carries app + team (bounded, allowlisted) and pod-template-hash
# (NOT allowlisted). The allowlist must keep the hash out of the tags.
cat > "$WORK/labels.json" <<'JSON'
{
  "shop/web-59f8c":    {"app": "web", "team": "pay", "pod-template-hash": "59f8c6b7d4"},
  "billing/api-7b3d2": {"app": "api", "team": "ops", "pod-template-hash": "7b3d29ac10"}
}
JSON

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
labels = ["app", "team"]
label_file = "$WORK/labels.json"
EOF

STATE="$WORK/state"; mkdir -p "$STATE"
echo "-- run (NODE_NAME=$NODE, labels allowlist=[app,team]) --"
NODE_NAME="$NODE" "$BIN" --config "$WORK/k8s.toml" --state-dir "$STATE" --once >"$WORK/run.log" 2>&1 || true
sleep 1

echo
echo "### LABEL ALLOWLIST ASSERTION ###"
BEFORE=$FAIL
python3 bench/k8s_labels_assert.py "$RECV" kube 2 "app,team" "pod-template-hash" || FAIL=1
check "labels allowlist: app/team stamped, pod-template-hash excluded, series bounded" "$FAIL" "$BEFORE"

kill "$MOCK" 2>/dev/null; wait "$MOCK" 2>/dev/null
echo
if [ "$FAIL" = 0 ]; then echo "=== ALL PASS ==="; else echo "=== FAILURES ==="; fi
rm -rf "$WORK"
exit "$FAIL"
