#!/usr/bin/env bash
# Live smoke test of the pod-label API path on a real cluster (#72).
#
# The offline drills (bench/k8s_labels_drill.sh) resolve labels from a static
# label_file because they can't reach an API server. This one CAN: it stands up
# a kind cluster, runs the REAL deploy/k8s/daemonset.yaml against it (only the
# image, pull policy and output URL are overridden for the test cluster — the
# RBAC, ServiceAccount, security context and Downward API are exactly what ships),
# and proves the live path end to end:
#
#   * the DaemonSet's ServiceAccount + read-only pods ClusterRole actually let it
#     read a workload pod's labels from the API server,
#   * an ALLOWLISTED label (`app`) lands as a tag,
#   * a NON-allowlisted label (`pod-template-hash`, which kubernetes adds itself)
#     does NOT,
#   * pod/namespace/container enrichment from the CRI path works, and
#   * the CRI text parser handles kind's real containerd logs.
#
# kind uses containerd, so this is also the only place `parser = "cri"` meets
# genuine runtime output rather than a synthesised corpus.
#
# CI note: this is a runnable script, not a gating CI job, because Actions is
# billing-blocked here (a job would show red with steps=0). It belongs on the
# self-hosted runners once those are live; until then, run it by hand on any box
# with docker + kind + kubectl. It needs a runner that can nest containers
# (kind runs the node as a privileged container).
#
# Usage: deploy/k8s/kind-smoke.sh [--keep]     (--keep leaves the cluster up)
set -euo pipefail

CLUSTER=trib-smoke
IMAGE=tributary:smoke
KEEP=0
[ "${1:-}" = "--keep" ] && KEEP=1

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"
FAIL=0
say() { echo "== $*"; }
check() { if [ "$2" = "$3" ]; then echo "  [PASS] $1 ($2)"; else echo "  [FAIL] $1 (got $2, want $3)"; FAIL=1; fi; }

for tool in kind kubectl docker; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 2; }
done

cleanup() { [ "$KEEP" = 1 ] || kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; }
trap cleanup EXIT

say "cluster"
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER" --wait 120s
KC="kubectl --context kind-$CLUSTER"

say "build + load the agent image"
# --provenance=false: an attestation manifest makes `kind load` (ctr import)
# choke on Docker Desktop's containerd image store. The agent image is
# local-only, so it MUST be loaded; the public fixtures (python/busybox) are
# left for the node to pull itself — kind-loading multi-arch Hub images hits the
# same attestation snag, and the node has internet anyway.
docker build --provenance=false -t "$IMAGE" . >/dev/null
kind load docker-image "$IMAGE" --name "$CLUSTER"

say "mock write sink (records bodies to a file, so its own stdout is NOT tailed"
say "     back into itself — a stdout recorder would feed the DaemonSet a loop)"
$KC apply -f - >/dev/null <<'YAML'
apiVersion: v1
kind: Namespace
metadata: { name: tributary }
---
apiVersion: v1
kind: ConfigMap
metadata: { name: mock-src, namespace: tributary }
data:
  mock.py: |
    import http.server, sys
    class H(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            n = int(self.headers.get('content-length', 0))
            body = self.rfile.read(n)
            with open('/tmp/recv.lp', 'ab') as f:
                f.write(body if body.endswith(b'\n') else body + b'\n')
            self.send_response(204); self.end_headers()
        def log_message(self, *a): pass
    http.server.HTTPServer(('0.0.0.0', 8899), H).serve_forever()
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: mock, namespace: tributary }
spec:
  replicas: 1
  selector: { matchLabels: { app: mock } }
  template:
    metadata: { labels: { app: mock } }
    spec:
      containers:
        - name: mock
          image: python:3.12-slim
          command: ["python3", "/src/mock.py"]
          ports: [ { containerPort: 8899 } ]
          volumeMounts: [ { name: src, mountPath: /src } ]
      volumes: [ { name: src, configMap: { name: mock-src } } ]
---
apiVersion: v1
kind: Service
metadata: { name: mock, namespace: tributary }
spec:
  selector: { app: mock }
  ports: [ { port: 8899, targetPort: 8899 } ]
YAML
$KC -n tributary rollout status deploy/mock --timeout=90s

say "the REAL DaemonSet manifest, overridden only for this cluster"
# Only three things change from what ships: the image (kind-loaded, not ghcr),
# the pull policy (use the loaded image), the output URL (the mock) and gzip off
# so the mock can read plain line protocol. The RBAC, SA, securityContext,
# Downward API and the cri/kubernetes source are applied verbatim.
sed -e "s#ghcr.io/timelakelabs/tributary:latest#$IMAGE#" \
    -e "s#http://timelakedb.timelakedb.svc:1963#http://mock.tributary.svc.cluster.local:8899#" \
    -e "s#gzip = true#gzip = false#" \
    -e "s#image: $IMAGE#image: $IMAGE\n          imagePullPolicy: IfNotPresent#" \
    deploy/k8s/daemonset.yaml | $KC apply -f - >/dev/null
$KC -n tributary rollout status daemonset/tributary --timeout=120s

say "a labelled workload: app=labeltest (allowlisted), and the pod-template-hash"
say "     kubernetes adds itself (NOT allowlisted, the cardinality bomb)"
$KC apply -f - >/dev/null <<'YAML'
apiVersion: v1
kind: Namespace
metadata: { name: smoke-app }
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: labeltest, namespace: smoke-app }
spec:
  replicas: 1
  selector: { matchLabels: { app: labeltest } }
  template:
    metadata: { labels: { app: labeltest } }
    spec:
      containers:
        - name: chatter
          image: busybox:1.36
          command: ["sh", "-c", "i=0; while true; do echo SMOKELINE-$i; i=$((i+1)); sleep 1; done"]
YAML
$KC -n smoke-app rollout status deploy/labeltest --timeout=90s

say "wait for the workload's LABELLED lines to reach the mock (label resolution"
say "     happens once at child startup, a beat after the line first ships)"
MOCK=$($KC -n tributary get pod -l app=mock -o jsonpath='{.items[0].metadata.name}')
# Wait for a line carrying app=labeltest specifically — not just any SMOKELINE —
# so the assert can't race the one-shot label resolution. app=labeltest is
# unique to the workload, so its presence means the live API lookup landed.
i=0
until $KC -n tributary exec "$MOCK" -- sh -c 'grep -c "app=labeltest" /tmp/recv.lp 2>/dev/null || echo 0' | grep -qE '^[1-9]'; do
  i=$((i+1)); [ $i -gt 60 ] && { echo "  timed out waiting for a labelled line"; break; }
  sleep 2
done
$KC -n tributary exec "$MOCK" -- cat /tmp/recv.lp > /tmp/smoke-recv.lp 2>/dev/null || true
LINES=$($KC -n tributary exec "$MOCK" -- sh -c 'grep SMOKELINE /tmp/recv.lp' 2>/dev/null || true)

echo
say "ASSERTIONS (on the delivered line protocol for the workload)"
n=$(printf '%s\n' "$LINES" | grep -c SMOKELINE || true)
check "the workload's lines were delivered at all" "$([ "$n" -gt 0 ] && echo yes || echo no)" "yes"
# app=labeltest — the allowlisted label, resolved LIVE from the API server.
check "allowlisted label app=labeltest is a tag" \
  "$(printf '%s\n' "$LINES" | grep -c 'app=labeltest' | grep -qE '^[1-9]' && echo yes || echo no)" "yes"
# pod-template-hash — present on the pod, NOT allowlisted, must never be a tag.
check "non-allowlisted pod-template-hash is NOT a tag" \
  "$(printf '%s\n' "$LINES" | grep -qE 'pod-template-hash=|pod_template_hash=' && echo present || echo absent)" "absent"
# path enrichment through the cri parser.
check "namespace=smoke-app enriched from the CRI path" \
  "$(printf '%s\n' "$LINES" | grep -qE '(^|,)namespace=smoke-app(,| )' && echo yes || echo no)" "yes"
check "container=chatter enriched from the CRI path" \
  "$(printf '%s\n' "$LINES" | grep -qE '(^|,)container=chatter(,| )' && echo yes || echo no)" "yes"
check "stream=stdout from the cri parser" \
  "$(printf '%s\n' "$LINES" | grep -qE '(^|,)stream=stdout(,| )' && echo yes || echo no)" "yes"

echo
echo "  sample delivered line:"
printf '%s\n' "$LINES" | head -1 | sed 's/^/    /'
echo
if [ "$FAIL" = 0 ]; then echo "== ALL PASS"; else echo "== FAILURES"; fi
exit "$FAIL"
