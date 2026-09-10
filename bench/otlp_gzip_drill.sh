#!/bin/sh
# #75 drill: a real OpenTelemetry Collector at its DEFAULTS delivers logs to
# the OTLP receiver. The otlphttp exporter gzips unless told not to, and for
# three releases that was a 400 per batch that nothing counted.
#
# Two agents, same Collector config, same lines:
#   before  the agent built from `main` before #75 (BEFORE_IMAGE, built here
#           from `git archive main` unless you pass one): the Collector
#           logs a permanent 400 on every export and the receiver's record
#           counter never moves — the bug, and the reason it was invisible;
#   after   the agent built from this checkout: every line lands on the
#           durable queue and `tributary_otlp_requests_rejected_total` is 0.
# Then, against the fixed agent, the refusals the unit tests assert, over
# the wire: an unsupported Content-Encoding is 415 and counted; a body that
# claims gzip and is not is 400 and counted.
#
# Everything runs on a private docker network with a shared volume; nothing
# is published to the host and no docker argument carries a host path (Git
# Bash mangles those). The Collector image is distroless, so its config
# arrives on the shared volume, written by a helper container.
#
#   sh bench/otlp_gzip_drill.sh
#   BEFORE_IMAGE=tributary:smoke sh bench/otlp_gzip_drill.sh   # skip the main build
set -e
export MSYS_NO_PATHCONV=1
HERE=$(cd "$(dirname "$0")/.." && pwd)
COLLECTOR=${COLLECTOR:-otel/opentelemetry-collector-contrib:0.116.1}
NET=trib-75; VOL=trib-75-shared
LINES=${LINES:-5}

pass=0; fail=0
chk() { if [ "$1" = "$2" ]; then echo "  PASS  $3"; pass=$((pass+1));
        else echo "  FAIL  $3 (got '$1' want '$2')"; fail=$((fail+1)); fi; }
chk_ge() { if [ "$1" -ge "$2" ] 2>/dev/null; then echo "  PASS  $3 ($1 >= $2)"; pass=$((pass+1));
           else echo "  FAIL  $3 (got '$1' want >= $2)"; fail=$((fail+1)); fi; }
files() { docker exec -i trib-75-files sh -c "$1"; }
curl_() { docker exec trib-75-curl curl -s "$@"; }
metric() { curl_ "http://$1:9109/metrics" | grep "^$2 " | awk '{print $2}' | head -1; }
cleanup() {
  docker rm -f trib-75-files trib-75-curl trib-75-collector trib-75-before trib-75-after >/dev/null 2>&1 || true
  docker volume rm -f "$VOL" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "== #75: the OTel Collector at its defaults (gzip) against the OTLP receiver =="

echo "-- images --"
cd "$HERE"
if [ -z "$BEFORE_IMAGE" ]; then
  BEFORE_IMAGE=tributary:otlp-before
  echo "  building $BEFORE_IMAGE from main ($(git rev-parse --short main))"
  git archive --format=tar main | docker build -q -t "$BEFORE_IMAGE" - >/dev/null
fi
AFTER_IMAGE=tributary:otlp-after
echo "  building $AFTER_IMAGE from the working tree ($(git rev-parse --short HEAD))"
docker build -q -t "$AFTER_IMAGE" . >/dev/null
echo "  collector: $COLLECTOR"
# Pulled up front and quietly, so a first run's transcript is the drill and
# not thirty lines of layer downloads.
for img in "$COLLECTOR" alpine:3.20 curlimages/curl:8.10.1; do docker pull -q "$img" >/dev/null; done

echo "-- rig --"
cleanup
docker network create "$NET" >/dev/null
docker volume create "$VOL" >/dev/null
docker run -d --name trib-75-files --network "$NET" -v "$VOL:/shared" alpine:3.20 sleep infinity >/dev/null
docker run -d --name trib-75-curl --network "$NET" --entrypoint sleep curlimages/curl:8.10.1 infinity >/dev/null
files 'mkdir -p /shared/logs && chmod 777 /shared /shared/logs && cat > /shared/tributary.toml' <<'EOF'
[output]
url = "http://sink.invalid:1"   # nothing listens; the receiver acks after the DURABLE queue, shipping retries
database = "poc"

[telemetry]
addr = "0.0.0.0:9109"

[otlp]
listen = "0.0.0.0:4318"
name = "otel"
table = "logs"
tags = ["service.name", "severity_text"]

[otlp.fields]
body = "string"
EOF
# `compression` is deliberately NOT set on the exporter: the default is gzip,
# and the default is what the fleet copies.
files 'cat > /shared/collector.yaml' <<'EOF'
receivers:
  filelog:
    include: [/shared/logs/*.log]
    start_at: beginning
exporters:
  otlphttp:
    endpoint: http://AGENT:4318
    tls:
      insecure: true
    retry_on_failure:
      enabled: false
service:
  telemetry:
    logs:
      level: info
  pipelines:
    logs:
      receivers: [filelog]
      exporters: [otlphttp]
EOF
files "chmod 644 /shared/tributary.toml /shared/collector.yaml"

run_agent() { # $1=name $2=image
  docker run -d --name "$1" --network "$NET" -v "$VOL:/shared" "$2" \
    --config /shared/tributary.toml --state-dir /var/lib/tributary/state >/dev/null
  for i in $(seq 1 30); do curl_ -f "http://$1:9109/healthz" >/dev/null 2>&1 && break; sleep 1; done
}
run_collector() { # $1=agent name
  docker rm -f trib-75-collector >/dev/null 2>&1 || true
  files "sed 's/AGENT/$1/' /shared/collector.yaml > /shared/collector.$1.yaml"
  docker run -d --name trib-75-collector --network "$NET" -v "$VOL:/shared" "$COLLECTOR" \
    --config "/shared/collector.$1.yaml" >/dev/null
}
write_lines() { # $1=file tag
  i=1; while [ "$i" -le "$LINES" ]; do
    files "echo 'drill $1 line $i' >> /shared/logs/$1.log"; i=$((i+1))
  done
}

echo "-- before: $BEFORE_IMAGE --"
run_agent trib-75-before "$BEFORE_IMAGE"
chk "$(curl_ -o /dev/null -w '%{http_code}' http://trib-75-before:9109/healthz)" "200" "old agent healthy"
run_collector trib-75-before
write_lines before
sleep 12
docker logs trib-75-collector 2>&1 | grep -i "error" | head -2 | cut -c1-600 | sed 's/^/  collector: /'
# The bug, asserted as what it is: the Collector at its defaults is refused,
# and the agent that refused it counted nothing.
chk_ge "$(docker logs trib-75-collector 2>&1 | grep -c '400')" 1 "the Collector logs a 400 from the old agent (the bug)"
chk "$(metric trib-75-before tributary_otlp_records_received_total)" "0" "and the old agent counted NO records while refusing every batch (the invisibility)"

echo "-- after: $AFTER_IMAGE --"
run_agent trib-75-after "$AFTER_IMAGE"
chk "$(curl_ -o /dev/null -w '%{http_code}' http://trib-75-after:9109/healthz)" "200" "new agent healthy"
run_collector trib-75-after
write_lines after
N=0
for i in $(seq 1 30); do
  N=$(metric trib-75-after tributary_otlp_records_received_total)
  [ "${N:-0}" -ge "$((LINES * 2))" ] 2>/dev/null && break; sleep 1
done
# start_at: beginning re-reads BOTH files (before.log and after.log), so the
# new agent should see every line ever written.
chk_ge "${N:-0}" "$((LINES * 2))" "the Collector, gzip by default, delivered every line to the fixed agent"
chk "$(metric trib-75-after tributary_otlp_requests_rejected_total)" "0" "no request was refused"
chk "$(docker logs trib-75-collector 2>&1 | grep -c '400')" "0" "and the Collector saw no 400"

echo "-- refusals over the wire, against the fixed agent --"
chk "$(curl_ -o /dev/null -w '%{http_code}' -X POST -H 'content-encoding: zstd' --data-binary 'x' http://trib-75-after:4318/v1/logs)" \
    "415" "Content-Encoding: zstd -> 415"
chk "$(curl_ -o /dev/null -w '%{http_code}' -X POST -H 'content-encoding: gzip' --data-binary 'not gzip' http://trib-75-after:4318/v1/logs)" \
    "400" "Content-Encoding: gzip on a body that is not gzip -> 400"
chk "$(curl_ -o /dev/null -w '%{http_code}' -X POST --data-binary 'not protobuf' http://trib-75-after:4318/v1/logs)" \
    "400" "a body that is not a protobuf request -> 400"
chk "$(metric trib-75-after tributary_otlp_requests_rejected_total)" "3" "all three counted on tributary_otlp_requests_rejected_total"
chk "$(metric trib-75-after tributary_otlp_records_rejected_total)" "0" "and none on the per-record counter, which is the point"

echo
echo "== $pass passed, $fail failed =="
[ "$fail" -eq 0 ]
