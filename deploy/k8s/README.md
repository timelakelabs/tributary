# Tributary on Kubernetes

`daemonset.yaml` runs Tributary as a DaemonSet — one pod per node, tailing every
container log on that node and shipping it to TimeLakeDB, each line tagged with
the pod, namespace, container and node it came from.

## Apply

Every Release carries this manifest stamped with the release's image tag; that
is the copy to apply (#77):

```sh
# Point the ConfigMap's output.url at your TimeLakeDB service first.
curl -sSLO https://github.com/timelakelabs/tributary/releases/download/v0.5.0/daemonset.yaml
kubectl apply -f daemonset.yaml
```

The file in this tree pins `ghcr.io/timelakelabs/tributary:<the crate version
of this tree>`, which exists as an image only once that version is tagged — so
applying it from `main` between releases pulls nothing, on purpose. It used to
pin `:latest`, which was whatever `main` last built, and a node that rebooted
pulled a different agent than its neighbours; a fleet running three versions of
the agent reads as a product bug. A cargo test holds the manifest's tag equal to
the crate version, so the release commit that bumps `Cargo.toml` has to bump it
too. To run the tree's own build on a cluster, `kind-smoke.sh` shows the one
override that takes: the `image:` line.

`validate.py` structurally checks the manifest without a cluster (an equivalent
to `kubectl apply --dry-run=client`, which needs a cluster's schema), including
the pinned tag, the telemetry listener and the probe:

```sh
python3 deploy/k8s/validate.py deploy/k8s/daemonset.yaml
```

## Liveness, and why there is no readiness

The ConfigMap serves `[telemetry]` on `0.0.0.0:9109`, and the container's
`livenessProbe` asks `GET /healthz` there. `/healthz` is 503 only when the main
loop has stopped ticking for 60 s — a wedged agent, the one condition a restart
fixes. It stays 200 through a TimeLakeDB outage, deliberately: a probe that
restarts the agent then discards the in-memory and in-flight batches, which is
the one moment the durable queue exists to protect (`server.rs` says the same at
length). There is no `readinessProbe`, also deliberately. A DaemonSet serves no
traffic, so "not ready" gains nothing, and the body's `shipping` field is false
for the whole of every outage. Watch the outage on `/metrics` instead:
`tributary_queue_bytes` climbing while `tributary_lines_shipped_total` is flat.

## How it works

- **One glob source** (`/var/log/containers/*.log`) tails the whole directory,
  fanning out an independent per-file pipeline and discovering/retiring them as
  pods come and go (#64). No sidecars, no one-source-per-container.
- **Enrichment** comes from the log path (#63): `/var/log/containers/<pod>_<ns>_<container>-<id>.log`
  yields the `pod`, `namespace` and `container` tags. No apiserver call.
- **Node name** is stamped from the Downward API: the DaemonSet injects
  `NODE_NAME` (`spec.nodeName`), and the config references it as
  `node = "${NODE_NAME}"`.
- **State** (checkpoints + queue) lives on a `hostPath` (`/var/lib/tributary`),
  so a restarted agent resumes exactly where it stopped instead of re-shipping
  the world or losing its queue.
- **Labels** (`app`, `version`, …) are opt-in via the `labels` allowlist (#66):
  a label becomes a tag only if named, resolved once per pod from the API server
  and cached. A label the operator did not name — the unbounded
  `pod-template-hash` — never becomes a tag. Drop the allowlist and no API call
  is made.
- **Permissions** are a ServiceAccount plus **read-only pods** (`get`/`list`/
  `watch`) — the only RBAC here, and only because of label enrichment. Nothing
  writes, no other resource, no secrets. `/var/log` is mounted **read-only**;
  the agent runs as root only to read root-owned pod logs, with no privilege
  escalation, all capabilities dropped, and a read-only root filesystem.

## Cardinality — the thing that makes this safe

A container that restarts leaves a new log file with a new 64-hex container-id
in its name. That id is unbounded, and it is right there in the path. Tributary
stamps the container **name** (bounded), never the **id**: the id keys the
per-file checkpoint on disk (so each restart resumes on its own) but is stripped
out of every tag. A deployment that rolls a hundred times is a hundred files and
**one** series, not a hundred dead ones.

`bench/k8s_cardinality_drill.sh` proves it: 100 files across 50 restarts of two
deployments collapse to 2 series, with zero 64-hex tag values
(`docs/evidence/k8s-cardinality-drill.log`).

## The CRI text format

containerd and CRI-O write the CRI **text** format —
`2024-01-01T00:00:00.000000000Z stdout F the message`. The manifest uses
`parser = "cri"` (#71): it extracts the log's own timestamp (so records are
stamped at event time, not ingestion time), promotes `stdout`/`stderr` to the
`stream` tag, and reassembles the `P`/`F` splits the kubelet makes at ~16 KB
back into one record. `bench/k8s_cri_drill.sh` proves it end to end
(`docs/evidence/k8s-cri-drill.log`). Nodes on the Docker json-file driver set
`parser = "docker_json"` instead — the config is otherwise identical, since both
parsers emit the same `{log, stream, time}` envelope.

## Label enrichment offline

Labels normally come from the API server, but a `label_file` under
`[source.kubernetes]` resolves them from a static `namespace/pod -> {label:
value}` JSON map instead — for air-gapped clusters, and the only way the offline
drill can supply labels. `bench/k8s_labels_drill.sh` uses it to prove the
allowlist: 100 files carrying `app`/`team`/`pod-template-hash` collapse to 2
series with `app` and `team` stamped and `pod-template-hash` never a tag
(`docs/evidence/k8s-labels-drill.log`).

## Live smoke test

`kind-smoke.sh` runs this exact manifest on a throwaway `kind` cluster and
proves the one thing the offline drills can't: labels resolved **live** from the
API server through the ServiceAccount + read-only `pods` ClusterRole. It applies
the real manifest (only the image, pull policy and output URL are overridden),
runs a labelled workload, and asserts `app` lands as a tag while the
API-injected `pod-template-hash` does not — end to end, including the CRI parser
on genuine containerd logs. Since #77 it also asserts the liveness wiring: the
DaemonSet's probe asks `/healthz` and nothing asks readiness, `/healthz` on the
pod IP answers 200 with `live: true`, and the kubelet has restarted nothing.
Needs `docker`, `kind` and `kubectl`, and a host that can nest containers.

```sh
deploy/k8s/kind-smoke.sh            # creates + tears down the cluster
deploy/k8s/kind-smoke.sh --keep     # leaves it up to inspect
```

Evidence: `docs/evidence/k8s-kind-smoke.log`.
