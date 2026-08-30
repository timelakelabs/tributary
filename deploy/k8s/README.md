# Tributary on Kubernetes

`daemonset.yaml` runs Tributary as a DaemonSet — one pod per node, tailing every
container log on that node and shipping it to TimeLakeDB, each line tagged with
the pod, namespace, container and node it came from.

## Apply

```sh
# Point the ConfigMap's output.url at your TimeLakeDB service first.
kubectl apply -f deploy/k8s/daemonset.yaml
```

`validate.py` structurally checks the manifest without a cluster (an equivalent
to `kubectl apply --dry-run=client`, which needs a cluster's schema):

```sh
python3 deploy/k8s/validate.py deploy/k8s/daemonset.yaml
```

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
- **Permissions** are a ServiceAccount and nothing else — tailing host files
  needs no apiserver access. `/var/log` is mounted **read-only**; the agent runs
  as root only to read root-owned pod logs, with no privilege escalation, all
  capabilities dropped, and a read-only root filesystem.

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

## Known gap: the CRI text format

containerd and CRI-O write the CRI **text** format —
`2024-01-01T00:00:00.000000000Z stdout F the message`. This manifest uses
`parser = "plain"`, which ships each line whole as `message`: correct and
lossless, but the record timestamp is ingestion time and the `stdout`/`stderr`
stream and the partial-line (`P`/`F`) markers are not split out. A structured
CRI parser that does that is a worthwhile follow-up. Nodes still on the Docker
json-file format can set `parser = "dockerjson"` today, which Tributary already
reassembles.

## Coming in phase 4 (#66)

Allowlisted pod **labels** as tags. That reads pod metadata from the apiserver,
so it will add a read-only `pods` Role — the first and only RBAC this needs.
