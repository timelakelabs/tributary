#!/usr/bin/env python3
"""Structural validation of the Tributary DaemonSet manifest (#8 phase 3).

`kubectl apply --dry-run=client` needs a cluster's OpenAPI schema and a kubectl
binary; the ticket allows an equivalent. This loads every document in
daemonset.yaml and asserts the invariants that actually matter for a working
DaemonSet — the shape kubectl would check, plus the Tributary-specific wiring
(the glob source, CRI enrichment, the Downward API node stamp, read-only log
mounts, minimal RBAC) that kubectl would not.

Exit 0 = all checks pass. Usage: validate.py [path-to-daemonset.yaml]
"""
import sys

import yaml

PATH = sys.argv[1] if len(sys.argv) > 1 else "deploy/k8s/daemonset.yaml"

fail = 0


def check(label, cond):
    global fail
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    if not cond:
        fail = 1


docs = [d for d in yaml.safe_load_all(open(PATH, encoding="utf-8")) if d]
by_kind = {}
for d in docs:
    check(f"{d.get('kind','?')} has apiVersion/kind/metadata.name",
          bool(d.get("apiVersion") and d.get("kind") and d.get("metadata", {}).get("name")))
    by_kind.setdefault(d.get("kind"), []).append(d)

check("has the six resources (Namespace, SA, ClusterRole(+Binding), ConfigMap, DaemonSet)",
      set(by_kind) == {"Namespace", "ServiceAccount", "ClusterRole",
                       "ClusterRoleBinding", "ConfigMap", "DaemonSet"})
# The RBAC is read-only pods and nothing more — that is the "scoped to what it
# needs and no more" claim, now that phase 4 reads pod labels from the API.
cr = by_kind.get("ClusterRole", [{}])[0]
rules = cr.get("rules", [])
check("RBAC is read-only pods only (get/list/watch, no writes, no other resources)",
      len(rules) == 1
      and rules[0].get("resources") == ["pods"]
      and set(rules[0].get("verbs", [])) == {"get", "list", "watch"})
crb = by_kind.get("ClusterRoleBinding", [{}])[0]
subj = (crb.get("subjects") or [{}])[0]
check("the binding points the ClusterRole at the tributary ServiceAccount",
      crb.get("roleRef", {}).get("name") == "tributary-pod-reader"
      and subj.get("kind") == "ServiceAccount" and subj.get("name") == "tributary")

ds = by_kind.get("DaemonSet", [{}])[0]
spec = ds.get("spec", {}).get("template", {}).get("spec", {})
check("DaemonSet uses the tributary ServiceAccount",
      spec.get("serviceAccountName") == "tributary")

containers = spec.get("containers", [])
c = containers[0] if containers else {}
args = c.get("args", [])
check("container passes --config and --state-dir",
      "--config" in args and "--state-dir" in args)

env = {e.get("name"): e for e in c.get("env", [])}
node = env.get("NODE_NAME", {}).get("valueFrom", {}).get("fieldRef", {}).get("fieldPath")
check("NODE_NAME comes from the Downward API (spec.nodeName)", node == "spec.nodeName")

mounts = {m.get("name"): m for m in c.get("volumeMounts", [])}
check("/var/log is mounted READ-ONLY (never write to tailed logs)",
      mounts.get("varlog", {}).get("mountPath") == "/var/log" and mounts["varlog"].get("readOnly") is True)
check("a writable state mount exists (checkpoints + queue)",
      "state" in mounts and not mounts["state"].get("readOnly"))

volumes = {v.get("name"): v for v in spec.get("volumes", [])}
check("/var/log is a hostPath (the node's real logs)",
      volumes.get("varlog", {}).get("hostPath", {}).get("path") == "/var/log")
check("state is a hostPath (survives a pod restart, so resume is exact)",
      "hostPath" in volumes.get("state", {}))

sc = c.get("securityContext", {})
check("no privilege escalation and all capabilities dropped",
      sc.get("allowPrivilegeEscalation") is False and sc.get("capabilities", {}).get("drop") == ["ALL"])

# The embedded Tributary config is the load-bearing part kubectl can't see.
cm = by_kind.get("ConfigMap", [{}])[0]
toml = cm.get("data", {}).get("tributary.toml", "")
check("config tails the CRI glob /var/log/containers/*.log",
      "/var/log/containers/*.log" in toml)
check("config enables kubernetes CRI enrichment ([source.kubernetes])",
      "[source.kubernetes]" in toml)
check("config stamps the node from the environment (node = \"${NODE_NAME}\")",
      'node = "${NODE_NAME}"' in toml)
check("config declares a pod-label allowlist (#66)",
      "labels = [" in toml)

print("ALL PASS" if fail == 0 else "FAILURES")
sys.exit(fail)
