# Changelog

All notable changes to Tributary are recorded here. This project adheres to
[Semantic Versioning](https://semver.org).

## [Unreleased]

### Added

- **Allowlisted pod-label enrichment** (#8, phase 4, #66). `[source.kubernetes]`
  gains a `labels` allowlist: a pod label becomes a tag only if named there,
  resolved once per pod (never per line — that would rate-limit the agent off
  the node) and cached, with the child's held labels dropped when its log file
  disappears. Nothing is stamped by default, because a label like
  `pod-template-hash` is unbounded and would blow up cardinality — the FR-2
  failure, reintroduced at the shipper where TimeLakeDB can't defend against it.
  Labels come from the in-cluster API server (a read-only `pods` Role, the only
  RBAC the DaemonSet needs) or a static `label_file` for air-gapped clusters and
  the offline drill. `bench/k8s_labels_drill.sh` proves it: 100 files carrying
  `app`/`team`/`pod-template-hash` collapse to 2 series with `app` and `team`
  stamped and `pod-template-hash` never a tag
  (`docs/evidence/k8s-labels-drill.log`). This completes the DaemonSet epic (#8).
- **Kubernetes DaemonSet manifest** (#8, phase 3, #65). `deploy/k8s/daemonset.yaml`
  runs one Tributary per node tailing `/var/log/containers/*.log`, with the node
  name stamped from the Downward API, host logs mounted read-only, checkpoints on
  a hostPath so a restart resumes exactly, and a ServiceAccount with no RBAC
  (tailing host files needs no apiserver access — pod labels in phase 4 will).
  `deploy/k8s/validate.py` checks it structurally without a cluster.
- **`${VAR}` expansion in static tag values** (#65). A static tag value may
  reference the environment — `node = "${NODE_NAME}"` — so the DaemonSet can
  stamp the node name from the Downward API. Scoped to tag values (not the whole
  file, which would eat a redact rule's `$1` capture refs); an unset variable is
  a startup error, not a silently blank tag.
- **Glob tailing for a directory of container logs** (#8, phase 2, #64). A
  source whose `path` has a wildcard in its last segment
  (`/var/log/containers/*.log`) becomes a supervisor that tails the whole
  directory: one independent per-file pipeline each — its own stamper,
  watermark, checkpoint and queue — sharing only the shipper and telemetry.
  A file appearing (a pod starting) is picked up on the next rescan without a
  restart; a file vanishing (a pod dying) stops that tail and retires its
  checkpoint, and its queue once it has drained (a queue still holding
  undelivered lines is kept, not dropped). Each file carries ITS pod/namespace/
  container from phase 1, so one source distinguishes every pod on the node.
  Per-file crash-resume is exact — each file resumes from its own offset. This
  is what makes a DaemonSet possible; the manifest and cardinality drill are
  phase 3. Drilled end to end over discovered files
  (`bench/k8s_glob_drill.sh`, `docs/evidence/k8s-glob-drill.log`): discovery +
  enrichment, a pod appearing mid-run, a SIGKILL that resumes each file from
  its own checkpoint, and a pod dying that retires only its own state.
- **Kubernetes CRI path enrichment** (#8, phase 1). A source that sets
  `[source.kubernetes]` and tails a CRI container log parses `pod`,
  `namespace` and `container` out of the filename
  (`/var/log/containers/<pod>_<namespace>_<container>-<id>.log`) and stamps
  them as tags — no apiserver call and no sidecar. The three are bounded by
  the node's pod count, so they stay inside the FR-2 allowlist rule; pod
  labels, which are not bounded, wait for a later opt-in allowlist. A source
  whose path is not a CRI log (a plain `/var/log/app.log`) is left exactly as
  before — the parse fails closed rather than inventing tags. Phase 1 of the
  DaemonSet epic; glob tailing, the manifest and the label allowlist follow.
- **Multiple sources per agent** (#49). One process now tails many `[[source]]`
  blocks concurrently — each with its own tail, framer, watermark, checkpoint
  and durable queue, namespaced on disk by source name (`queue-<name>/`,
  `<name>.checkpoint`, `dead-letter-<name>.lp`) — over one shared connection
  pool and one telemetry endpoint whose counters carry per-source labels. A
  pre-existing single-source `queue/` is migrated to `queue-<name>/` in place.
  Source names must be unique (they key the on-disk state); `journald` and
  `winlog` stay one-per-agent, being single-cursor pull loops, not file tails.
  One source failing stops the whole agent, loudly. Landed over #50 (extract the
  per-source pipeline), #56 (per-source telemetry), #52 (concurrent tasks) and
  #53 (live source-set reload), and drilled end to end in #54 —
  `bench/multi_source_drill.sh`, evidence in
  `docs/evidence/multi-source-drill.log`: two streams each exact, a SIGKILL that
  resumes each source from its own checkpoint, and a live SIGHUP add/remove.
- **Config reload without a restart** (`SIGHUP`, T-5, #10). Re-reads the
  `--config` file and hot-applies the transform stage (`filter`/`sample`/
  `redact`) and the output knobs (`batch_lines`, `max_inflight`,
  `watermark_every_secs`, `rpo_report_secs`) on every running tail — checkpoint,
  queue and in-flight batches untouched. The reload also diffs the **source
  set**: an added `[[source]]` starts tailing and a removed one stops and
  drains, without disturbing the sources that stayed (#53). Validate-before-swap:
  a file that will not load or validate is refused and the last-good config
  keeps running, with `tributary_config_reloads_total`,
  `tributary_config_reloads_refused_total` and
  `tributary_config_last_reload_ok` making the outcome visible. Changes to an
  existing source's identity/schema or to bound resources (`output.url`, TLS,
  the listeners) are reported as restart-required rather than silently ignored.
  Unix-only.

### Fixed

- **A glob child's `stream` tag no longer carries the container id** (#65,
  fixing #64 before release). The per-file stream identity includes the 64-hex
  container id so each container instance keeps its own checkpoint — but that id
  was also going into the `stream` TAG, which would have made every pod restart
  a brand-new series and blown up cardinality exactly the way pod labels would.
  The state key keeps the id; the tag is now the bounded label with it stripped
  (`pod_namespace_container`). Caught by the phase-3 cardinality drill.

## [0.3.0] — 2026-08-26

Tributary becomes a full **Telegraf + log-agent replacement**: four new
ingestion sources beyond file tailing, a complete Telegraf-schema host-metrics
collector, and a Vector-shaped transform stage (filter, sample, redact).

### Added

**Ingestion sources**

- **OpenTelemetry (OTLP/HTTP) receiver** — receive pushed log records, not only
  tail files; a record is acknowledged only once it is durably queued, so a
  Collector never believes it delivered something a restart then dropped (#21).
- **Docker `json-file` source** — read Docker's `json-file` driver directly and
  reassemble the 16 KB frame splits Docker breaks a long line across, stdout and
  stderr separately (#22).
- **journald source** (feature-gated) — read the systemd journal and resume
  crash-exact from its cursor; a default build links no libsystemd, so CI on a
  plain runner is unaffected (#24).
- **Windows Event Log source** (feature-gated) — read Event Log channels via
  `wevtapi` and resume from the opaque bookmark, not a record offset; a default
  build links no `wevtapi` (#26).

**Host metrics (Telegraf-compatible)**

- A `[metrics]` collector emitting Telegraf's `cpu` / `mem` / `disk` / `net` /
  `system` / `swap` with the **exact** measurement, field, and tag names, so a
  dashboard built against InfluxDB + Telegraf keeps working after the swap (#27).
- CPU per-state breakdown (`usage_user` / `usage_iowait` / …) from `/proc/stat`
  on Linux (#33); disk inodes via `statvfs` (#34); buffered/cached memory from
  `/proc/meminfo` (#36); a `diskio` collector from `/proc/diskstats` (#37).
- An **exec collector** — run a command on an interval and ingest its
  line-protocol output, killing the whole process group on timeout and capping
  runaway stdout (#38).
- The disk collector now isolates **per mount** and quarantines an unresponsive
  one, so a dead NFS mount no longer wedges it or leaks a thread per tick — the
  leak is bounded to one blocked thread per dead mount (#39, #41).
- "Additional fields": `[metrics.global_tags]` and `[metrics.static_fields]`
  are stamped on every emitted metric (#27).

**Transform stage** — `[[source.filter]]` / `[[source.sample]]` /
`[[source.redact]]`, run on the mapped record after `map_line`

- **filter** — drop records by a tag/field equality (deny by default, or an
  allow-list), *before* the queue and *before* the watermark counts them, so a
  dropped record is never claimed as arrived (#45).
- **sample** — keep 1-in-`rate`, decided by a fixed-seed hash of the record's
  identity so a crash-resumed tail re-decides identically and never
  double-counts; composes with a filter predicate to sample a subset (#46).
- **redact** — regex-replace a value inside a string field *before* the record
  is ever written durably, so a secret in a log line never leaves the host; the
  regex is compiled and validated at load (#47).
- Drops are counted apart from loss as
  `tributary_records_dropped_total{stage="filter"|"sample"}`, and the
  completeness invariant is now `read − shipped − quarantined − dropped = queue
  depth`.

**Packaging & CI**

- Publish the agent container image to GitHub Container Registry (#20).
- `.deb` / `.rpm` install instructions in the README (#19).

## [0.2.0] — 2026-08-25

Initial packaged release: file tailing with rotation and crash-resume, the
durable queue with poison isolation and observed watermarks, data-plane token
authentication, mTLS client certificates that rotate under load, and a measured
queue RPO. See the release for detail.

[0.3.0]: https://github.com/timelakelabs/tributary/releases/tag/v0.3.0
[0.2.0]: https://github.com/timelakelabs/tributary/releases/tag/v0.2.0
