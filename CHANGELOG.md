# Changelog

All notable changes to Tributary are recorded here. This project adheres to
[Semantic Versioning](https://semver.org).

## [Unreleased]

### Added

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
  #53 (live source-set reload); the end-to-end exactness drill is #54.
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
