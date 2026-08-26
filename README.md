# Tributary

[![ci](https://github.com/timelakelabs/tributary/actions/workflows/ci.yml/badge.svg)](https://github.com/timelakelabs/tributary/actions/workflows/ci.yml)

A log-file agent for [TimeLakeDB](https://github.com/timelakelabs/timelakedb).

A tributary feeds a lake. This one tails log files — and receives
OpenTelemetry (OTLP) logs pushed to it — and writes them into
TimeLakeDB over line protocol — the same wire Telegraf already uses for
metrics, so one host ships both through one endpoint and one data model.

**Status: phases L0–L4 shipped, plus data-plane authentication and self-telemetry.** Tailing,
rotation and crash-resume, the durable queue with poison isolation and
observed watermarks, throughput, presenting a bearer token to TimeLakeDB
without ever logging it, and — since L4 — presenting a **client
certificate** that rotates under load without dropping a line. The queue's
**RPO is measured rather than asserted** (P1-7): zero for a restart on a
surviving disk, and a bounded, configurable window when the node itself is
lost. Every phase is gated by a recorded run rather than by unit tests
alone — see `bench/results/`:

| Phase | What it proved | Evidence |
|---|---|---|
| L0 | Exact count on a static file; the millisecond disambiguator is real | `bench/results/l0-exact-count.log` |
| L1 | Rotation and crash-resume, both exact | `bench/results/l1-rotation-resume.log` |
| L2 | Outage absorption, poison isolation, watermarks, multiline joins | `bench/results/l2-queue-bisect-watermark.log` |
| L3 | 156k → 492k lines/s (the checkpoint was the bottleneck) | `bench/results/l3-throughput.log` |
| P0-5 | Presents the data-plane token; never logs it; spools rather than drops on 401 | `bench/results/p05-data-auth.log` |
| L4 | mTLS: presents a client certificate; both certificates rotate under load; a rejected renewal keeps the last-good pair; anonymous callers still served | `bench/results/l4-mtls-rotation.log` |
| P1-7 | The queue's RPO, measured: 0 on a surviving disk, `batch_lines × (1 + max_inflight)` on node loss | `bench/results/p17-queue-rpo.log` |
| T-1 | `/metrics` and `/healthz`: 26 series, and a database outage leaves liveness green so nothing restarts the agent out from under its queue | `bench/results/t1-self-telemetry.log` |

Next is L5 (discovery and cloud metadata) and L6 (the Flight `DoPut`
wire, gated on TimeLakeDB growing it) — see
[`ROADMAP.md`](ROADMAP.md). [`DESIGN.md`](DESIGN.md) remains the
specification, and its §1 explains why this is a purpose-built agent
rather than a Vector configuration.

## Install (Linux packages)

Each release attaches a `.deb` and an `.rpm` built from that tag.

```bash
VER=0.2.0

# Debian / Ubuntu
curl -LO https://github.com/timelakelabs/tributary/releases/latest/download/tributary_${VER}_amd64.deb
sudo apt install ./tributary_${VER}_amd64.deb

# RHEL / Rocky / Alma / Amazon Linux 2023
curl -LO https://github.com/timelakelabs/tributary/releases/latest/download/tributary-${VER}-1.x86_64.rpm
sudo dnf install ./tributary-${VER}-1.x86_64.rpm
```

The package installs the agent, a hardened systemd unit, and
`/etc/tributary/{config.toml,tributary.env}`. **It does not start anything** —
the shipped `config.toml` has no `[[source]]`, so the agent refuses to start
until you point it at your server and your log files:

```bash
# 1. the pipeline: set output.url and add one [[source]] per file
sudoedit /etc/tributary/config.toml
# 2. if TimeLakeDB runs with data-plane auth, set TRIBUTARY_TOKEN in:
sudoedit /etc/tributary/tributary.env
# 3. let the unprivileged agent read your logs, then start it
sudo usermod -aG adm tributary          # Debian/Ubuntu; grant read another way elsewhere
sudo systemctl enable --now tributary
```

Check it, if you kept `[telemetry]` on: `curl http://127.0.0.1:9109/healthz`.

### Receiving OTLP logs (push)

Beyond tailing files, Tributary can be an OpenTelemetry logs **receiver**:
point an OTLP/HTTP exporter (an SDK, or the OTel Collector) at it and each
log record lands on the same durable queue → ship path a file tail uses,
**acknowledged only once it is durably queued** — so a Collector never
believes it delivered something a restart then dropped.

```toml
[otlp]
listen = "0.0.0.0:4318"   # OTLP/HTTP; the receiver is POST /v1/logs
name   = "otel"           # becomes the `stream` tag
table  = "logs"
# tags is an ALLOWLIST over resource/scope/log attributes (plus body,
# severity_text, scope.name). An attribute becomes a tag ONLY if named here,
# so unbounded ones (k8s.pod.uid, a trace id per record) never explode the
# series. A dictionary-vs-plain distinction does not survive OTLP, so a
# string is a tag by default.
tags   = ["service.name", "k8s.namespace.name", "severity_text"]

[otlp.fields]
body = "string"           # the log message; declared types only, as for a source
```

A config may carry an `[otlp]` receiver, one or more `[[source]]` file
tails, or both. The receiver maps resource/scope/log attributes through the
allowlist, the log body to a field, and the record's `time_unix_nano` to the
timestamp — then hands the record to the same map → queue → ship path, so
auth, TLS, watermarks and disk buffering are inherited, not reimplemented.

Requires glibc 2.31+ (Debian 11+, Ubuntu 20.04+, RHEL/Rocky 9+, AL2023);
verified on Debian 12, Ubuntu 22.04, Rocky 9 and Amazon Linux 2023. See
[`packaging/`](packaging/README.md) to build them yourself.

### Docker json-file logs

A source can read Docker's `json-file` driver directly — the
`/var/lib/docker/containers/<id>/<id>-json.log` files — with
`parser = "docker_json"`:

```toml
[[source]]
name   = "web"
path   = "/var/lib/docker/containers/<id>/<id>-json.log"
table  = "container_logs"
parser = "docker_json"
timestamp   = { field = "time", format = "rfc3339" }
fields      = { log = "string" }        # the message
tags        = ["stream"]                # keep stdout vs stderr
tags_static = { container = "web" }
```

It decodes the `{"log","stream","time"}` envelope and — the part a naive
reader gets wrong — **reassembles the 16 KB frame splits**: Docker breaks a
log line longer than 16 KB across several JSON objects, and only the last
ends in a newline, so joining them back is the difference between one record
and a shredded one. stdout and stderr are reassembled separately.

**One caveat, stated plainly:** Tributary already emits a `stream` tag equal
to the source `name` (its stream identity). Allowlist docker's own `stream`
(stdout/stderr) and it takes that tag over — last write wins. Give the
container its identity through `tags_static`, as above, and let `stream`
carry stdout/stderr.
## journald source (feature-gated)

On a systemd host, half the services log only to the journal — not a file you
can tail. Build with the `journald` feature and declare a `parser = "journald"`
source:

```
cargo build --release --features journald   # links libsystemd

[[source]]
name   = "journald"
table  = "syslog"
parser = "journald"
fields = { MESSAGE = "string" }         # the log line -> a field
tags   = ["_SYSTEMD_UNIT", "PRIORITY"]  # only NAMED fields become tags (FR-2)
```

Each entry becomes a record on the same map -> queue -> ship path a file tail
uses, timestamped by the journal's own `__REALTIME_TIMESTAMP`. The resume token
is the journal **cursor** — an opaque string persisted through the checkpoint,
not a byte offset. A default build (no feature) links no libsystemd and refuses
a journald config at startup, so CI on a plain runner is unaffected.

Crash-exact resume is drilled end to end — 200 entries, a hard `kill -9`
mid-run, restart, every entry delivered exactly once, none skipped or
duplicated — by `bench/journald_resume_drill.sh` (evidence
`docs/evidence/journald-resume-drill.log`).

## Windows Event Log source (feature-gated)

The Windows equivalent of the journal: on Windows, services log to Event Log
channels (`System`, `Application`, …), not files. Build with the `winlog`
feature — it links `wevtapi` through the `windows` crate — and declare a
`parser = "winlog"` source whose `path` names the channel:

```
cargo build --release --features winlog     # links wevtapi (Windows only)

[[source]]
name   = "winsys"
path   = "System"                 # the channel to read (path == channel here)
table  = "eventlog"
parser = "winlog"
timestamp = { resolution = "us" } # events carry 100 ns time; us fills seq safely
fields = { EventID = "string", Computer = "string" }
tags   = ["Provider", "Channel"] # only NAMED fields become tags (FR-2)
```

Each event is rendered to XML, the kept fields are pulled out and mapped on the
same map -> queue -> ship path a file tail uses, timestamped by the event's own
`TimeCreated`. The resume token is the Windows **bookmark** — an opaque XML
token persisted through the checkpoint, NOT an `EventRecordID` offset (records
are purged and the channel wraps, so an offset can point at nothing; the
bookmark survives that). A default build (no feature) links no `wevtapi` and
refuses a winlog config at startup, and the reader is also `#[cfg(windows)]`, so
`--features winlog` on Linux still links nothing — the feature only bites on a
Windows target.

**The Security channel** is not read by default and is called out on purpose:
it needs elevation and carries audit weight, so name it explicitly
(`path = "Security"`) and run the agent with the rights to read it.

This machine has no MSVC toolchain, so the binary is **cross-compiled** from
the Linux build container and **run on the Windows host**, where the real Event
Log lives:

```
rustup target add x86_64-pc-windows-gnu
apt-get install -y gcc-mingw-w64-x86-64
cargo build --features winlog --target x86_64-pc-windows-gnu
# copy target/x86_64-pc-windows-gnu/debug/tributary.exe to the host and run it
```

Crash-exact resume is drilled on the host against the real System channel —
read the oldest 2N events one-shot, then read N + resume-from-bookmark N, and
assert the split read equals the one-shot read (every event once, no gap, no
dupe) — by `bench/winlog_resume_drill.py` (evidence
`docs/evidence/winlog-resume-drill.log`). The drill drives the binary's
`--winlog-dump` diagnostic, which runs the real reader and persists the
bookmark through the ordinary `Checkpoint` path:

```
python bench\winlog_resume_drill.py --exe path\to\tributary.exe
```

## Host metrics (Telegraf-compatible)

For a migration off InfluxDB + Telegraf: sample the machine on an interval and
write the same measurements Telegraf's `system` input family does, with
Telegraf's exact names, so the host dashboards keep working after the swap. Add
a `[metrics]` section — no `[[source]]` required, it runs on its own:

```
[metrics]
interval   = "10s"
collectors = ["cpu", "mem", "disk", "diskio", "net", "system", "swap"]   # this is the default set

# The "add your own fields" half: stamped on EVERY metric row.
[metrics.global_tags]        # the Telegraf [global_tags] equivalent
region = "us-east"
role   = "db"
[metrics.static_fields]      # constant fields; TOML type -> field type
deployment = "prod"          # string
weight     = 3               # integer (3i)
```

The measurements and their Telegraf names:

| measurement | tags | fields |
|---|---|---|
| `cpu` | `cpu` (`cpu-total`, `cpu0`, …) | `usage_idle`, `usage_active`; on Linux also `usage_user`/`usage_system`/`usage_iowait`/`usage_nice`/`usage_irq`/`usage_softirq`/`usage_steal`/`usage_guest`/`usage_guest_nice` |
| `mem` | — | `total`, `available`, `used`, `free`, `used_percent`, `available_percent`; on Linux also `buffered`/`cached` |
| `disk` | `device`, `path`, `fstype` | `total`, `free`, `used`, `used_percent`; on unix also `inodes_total`/`inodes_free`/`inodes_used` |
| `diskio` (Linux) | `name` | `reads`, `writes`, `read_bytes`, `write_bytes`, `read_time`, `write_time`, `io_time`, `weighted_io_time`, `iops_in_progress` |
| `net` | `interface` | `bytes_recv`, `bytes_sent`, `packets_recv`, `packets_sent`, `err_in`, `err_out` |
| `system` | — | `load1`, `load5`, `load15`, `n_cpus`, `uptime` |
| `swap` | — | `total`, `used`, `free`, `used_percent` |

One mostly-`sysinfo` code path covers Linux and Windows (the Linux CPU per-state
split is read straight from `/proc/stat`). The `host` tag defaults to the
OS hostname (override with `[metrics].host`), and `global_tags` cannot override
`host` or a structural tag. Every row in a tick shares one timestamp; distinct
series (a different `cpu`/`device`/`interface`) are distinct primary keys, so
the log stamper's per-record disambiguation is deliberately not used here.

**Known gaps** (documented, not bugs): the `cpu` per-state split
(`usage_user`/`usage_system`/`usage_iowait`/…) is read from `/proc/stat` and is
Linux-only — on other platforms `sysinfo` reports one aggregate percentage per
core, so `cpu` there carries `usage_idle`/`usage_active` only; disk inodes are
unix-only (`statvfs`; Windows has no inode concept); `mem` buffered/cached are
Linux-only (`/proc/meminfo`); `diskio` is Linux-only (`/proc/diskstats`);
`load*` is zero on Windows,
which has no load-average concept. `net`/`diskio` counters are emitted cumulative — take the
`derivative()` in the dashboard, do not diff them here.

On Linux the `disk` collector enumerates mounts from `/proc/self/mountinfo` (no
`statvfs`) and probes each mount in its own bounded task, so an unresponsive
mount (a dead NFS) quarantines itself — the healthy mounts keep reporting, and
its timed-out probe handle is HELD rather than re-spawned, so a wedged mount
costs exactly one blocked thread and is picked back up the instant its `statvfs`
returns. The filesystem-type filter matches Telegraf's `disk` default
ignores (`tmpfs`, `proc`, `overlay`, …); real and network filesystems are kept.

The collector is drilled against a live node: the real binary ships all six
measurements and they read back carrying Telegraf's names plus the configured
`global_tags`/`static_fields` — `bench/metrics_collector_drill.sh` (evidence
`docs/evidence/metrics-collector-drill.log`).

### Custom metrics from a command

For anything the built-in collectors don't cover — an app's own counters, a
device a script scrapes — run a command on an interval and ingest its
line-protocol stdout, the way Telegraf's `inputs.exec` does:

```toml
[[metrics.exec]]
command  = ["/usr/local/bin/queue_depth", "--lp"]  # argv, NOT a shell line
interval = "30s"                                    # optional; default = [metrics].interval
timeout  = "5s"                                     # optional; default 5s
```

`command` is an argv list, never a shell string, so there is no shell to
inject into — and it runs with the **agent's** privileges, so don't assume
root. The command prints line protocol (`myapp,tag=v field=1`); each line is
stamped with the metrics `global_tags` (and `host`) and shipped through the
same durable queue as everything else.

Two guardrails, because a metrics agent must outlive a bad command: a run that
exceeds `timeout` is killed — on unix the whole **process group**, so a
`sh -c "…"` wrapper's children die with it, not just the wrapper — and stdout
is capped at 1 MiB (a flooding command is truncated, never buffered
unbounded). A non-zero exit ships nothing and logs; it never takes the
collector down.

**Caveats:** `static_fields` are not injected into exec output in v1 (the
`global_tags` are); and the process-group kill is unix — on Windows the child
is killed best-effort, so a grandchild can outlive the timeout.

## The short version of why it exists

Three properties of TimeLakeDB's write contract turn a naive log shipper
into a data-loss machine:

1. **The primary key is the tag set plus the timestamp, deduplicated
   last-write-wins.** A service logging 10,000 lines/second with
   millisecond timestamps loses ~90% of them, silently, and the writes
   return `204`. Tributary fills the unused sub-millisecond precision
   with a deterministic per-stream sequence, which makes lines unique
   *and* makes retries idempotent.
2. **A batch is atomic** — one unparseable line rejects all 5,000, and a
   single non-UTF-8 byte rejects the request before the parser sees it.
   Tributary decodes lossily, validates in-process against the server's
   own parser crate, and bisects to quarantine anything that still fails.
3. **The first write of a field fixes its type, permanently.** Tributary
   declares types in configuration instead of letting ingestion order
   decide.

It also attaches Accumulo-style row visibility labels (`_visibility`) at
ingest, which no other log agent can do because no other target enforces
them inside the scan.

## The test that matters

> Lines written to the file == rows in the database. Exactly.

That single equality catches primary-key collision, poison-batch loss,
checkpoint bugs and rotation gaps at once. See `DESIGN.md` §6.

## Licence

Apache-2.0.
