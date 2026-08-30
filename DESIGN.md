# Tributary — Design

**Status:** v1 · updated 2026-08-18 (L0–L4, P0-5, P1-7, T-1 shipped) · a log-file agent for
[TimeLakeDB](https://github.com/timelakelabs/timelakedb).

A tributary feeds a lake. This one tails log files and writes them into
TimeLakeDB over line protocol, the same wire Telegraf already uses for
metrics (FR-1) — so a host that ships metrics with Telegraf ships logs
with Tributary, through one endpoint and one data model.

**Stack:** Rust · `tokio` · `notify` · the `timelake-ingest` parser crate
(shared with the server, see §4.3).

---

## 1. Why this is not a generic log shipper

Vector and Fluent Bit tail files well, and both can already emit line
protocol. If the only requirement were "get lines from a file to an HTTP
endpoint", this project should not exist and the answer would be a Vector
configuration.

It exists because **three properties of TimeLakeDB's write contract turn
a naive log shipper into a data-loss machine**, and none of them can be
handled by a generic sink. Everything in this design descends from them.

### 1.1 Primary-key collision destroys data silently

TimeLakeDB's primary key is *the tag set plus the timestamp*, and a
duplicate is resolved last-write-wins (FR-5). That is exactly right for
retry safety and exactly wrong for logs:

> A service emitting 10,000 lines/second with millisecond timestamps
> produces ~10 lines per millisecond. Lines sharing a millisecond *and* a
> tag set share a primary key, and all but one are **destroyed** — not
> rejected, not logged, not counted. The write returns 204.

No error surfaces anywhere. **Measured** (`bench/results/l0-exact-count.log`):
the same 200,000-line corpus shipped with the disambiguator produced
200,000 distinct rows; shipped without it, **60,825 lines — 30.4% —
vanished while the agent reported 200,000 shipped and zero errors.**

The rate is a birthday collision against the key: with ten lines per
millisecond spread over twelve tag sets, `12·(1−(11/12)¹⁰) = 6.96`
survive, predicting 30.4% loss against 30.4% measured. So loss scales as
lines-per-tick over tag cardinality — a single-purpose log file writing
under one tag set approaches ~90%, and richer tags merely dilute it.
Either way the data is gone and nothing says so.

§3 is the solution; §6.1 is the test that proves it.

### 1.2 One malformed line rejects the whole batch

> "The batch is atomic: if any line fails to parse the whole request is
> rejected with 400 and nothing in it is written."

A 5,000-line batch containing one unparseable line writes **zero** lines.
A shipper that simply retries wedges forever on the poison line, and the
tail stops advancing. §4.3 and §4.4 handle it.

The same class of failure at the transport level: a body that is not
valid UTF-8 is refused whole, before the parser, and line protocol has no
byte escape. Log files contain binary garbage — a truncated write, a core
dump fragment, a mis-set locale. One bad byte kills 5,000 good lines.

### 1.3 Field types are permanent, and the first line decides them

> "The first write of a field name fixes its type."

If `duration=1.5` is written before `duration="unknown"`, that column is a
float forever and every later line carrying the string 400s — for the
lifetime of the table. Ingestion order is not a sound way to pick a
schema, so Tributary declares types in configuration instead (§5).

---

## 2. Data model: what becomes a tag

The single most consequential decision a log agent makes against a
time-series database, and the one most often made badly.

| Kind | Contents | Why |
|---|---|---|
| **Tag** | An explicit allowlist: `host`, `service`, `env`, `level`, `stream` | Dictionary-encoded `Dictionary<Int32, Utf8>` columns. High cardinality costs what a compressed column costs (FR-2) — but tags are also **in the primary key**, so each one changes dedup semantics. |
| **Field** | `message`, and every extracted value | Typed columns, *not* in the primary key, no dictionary. Where the payload belongs. |
| **Refused** | The message body, request/trace IDs, user IDs as tags | A dictionary whose values are ~100% unique is pure overhead, and widening the PK with them changes what "duplicate" means. |

**Tributary will not promote arbitrary parsed keys to tags.** The tag set
comes from an allowlist in the configuration and nowhere else. This is
the difference between a log agent that works at 2M entities/day and one
that quietly becomes the thing FR-2 exists to prevent.

`_visibility` is an ordinary tag holding an Accumulo-style label
expression (SEC-2), so Tributary can attach row-level visibility labels
at ingest — per source, or extracted from the log itself. No other log
agent can do this, because no other target enforces labels inside the
scan.

Not every tag comes from the line. A `[source.kubernetes]` source (#8)
parses `pod`, `namespace` and `container` out of the CRI log path
(`/var/log/containers/<pod>_<namespace>_<container>-<id>.log`) and stamps
them as tags — no apiserver call, no sidecar. These three stay inside the
allowlist rule because they are bounded by the node's pod count, not by
message content; pod *labels*, which are unbounded, are a separate
opt-in allowlist rather than blanket enrichment.

---

## 3. Delivery semantics: at-least-once, made exactly-once

Tributary ships at-least-once and relies on TimeLakeDB's last-write-wins
dedup to collapse replays. That only works if **a replayed line produces
a byte-identical primary key**, which requires the timestamp to be a
deterministic function of the source stream, not of wall-clock or
arrival order.

### 3.1 The disambiguator

When the source timestamp is coarser than nanoseconds, the unused
precision is filled with a per-stream sequence number:

```
ts_ns = source_ts_ns + seq        where 0 <= seq < (ns per source tick)
```

`seq` is the index of the line among those sharing the same source tick
within the same stream. At millisecond resolution that allows 1,000,000
lines per millisecond per stream before exhaustion — six orders of
magnitude of headroom over the pathological case in §1.1.

Properties this buys:

- **Uniqueness** — no two lines in a stream share a PK, so nothing is
  silently dropped.
- **Order preservation** — `seq` increases with file offset, so
  `ORDER BY time` returns lines in the order they were written.
- **Determinism** — see §3.2, which is what makes retries free.

**Honest cost, and it belongs in the user-facing docs:** sub-tick
ordering is *assigned by Tributary*, not observed from the source. Two
lines one millisecond apart are truly ordered; two lines inside the same
millisecond are ordered by their position in the file, which is the best
information that exists.

### 3.2 The checkpoint makes replay identical

A crash between shipping a batch and recording progress must regenerate
the *same* timestamps, or the replay writes duplicates instead of
collapsing into the originals. So the checkpoint carries enough state to
resume the sequence exactly:

```json
{ "stream": "app",
  "file": {"dev": 2049, "ino": 918273, "size": 104857600},
  "offset": 73400320,
  "last_tick_ns": 1786280343206000000,
  "seq": 37 }
```

Restoring `last_tick_ns` and `seq` — not just the offset — is what closes
the window where a checkpoint lands mid-millisecond. Without it, the
lines after resume restart at `seq = 0` and overwrite the ones before it.
Checkpoints are written with the same temp-file + fsync + rename
discipline the database uses for its own objects: durable or absent,
never torn.

### 3.3 When the source already has nanosecond precision

There is no unused precision to fill, so PK uniqueness depends on the
source. Configuring `resolution = "ns"` logs a warning at startup naming
the risk, and offers `disambiguate = "tag"` as an escape hatch, which
adds a `seq` **tag** instead. That widens the primary key and creates a
near-unique dictionary column — a real cost, chosen deliberately, and
only for sources that need it.

### 3.4 What the queue does and does not promise (RPO)

The disk queue is **node-local durability, not replication**. Two failure
models follow from that, and conflating them is how people end up
surprised:

| the node… | what is lost | why |
|---|---|---|
| **comes back** (process crash, restart, redeploy on the same disk) | **nothing** | the checkpoint and the queue are still on disk; the agent resumes at the exact offset, losing and duplicating nothing (§3.2, L1) |
| **is gone** (spot eviction, an evicted pod with an `emptyDir`) | everything not yet acked | the batch being assembled, the batches in flight, the queue, **and the log files themselves** |

The second row's bound is the RPO, and it is set by configuration:

```
RPO ≤ batch_lines × (1 + max_inflight)      + queue contents
      └─ the unacked window ─┘                └─ only while the server
                                                 is refusing writes ─┘
```

At the shipped defaults (`batch_lines = 5000`, `max_inflight = 4`) that is
25,000 lines — 25 s at 1,000 lines/s, 2.5 s at 10,000. Halving either knob
halves the ceiling, at a throughput cost L3 measures separately.

Two things this is careful about. The **unread source bytes** count: on a
node that vanishes the log file dies with it, so data the agent had not yet
read is as lost as data it had queued — which is the term people forget when
they reason about the queue alone. And the **checkpoint interval governs
duplicates, not loss**: a longer interval means a restart re-reads more, not
that a dead node loses more.

Because none of this is visible from the config alone, the agent reports its
own exposure every `rpo_report_secs` (default 60):

```
INFO tributary: at risk if this node is lost now pending_lines=800
     inflight_batches=0 queue_segments=0 queue_bytes=0 unread_bytes=0
```

Measured, not asserted: `bench/drill-p17.sh`, evidence in
`bench/results/p17-queue-rpo.log`.

---

## 4. Pipeline

```
discover → tail → decode → parse → map → validate → batch → ship → checkpoint
                                                        ↕
                                                   disk queue
```

### 4.1 Discover and tail

Files are tracked by `(device, inode)`, not by path, so a rotation does
not look like a truncation. The tailer handles the three rotation styles —
rename-and-recreate, copy-and-truncate, and create-new-with-suffix — plus
the case that matters most in practice: **finishing the tail of a
rotated-away file before following the new one**, so the last few
kilobytes before a rotation are not lost.

A file whose inode disappears while unread bytes remain is a named,
counted event (`tributary_files_lost_total`), never a silent gap.

A source whose `path` carries a wildcard in its last segment
(`/var/log/containers/*.log`, #8/#64) is a **glob source**: one supervisor
tailing a whole directory of files, discovered and retired as they come
and go. Each matched file is a full independent tail — its own stamper,
watermark, checkpoint and queue — because a stamper shared across files
would break the per-stream replay dedup of §3.1. They share only the
source's shipper and telemetry. A file appearing (a pod starting) is
adopted on the next rescan without a restart; a file vanishing (a pod
dying) stops that tail and retires its checkpoint — and its queue too,
but *only once it has drained*, because leftover segments are lines that
never reached the server, and dropping them is the loss §4.5 exists to
prevent. The per-file identity is derived from the filename, which for a
CRI symlink includes the container id, so a restarted container correctly
becomes a new stream rather than resuming a dead one's offset.

### 4.2 Decode and parse

Bytes are decoded as UTF-8 **lossily** (invalid sequences become U+FFFD)
before anything else touches them. This is not a nicety: §1.2 means one
invalid byte would otherwise reject the entire batch at the transport
layer, and no amount of downstream care recovers from that.

Lossy decode is the right default for an *undeclared* source, and the
wrong fate for a file genuinely written in another character set — a
Shift-JIS or Windows-1252 log would arrive with every non-ASCII character
replaced. The requirement to do better is **FR-10** in TimeLakeDB's
`REQUIREMENTS.md` (added 2026-08-12): a source may declare its encoding,
declared sources transcode losslessly or quarantine the offending line,
and only undeclared sources take this lossy path — with replacements
counted, never silent.

Parsers: `json`, `logfmt`, `regex` (named captures), `plain` (the whole
line becomes `message`). Multiline joins are configured by a start
pattern, with a size cap and a flush timeout so an unterminated stack
trace cannot pin memory.

### 4.3 Map and validate locally

Mapping applies the §2 rules and the declared types from §5. Values that
will not coerce to their declared type are **quarantined**, not shipped.

Then the batch is validated *in process*, using **the same
`timelake-ingest` parser crate the server uses**. Because Tributary and
TimeLakeDB are built by the same project, a line that would 400 can be
caught before it costs a round trip. This is the single strongest
argument for a native agent over a Vector configuration, and it requires
publishing `timelake-ingest` as a standalone crate — worth doing anyway,
since it already carries no heavy dependencies.

**Transform stage (T-2, #7).** Between the mapped record and the queue is
where a record can be dropped, sampled, or redacted — declared, not a DSL.
Filter (#42) drops records by a `[[source.filter]]` equality; sample (#43)
keeps 1-in-`rate` by a *deterministic hash of the record's identity*, so a
crash-resume re-decides the same way and last-write-wins collapses the
replay rather than double-counting; redact (#44) regex-scrubs a value in a
string field *before the record is encoded*, so a secret never reaches the
queue, the checkpoint, or anything durable. The load-bearing detail is *ordering against the
watermark*: the drop runs **before `wm.observe()`**, so a dropped record's
timestamp is never counted as arrived. Dropping after the watermark had
counted it would make the completeness claim — Tributary's whole
differentiator — quietly lie about data that was deliberately thrown away.
Drops are counted as a decision (`tributary_records_dropped_total{stage}`),
apart from the loss/at-risk accounting, so `read − shipped − quarantined −
dropped` still balances.

### 4.4 Ship, with bisect as the safety net

`POST /api/v3/write_lp`, gzipped. Responses:

| Status | Meaning | Action |
|---|---|---|
| 204 | WAL-durable | advance the checkpoint |
| 400 | a line was rejected | **bisect** the batch to isolate it, quarantine, ship the remainder |
| 429 | backpressure (RR-5) | honour `Retry-After`; queue |
| 5xx / timeout | unknown outcome | retry with jitter; replay is safe by §3 |

Local validation should make 400 unreachable; bisect exists because
"should" is not a guarantee across version skew. Isolating one bad line
in a 5,000-line batch costs ~13 requests, which is affordable precisely
because it is rare.

### 4.5 Queue and backpressure

Tailing a file is not like receiving a network stream: there is nobody to
push back on, and the file keeps growing. So the queue exists to buy
time, and its exhaustion policy is a decision, not an accident.

Default: spool to disk up to `max_bytes`, then **stop reading and let the
checkpoint lag**, with a named alarm and a gauge. Never drop silently —
the same posture RR-5 demands of the database ("guardrails are visible,
tunable, and never silent"). The real risk in this state is a file
rotating away before Tributary catches up, which is why
`tributary_checkpoint_lag_bytes` is the metric to alert on.

### 4.6 Host metrics (Telegraf compatibility)

A `[metrics]` collector (#25) makes Tributary a Telegraf replacement for the
host-metrics half of an InfluxDB migration, not only the log half. It samples
the machine every `interval` through `sysinfo` — one cross-platform reader —
and emits Telegraf's `system` input family: `cpu`, `mem`, `disk`, `net`,
`system`, `swap`. The point is **schema fidelity**: the measurement, field and
tag names are Telegraf's exact strings (`used_percent`, `usage_idle`, tag
`host`, tag value `cpu-total`), because a Grafana panel keys on those literals
and one rename blanks it with no error. The mapping is a set of pure
`sample -> Record` functions whose output is pinned by golden-string tests, so
a rename fails review instead of a dashboard.

The collector is its own pipeline — its own queue and shipper — feeding the
same durable `Queue -> Shipper` path (§4.4–4.5) a source uses. Two properties
differ from a log source and are deliberate:

- **One timestamp per tick.** All rows a tick produces share the tick's
  timestamp. The §3.1 disambiguator is a log defence (many records on one
  stream at one instant); applying it here would push `cpu0` a nanosecond off
  `cpu-total` and break the time-alignment a dashboard depends on. Distinct
  series (a different `cpu`/`device`/`interface` tag) are already distinct
  primary keys, so there is nothing to disambiguate.
- **Counters stay cumulative.** `net`/`disk` byte and packet counters are
  emitted as read; the dashboard takes the derivative. (`sysinfo` counts from
  first observation rather than boot, but the derivative is identical.)

`global_tags`/`static_fields` are the "add your own fields" half of the
request — constant tags and fields stamped on every row (the Telegraf
`[global_tags]` equivalent), mirroring a source's `tags_static`. Neither can
override the `host` tag, a structural tag, or a real metric field, so a stray
entry cannot corrupt a series or emit a duplicate field key.

On Linux the `disk` collector enumerates mounts from `/proc/self/mountinfo`
(a plain read, no `statvfs`) and probes each mount's `statvfs` in its own
bounded task. A mount that times out has its probe handle HELD, not re-spawned:
it is skipped until that held `statvfs` finally returns (i.e. the mount
recovered), so an unresponsive mount (a dead NFS) neither wedges the collector
nor leaks more than one blocked thread — while the healthy mounts keep
reporting. The fs-type filter matches Telegraf's default ignores.

Known gaps are honest, not hidden: the `cpu` per-state split
(`usage_user`/`usage_system`/`usage_iowait`/…) is read from `/proc/stat` and is
Linux-only — elsewhere `sysinfo` gives one aggregate percentage per core, so
`cpu` carries `usage_idle`/`usage_active` only; disk inodes are unix-only
(`statvfs`; Windows has no inode concept); `mem` buffered/cached are Linux-only
(`/proc/meminfo`); `diskio` is Linux-only (`/proc/diskstats`); `load*` is zero
on Windows, which has no load-average concept.

An `[[metrics.exec]]` runs a command (argv, never a shell string) on an
interval and ships its line-protocol stdout through the same queue, with the
`global_tags` stamped in — the escape hatch for anything the built-ins don't
cover. Each run is bounded twice: a `timeout` that, on unix, SIGKILLs the
command's whole process group (a `sh -c` wrapper's children die with it, not
just the wrapper), and a 1 MiB stdout cap that drains-and-truncates a flooding
command instead of buffering it. A non-zero exit ships nothing. Windows gets a
best-effort child kill (no process groups), and `static_fields` injection into
arbitrary line protocol is a v1 gap; `global_tags` are applied.

### 4.7 Many sources in one agent

A deployment rarely has exactly one log to ship. Rather than run a process per
file — a supervisor entry, a state directory and a scrape target each — one
agent tails every `[[source]]` it is given (#49). Each is a full copy of §4.1–
4.4: its own tailer, framer, stamper, watermark and checkpoint, and its own
durable queue. What they share is deliberately only the expensive, safe-to-
share things — one `reqwest` connection pool (the `Shipper` is `Clone`, an
`Arc` over the pool and the counters) and one `[telemetry]` listener. Per-source
state is namespaced on disk by name (`queue-<name>/`, `<name>.checkpoint`,
`dead-letter-<name>.lp`), so a crash resumes each source at its own position
with no chance of one source reading another's checkpoint. The single-source
layout that predates this — a bare `queue/` — is renamed in place the first
time a one-source agent starts under the new code.

Telemetry had to go per-source *before* concurrency did (#56 before #52): the
Prometheus exposition is one endpoint, so N tasks writing one flat counter set
would clobber each other's numbers. Each source owns a `SourceSnap`; the
exposition sums them and also labels them, so the completeness invariant holds
per source and in aggregate.

The tasks run under one `JoinSet` with a coordinator that owns three things: an
OS stop signal, a `SIGHUP` reload, and `join_next()`. A per-source `watch`
channel lets the coordinator stop *one* source — the case a reload that dropped
a `[[source]]` needs — while a global stop signals them all. `SIGHUP` re-reads
the config and diffs the running set by name (§4.7 is why the name is the key):
a name gone from the file is signalled to stop and drain, a new name is spawned,
and a name in both is left running and picks up its own transform changes off a
generation counter — a shared flag would let only one of N tasks observe the
signal. One source returning `Err` fails the whole agent on purpose: a
half-collecting agent behind a green process is the outage you find out about
from the gap in the data, which is exactly the failure mode this project exists
to refuse. `journald` and `winlog` are excluded from the set at load time —
they are one-cursor subsystem pulls, not tails, and belong to their own agent.

---

## 5. Configuration

The configuration is the product: every field below exists because of a
property in §1.

```toml
[output]
url      = "http://timelake:1963"
database = "logs"
table    = "app_logs"
batch    = { max_bytes = "4MiB", max_age = "1s", gzip = true }
queue    = { dir = "/var/lib/tributary/queue", max_bytes = "2GiB" }

# The data-plane token (SEC-4), when the node runs TIMELAKE_DATA_AUTH=
# optional|required. Sourced from the TRIBUTARY_TOKEN environment variable
# (which wins) or this file — NEVER inline here, because a secret in a
# committed config is a secret leaked. The token is sent as
# `Authorization: Bearer <token>` and never reaches a log line.
token_file = "/etc/tributary/token"    # optional; omit for a mode=off node

# How often to log the "at risk if this node is lost now" line (§3.4). This
# is the deployment's live RPO: everything the server has not acked lives
# only on this node. 0 turns it off.
rpo_report_secs = 60

# The agent's OWN log, not the files it tails. Absent = stdout only,
# which is right under systemd or Docker where stdout is captured and
# rotated for you. Set it for a bare-process deployment, where stdout
# redirected to a file grows until the disk fills.
#
# Rotation fires on EITHER trigger, whichever comes first. Elapsed since
# the file was opened, not aligned to midnight. `keep` omitted retains
# every rotated file, which is the safe default for anything someone may
# need after an incident. This sink owns the file: do not also point
# logrotate at the same path.
[log]
file         = "/var/log/tributary/agent.log"
rotate_size  = "100MiB"        # KiB is 1024, KB is 1000 — both accepted
rotate_every = "1d"
keep         = 7

# Self-telemetry (T-1). Absent = no listener at all, so an agent that
# never configured this behaves exactly as it did before the endpoint
# existed. 127.0.0.1 is the safe start; a DaemonSet scraped across the pod
# network needs 0.0.0.0, and that is a deliberate choice because the
# endpoint carries no authentication and reports file paths and volumes.
[telemetry]
addr = "127.0.0.1:9109"        # GET /metrics, GET /healthz

# Transport security (L4). Both halves are independent and both optional:
# `ca_file` alone is plain HTTPS against a private issuer (what Telegraf
# does); cert_file + key_file present an identity, and it is both or
# neither. TimeLakeDB verifies a presented certificate in WANT mode, so a
# client without one is served exactly as before — this section is
# additive, and removing it restores pre-L4 behaviour.
[output.tls]
ca_file      = "/etc/tributary/certs/ca.crt"
cert_file    = "/etc/tributary/certs/client.crt"
key_file     = "/etc/tributary/certs/client.key"
# SEC-3 assumes ~24 h certificates, so a renewal lands while the agent is
# shipping. It is picked up without a restart; one that fails validation is
# refused and the last-good pair keeps shipping.
refresh_secs = 30

[[source]]
name      = "app"                       # the stream identity (§3.1)
paths     = ["/var/log/app/*.log"]
parser    = "json"
multiline = { starts_with = '^\d{4}-\d{2}-\d{2}' }

# Declared resolution drives the disambiguator (§3.1).
timestamp = { field = "ts", format = "rfc3339", resolution = "ms" }

# An allowlist, never "promote everything" (§2).
tags        = ["service", "level"]
tags_static = { host = "${HOSTNAME}", env = "prod" }

# Declared types, so ingestion order cannot decide them forever (§1.3).
[source.fields]
message  = "string"
duration = "float"
status   = "integer"

visibility = "(ops&audit)|admin"        # SEC-2 row label (§2)
```

Defaults are chosen so that a config which omits everything optional is
*safe*: no tags beyond `host` and `stream`, `plain` parser, `message` as
the only field, ingest-time timestamps at millisecond resolution.

---

## 6. Acceptance test — the harness is the specification

Borrowed wholesale from TimeLakeDB's discipline, because it applies even
more sharply here: the failure mode in §1.1 is invisible to every
conventional test.

### 6.1 The assertion that matters

> **Lines written to the file == rows in the database. Exactly.**

One equality catches primary-key collision, poison-batch loss, checkpoint
bugs and rotation gaps simultaneously. Every scenario below asserts it.

| Scenario | What it proves |
|---|---|
| 1M lines, 1 stream, ms timestamps at **10k lines/s** | The §3.1 disambiguator. Without it this loses ~90% and reports success. |
| Rotation every 10 MB during a sustained write | The tail of a rotated file is not lost (§4.1) |
| `SIGKILL` mid-stream, then restart | Resume loses nothing **and duplicates nothing** (§3.2) |
| A malformed line and a binary blob mid-file | Quarantined; the batch still ships (§1.2, §4.4) |
| Database stopped 60 s, then restarted | Queue absorbs; nothing lost; no hammering (§4.5) |
| File growing faster than the network for 10 min | RSS stays bounded; lag is visible |
| Same file replayed from an old checkpoint | Row count **unchanged** — replay is idempotent (§3) |
| Two `[[source]]` files under one agent | Each stream exact; a `SIGKILL` resumes each source from its own checkpoint; a live `SIGHUP` adds and removes a source (§4.7). `docs/evidence/multi-source-drill.log` |

Results land in `bench/results/` with the run recorded, in the same
format as the database's own evidence. A claim without a run behind it
does not go in the README.

### 6.2 Observability

`tributary_lines_read_total`, `_lines_shipped_total`,
`_lines_quarantined_total`, `_batches_rejected_total`,
`_bisects_total`, `_queue_bytes`, `_checkpoint_lag_bytes`,
`_files_open`, `_files_lost_total`, `_pk_disambiguated_total`.

**Shipped as T-1** (2026-08-18): `GET /metrics` and `GET /healthz`, served
when `[telemetry]` is configured (§5). 26 series, including the P1-7
exposure (`at_risk_lines`, `inflight_batches`, `unread_bytes`) and the L4
credential (`credential_expiry_seconds`, `_healthy`,
`_renewals_refused`). Evidence: `bench/results/t1-self-telemetry.log`.

`tributary_credential_expiry_seconds` is the series to page on: a renewal
that silently stops landing shows up there long before the handshake
starts failing. It reports **-1 when no certificate is configured**,
deliberately distinct from 0, so an agent that never had one does not look
like one whose certificate just expired.

#### What `/healthz` means — and what it deliberately does not

It reports **liveness**: is the main loop still turning? That is the one
question a restart can answer. It does **not** go red when TimeLakeDB is
unreachable.

That is the important decision here, not an oversight. A shipper whose
liveness probe fails on database trouble gets killed by its orchestrator
exactly when the queue is doing its job — and the restart discards the
batch being assembled and every batch in flight, which §3.4 measures as
the whole of the node-loss RPO. The monitoring turns a recoverable outage
into data loss.

So an outage surfaces as `status: degraded` and `shipping: false` in the
body, and as `queue_bytes` climbing while `lines_shipped_total` stays
flat — where an operator and an alert can see it, while the process is
left alone. Use `shipping` for a *readiness* probe if you want
traffic-shaping behaviour; do not wire it to liveness. The only thing that
returns 503 is a main loop that has not turned for 60 seconds.

`lines_read` minus `lines_shipped` minus `lines_quarantined` should be
the queue depth. If it is not, something is being lost, and the metric
set is designed so that arithmetic is checkable from the outside.

---

## 7. Milestones

Every phase is gated by a recorded run, never by unit tests alone. The
evidence lives in `bench/results/`.

| M | Deliverable | Gate | State |
|---|---|---|---|
| **L0** | Publish `timelake-ingest`; workload generator; tail → map → ship | Exact count on a static file | **shipped** — `l0-exact-count.log` |
| **L1** | Rotation, checkpoints with `seq`, crash-resume, quarantine | Exact count under rotation and `SIGKILL` | **shipped** — `l1-rotation-resume.log` |
| **L2** | Disk queue, backpressure, multiline, bisect | Exact count across a 60 s outage | **shipped** — `l2-queue-bisect-watermark.log` |
| **L3** | Full-scale run, memory bound, container image, docs site | The whole §6.1 table green, recorded | **shipped** — `l3-throughput.log` |
| **L4** | Client certificates (§5 `[output.tls]`), rotation with validate-before-swap | Rotate both certificates under sustained shipping: exact count, a rejected renewal keeps the last-good pair, an anonymous caller still served | **shipped** — `l4-mtls-rotation.log`, 10/10 |
| **L5** | Discovery and cloud metadata; a DaemonSet | Exact count across a node drain | planned |
| **L6** | Flight `DoPut` | Gated on L3's measurement showing line protocol is the bottleneck | planned |

Two items outside the L-phases:

| | Deliverable | Gate | State |
|---|---|---|---|
| **P0-5** | Present the data-plane token | Never logged; spools rather than drops on 401 | **shipped** — `p05-data-auth.log` |
| **P1-7** | State the queue's RPO (§3.4) | Measured under both failure models, not asserted | **shipped** — `p17-queue-rpo.log` |
| **T-1** | Self-telemetry: `/metrics`, `/healthz` (§6.2) | Counters move under load; the §6.2 arithmetic checks out from a live scrape; a real outage leaves liveness green | **shipped** — `t1-self-telemetry.log`, 16/16 |

L0 starts with the **workload generator**, not the tailer. It is the
piece that makes every later claim checkable, and the piece most likely
to be skipped under pressure.

---

## 8. Scope

**In v1:** log *files*. Globs, rotation, multiline, four parsers,
quarantine, queue, backpressure, metrics.

**Deliberately out:** metric extraction and aggregation (Telegraf already
does that, over the same wire, and is a first-class integration —
FR-8/FR-9); fan-out to other sinks (this is a TimeLakeDB agent, not a
router); a transformation DSL. Container, journald and Windows Event Log
sources were plausible v2 when this was written and have since shipped as
push/pull additions on the same map -> queue -> ship path — each behind a
feature (`docker_json` needs none, `journald` links libsystemd, `winlog`
links wevtapi on Windows), so the default build stays as small and portable
as v1 was. Still out: metric extraction, fan-out, a transformation DSL.

Refusing to become Vector is what keeps this small enough to prove.

---

## 9. Decisions and alternatives considered

| Decision | Chose | Over | Because |
|---|---|---|---|
| Build shape | Native Rust agent | Vector/Fluent Bit config | Local validation against the real parser, bisect-on-400, and deterministic PK assignment all require owning the HTTP client (§4.3) |
| PK uniqueness | Sub-tick sequence in the timestamp | A `seq` tag | A tag widens the primary key and creates a near-unique dictionary; the timestamp has unused precision sitting there |
| Delivery | At-least-once + deterministic PK | Two-phase or dedup cache | LWW dedup already exists in the database (FR-5); determinism makes it free |
| Tag selection | Config allowlist | Auto-promote parsed keys | Auto-promotion is how time-series databases get destroyed, and here it silently changes dedup |
| Field types | Declared in config | Inferred from first write | The database's first-writer-wins is permanent (§1.3); order is not a schema |
| Queue exhaustion | Stop reading, alarm | Drop oldest | Silent loss is the one thing this design exists to prevent |
| Wire evolution | Arrow Flight `DoPut` (`ROADMAP.md` §3) | A custom binary protocol | Line protocol repeats every tag and field key on every line; Arrow carries them once per batch and sends tag values as dictionary indices — TimeLakeDB's own representation (FR-2). A bespoke protocol could only win by discarding generality, and the generality is what makes Arrow cheap |
| Watermark lateness | Observed per stream (`ROADMAP.md` §2.2) | A configured constant | File skew is structural (`min` across open files); only the residue needs estimating, and a constant goes stale the moment logging changes |

---

## 10. Risks, each with its falsification test

1. **The disambiguator fabricates ordering that users read as truth.**
   *Mitigation: documented at the config field, in the reference, and in
   the README. Test: none possible — this is a documentation risk, and
   the honest response is prominence, not code.*
2. **Rotation races lose the tail of a file.** *Test: §6.1 rotation
   scenario at 10 MB intervals under sustained write, exact count.*
3. **`timelake-ingest` drifts from the server's parser.** *Test: the
   crate is versioned and shared, and L2 adds a contract test that
   round-trips a corpus through both the local validator and a live
   server.*
4. **Checkpoint fsync cost bounds throughput.** *Test: measure at L1;
   the lever is checkpoint interval, traded against replay volume — and
   replay is safe, so the interval can be generous.*
5. **A high-cardinality tag reaches the allowlist by accident.**
   *Mitigation: a startup warning when a tag's distinct count in the
   first N batches exceeds a threshold. Test: L2, with a deliberately
   bad config.*
