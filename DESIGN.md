# Tributary — Design

**Status:** Draft v1 · 2026-08-09 · a log-file agent for
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

### 3.3 What the queue does and does not promise (RPO)

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

Globs resolve to files; files are tracked by `(device, inode)`, not by
path, so a rotation does not look like a truncation. The tailer handles
the three rotation styles — rename-and-recreate, copy-and-truncate, and
create-new-with-suffix — plus the case that matters most in practice:
**finishing the tail of a rotated-away file before following the new
one**, so the last few kilobytes before a rotation are not lost.

A file whose inode disappears while unread bytes remain is a named,
counted event (`tributary_files_lost_total`), never a silent gap.

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

Results land in `bench/results/` with the run recorded, in the same
format as the database's own evidence. A claim without a run behind it
does not go in the README.

### 6.2 Observability

`tributary_lines_read_total`, `_lines_shipped_total`,
`_lines_quarantined_total`, `_batches_rejected_total`,
`_bisects_total`, `_queue_bytes`, `_checkpoint_lag_bytes`,
`_files_open`, `_files_lost_total`, `_pk_disambiguated_total`.

`lines_read` minus `lines_shipped` minus `lines_quarantined` should be
the queue depth. If it is not, something is being lost, and the metric
set is designed so that arithmetic is checkable from the outside.

---

## 7. Milestones

| M | Deliverable | Gate |
|---|---|---|
| **L0** | Publish `timelake-ingest`; workload generator; tail → map → ship | Exact count on a static file |
| **L1** | Rotation, checkpoints with `seq`, crash-resume, quarantine | Exact count under rotation and `SIGKILL` |
| **L2** | Disk queue, backpressure, multiline, bisect | Exact count across a 60 s outage |
| **L3** | Full-scale run, memory bound, container image, docs site | The whole §6.1 table green, recorded |

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
router); a transformation DSL. Container and syslog/journald sources are
plausible v2, and are excluded now so that v1 can be *correct* rather
than broad.

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
