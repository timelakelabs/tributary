# Tributary

[![ci](https://github.com/timelakedb/Tributary/actions/workflows/ci.yml/badge.svg)](https://github.com/timelakedb/Tributary/actions/workflows/ci.yml)

A log-file agent for [TimeLakeDB](https://github.com/timelakedb/TimeLakeDB).

A tributary feeds a lake. This one tails log files and writes them into
TimeLakeDB over line protocol — the same wire Telegraf already uses for
metrics, so one host ships both through one endpoint and one data model.

**Status: phases L0–L3 shipped, plus data-plane authentication.** Tailing,
rotation and crash-resume, the durable queue with poison isolation and
observed watermarks, throughput, and presenting a bearer token to
TimeLakeDB without ever logging it. Every phase is gated by a recorded run
rather than by unit tests alone — see `bench/results/`:

| Phase | What it proved | Evidence |
|---|---|---|
| L0 | Exact count on a static file; the millisecond disambiguator is real | `bench/results/l0-exact-count.log` |
| L1 | Rotation and crash-resume, both exact | `bench/results/l1-rotation-resume.log` |
| L2 | Outage absorption, poison isolation, watermarks, multiline joins | `bench/results/l2-queue-bisect-watermark.log` |
| L3 | 156k → 492k lines/s (the checkpoint was the bottleneck) | `bench/results/l3-throughput.log` |
| P0-5 | Presents the data-plane token; never logs it; spools rather than drops on 401 | `bench/results/p05-data-auth.log` |

Next is L4 (client certificates), L5 (discovery and cloud metadata) and
L6 (the Flight `DoPut` wire, gated on TimeLakeDB growing it) — see
[`ROADMAP.md`](ROADMAP.md). [`DESIGN.md`](DESIGN.md) remains the
specification, and its §1 explains why this is a purpose-built agent
rather than a Vector configuration.

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
