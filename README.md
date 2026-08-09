# Tributary

A log-file agent for [TimeLakeDB](https://github.com/TimeLakeLabs/TimeLakeDB).

A tributary feeds a lake. This one tails log files and writes them into
TimeLakeDB over line protocol — the same wire Telegraf already uses for
metrics, so one host ships both through one endpoint and one data model.

**Status: design.** Nothing is built yet. Read
[`DESIGN.md`](DESIGN.md) — it is the specification, and §1 explains why
this is a purpose-built agent rather than a Vector configuration.

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
