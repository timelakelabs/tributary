# Tributary

[![ci](https://github.com/timelakelabs/tributary/actions/workflows/ci.yml/badge.svg)](https://github.com/timelakelabs/tributary/actions/workflows/ci.yml)

A log-file agent for [TimeLakeDB](https://github.com/timelakelabs/timelakedb).

A tributary feeds a lake. This one tails log files and writes them into
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
