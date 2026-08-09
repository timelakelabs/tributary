# Tributary — Roadmap

**Status:** Draft v1 · 2026-08-09 · companion to `DESIGN.md`
(section references below point there).

Phases are gated the way TimeLakeDB's are: **no phase is done on unit
tests alone**, each one ends with a recorded run in `bench/results/`, and
the assertion from `DESIGN.md` §6.1 — *lines written == rows stored,
exactly* — holds at every gate, not just the last.

---

## 0. Two tracks, and why it matters

Three of the goals below cannot be built in this repository alone. They
need TimeLakeDB to grow a capability first, and pretending otherwise
would produce a roadmap that stalls halfway through a phase.

| Goal | Needs from TimeLakeDB | State there |
|---|---|---|
| mTLS | A client-certificate verifier on the listeners | **Designed, not built** — SEC-3 specifies the `RootCertStore` behind the same `ArcSwap`, with dual-CA overlap |
| Authenticated identity for SEC-2 labels | Data-plane authentication | Not started — SEC-4 is admin-surface only, and calls this "its own migration" |
| Faster wire | Flight `DoPut` ingest | Not implemented — recorded as a known gap |

Everything else — buffering, watermarks, bursts, rotation, cloud
metadata — is Tributary's alone. The roadmap sequences the independent
work first so the dependent work lands on a proven base.

---

## 1. Phases

### L0 · Prove the data model
**Workload generator first**, then tail → map → ship against a static
file. The generator is what makes every later claim checkable, and it is
the piece most likely to be skipped under pressure.
Also: publish `timelake-ingest` as a standalone crate (`DESIGN.md` §4.3).

*Gate:* exact count on a static 1M-line file; `--rate 10k/s` at
millisecond resolution proves the `DESIGN.md` §3.1 disambiguator (without it this
loses ~90% and reports success).

### L1 · Durability
Rotation across all three styles, checkpoints carrying `last_tick_ns`
and `seq` (`DESIGN.md` §3.2), crash-resume, quarantine of un-coercible lines.

*Gate:* exact count under 10 MB rotation; `SIGKILL` mid-stream resumes
with nothing lost **and nothing duplicated**; replay from a stale
checkpoint leaves the row count unchanged.

### L2 · Availability and watermarks
The disk queue, backpressure, bisect-on-400, multiline — and
**watermarks**, specified in §2.

*Gate:* 60 s database outage absorbed with nothing lost and no
hammering; watermark never advances past a line that is not durable.

### L3 · Burst and throughput
Bounded-concurrency in-flight batches (the design implies one request at
a time; a burst needs a pipeline), adaptive batch sizing, and honouring
`429` + `Retry-After` under sustained pressure.

Two burst shapes, and they fail differently:
- **Source burst** — a service dumps 100k lines at once. TimeLakeDB
  absorbs 100k in 0.12 s (PR-7), so the risk is Tributary's own read and
  encode path, not the database.
- **Catch-up burst** — after an outage the queue drains at maximum rate
  into a database that is also serving queries. The risk is Tributary
  becoming the reason someone else's query gets slow, so the drain rate
  is bounded and configurable.

*Gate:* both shapes at full scale, exact count, bounded RSS, and a
recorded ceiling in lines/second — **plus a measured breakdown of where
time goes**, which is the input L6 is gated on.

### L4 · Identity and mTLS  ⟂ *needs TimeLakeDB client-cert verification*
Tributary presents a client certificate; TimeLakeDB verifies it. Both
sides reuse the `timelake-tls` crate, so this inherits validate-
before-swap loading, the `ArcSwap` resolver consulted per handshake, and
last-good-on-bad-renewal — including on the client, which matters
because SEC-3 assumes ~24 h certificates.

Certificates arrive through a `CredentialSource` seam (§4), whose first
two backends are **files** and **HashiCorp Vault PKI**. Vault is not a
bolt-on here: SEC-3's whole design assumes short-TTL, hot-rotated certs,
and Vault's PKI engine is the canonical way to issue them. The agent
renews before expiry, validates before swapping, and on a Vault outage
keeps serving with the last-good certificate while alarming — the same
posture the database already takes on a bad renewal.

The part worth more than the encryption: **a client certificate is an
identity.** SECURITY.md records that SEC-2 authorizations are
"self-asserted claims" — `X-TimeLake-Authorizations` is whatever the
caller says. A verified client cert is exactly the credential that turns
those claims into grants, for machine clients, without the full
data-plane auth migration that would break Telegraf and Grafana. That
makes mTLS the cheapest available answer to the project's most-cited
security exposure, and it is worth designing on the TimeLakeDB side with
that in mind rather than as transport hardening.

*Gate:* an AT-7-style drill — rotate both server and client certificates
under sustained shipping, exact count, zero dropped connections, and a
rejected renewal keeps the last-good pair serving.

### L5 · Discovery and cloud
Endpoints arrive through an `EndpointSource` seam (§4) — static config,
**HashiCorp Consul**, DNS/SRV, or a Kubernetes service. Consul first,
because TimeLakeDB's own `Discovery` trait already names it as the v2
backend (CL-5), so both halves of the system learn topology the same way
from the same registry.

Then "cloud support", decomposed, because it is four unrelated things:

1. **Container log sources** — `/var/log/containers/*.log`, which are
   files, so v1's tailer already handles them. What is new is the
   **metadata**: pod, namespace, node, container, and selected labels,
   enriched onto each line. This is where `DESIGN.md` §2's tag allowlist earns its
   keep — Kubernetes labels are exactly the "promote everything" trap.
2. **Deployment** — a DaemonSet, a Helm chart, and a health/readiness
   surface. One Tributary per node, tailing every container.
3. **Identity** — short-lived certificates from the platform's workload
   identity rather than files on disk: a third `CredentialSource`
   backend (SPIFFE / projected service-account tokens) alongside Vault.
   Same mechanism as L4, sourced differently, which is why L4 comes
   first.
4. **Ephemeral nodes** — a node can vanish with a non-empty queue. The
   honest position is that the queue is *node-local durability, not
   replication*: on a spot instance it buys minutes, not guarantees.
   The mitigation is a shorter checkpoint interval and a smaller queue,
   documented as a trade rather than implied to be safe.

*Gate:* a kind/k3s cluster, a DaemonSet, pods that log through rotation
and eviction, exact count across a node drain.

### L6 · The fast wire  ⟂ *needs TimeLakeDB `DoPut`* · *gated on L3's measurement*
See §3 below. **Do not start this until L3 says line protocol is the
bottleneck.**

---

## 2. Watermarks

A watermark is a completeness claim: *every line from this stream with a
timestamp at or before `T` is durably stored.* It is the thing a reader
cannot otherwise know, and it is cheap for Tributary to know because it
already tracks exactly this to checkpoint safely.

**Definition.** Per stream, the low watermark is the source timestamp of
the last line whose batch returned `204`, minus a configured lateness
allowance. The allowance exists because logs are not perfectly ordered —
a multiline join completes late, and some sources stamp at write time
rather than event time.

**Two consumers, and the second is the interesting one:**

- *Operational* — `tributary_watermark_seconds` and
  `tributary_watermark_lag_seconds` per stream, alongside
  `tributary_checkpoint_lag_bytes`. The alert that matters is a
  watermark that stops advancing while lines are still being read.
- *Analytical* — Tributary writes watermarks **into TimeLakeDB** as an
  ordinary table (`tributary_watermarks`: tags `stream`, `host`; field
  `watermark_ns`). A dashboard can then distinguish "this window is
  empty" from "this window is not complete yet", which every log system
  gets wrong and almost none expose. It costs one line protocol row per
  stream per flush interval.

Watermarks also make the flow-control thresholds honest: the queue's
high/low marks (stop reading / resume reading) are published as metrics
with their configured values, per RR-5's rule that guardrails are
visible, tunable, and never silent.

---

## 3. The fast wire: adopt Flight, do not invent a protocol

The stated goal is "a custom over-line protocol to send data faster".
The recommendation is to **not build a custom protocol**, and to
implement **Arrow Flight `DoPut`** instead — which is already a
documented gap on the TimeLakeDB side rather than a new surface.

**Why line protocol is slow, precisely.** It is text: every float and
timestamp is encoded to decimal and re-parsed, and — the dominant cost
for logs — **every tag key and field key is repeated on every line**. A
5,000-line batch with five tags carries those five key names 5,000 times.
Arrow carries them once per batch, and tag *values* travel as dictionary
indices rather than repeated strings, which is the same representation
TimeLakeDB uses internally (FR-2). The encode/decode round trip through
text exists only to be undone.

**Why Flight rather than something bespoke:**

| | Arrow Flight `DoPut` | A custom binary protocol |
|---|---|---|
| Server work | Implement an existing, specified verb | Design, version, and document a new one |
| Client tooling | Every language has Arrow | Only what you write |
| Wire efficiency | Columnar, dictionary-encoded, zero-copy | At best the same |
| Fit | TimeLakeDB already serves Flight SQL on 1964 (FR-8) | A second protocol to secure, authenticate and operate |
| mTLS/auth | Inherits the L4 story on the same listener | Needs its own |

A bespoke protocol could only win by discarding generality, and the
generality is what makes Arrow cheap here.

**The honest cost.** Encoding moves from the server to the agent — and
in the L5 DaemonSet shape, that is one encoder per node instead of one
decoder per cluster, which is usually the right direction but is *not*
free on a CPU-constrained node. That trade is measurable, and it is the
second thing L6 must report.

**The gate before starting.** L3 produces a time breakdown of the ship
path. If line-protocol encoding plus server-side parsing is under ~10% of
it, L6 is not the bottleneck and should wait — the same discipline that
made TimeLakeDB's performance log reject bloom caching and TCP_NODELAY
after measuring that the thing being optimised was never on the critical
path.

---

## 4. Two seams: identity and discovery

Vault and Consul are backends, not features. The lesson the database
already learned four times over — `Store`, `Kms`, `Catalog`, `Discovery`
— is that the integration goes behind a trait, and the trait is what the
engine depends on. Tributary needs exactly two.

```rust
/// Where the client identity comes from (L4).
trait CredentialSource {
    fn current(&self) -> Result<CertifiedKey>;   // validated before use
    fn refresh_before(&self) -> Duration;        // renew ahead of expiry
}

/// Where TimeLakeDB is (L5).
trait EndpointSource {
    fn endpoints(&self) -> Vec<Endpoint>;        // healthy instances only
    fn watch(&self) -> Receiver<Vec<Endpoint>>;  // changes, not polling
}
```

| Seam | v1 | Then | Later |
|---|---|---|---|
| `CredentialSource` | files on disk | **Vault PKI** | SPIFFE / K8s projected tokens, cloud IAM |
| `EndpointSource` | static config | **Consul** (health-filtered) | DNS/SRV, K8s service |

### 4.1 Vault, concretely

The PKI secrets engine issues the client certificate; the agent
authenticates with AppRole (VMs) or Kubernetes auth (pods) and renews at
a fraction of the TTL. Three rules, each borrowed from something the
database already does:

- **Validate before swap.** A malformed or expired issuance never
  reaches the resolver; the last-good pair keeps shipping and a named
  alarm fires. This is `timelake-tls`'s behaviour, reused rather than
  reimplemented.
- **Vault being down is not an outage.** Certificates are valid for
  hours; a control-plane failure must degrade to "keep working, warn
  loudly", never to "stop shipping". The alarm is
  `tributary_credential_expiry_seconds`, which is the thing to page on.
- **Nothing unencrypted on disk.** Issued keys live in memory. If a
  deployment needs restart-without-Vault, that is an explicit
  configuration with its own warning, not a silent cache.

Vault can also hold the SEC-2 visibility expression for a source, which
is a more defensible place for it than a config file on the node — the
label is a security assertion, and it should come from the same place as
the identity that vouches for it.

### 4.2 Consul, concretely

Discovery of TimeLakeDB endpoints via health-filtered service lookup and
blocking queries (watch, don't poll). Tributary is a client, so it
consumes the catalogue; optionally it also registers itself so an
operator can see which hosts have a shipper running and healthy.

**The rule the database states for its own use of Consul applies here
unchanged:** discovery informs routing and availability only — a stale or
lying membership view may waste work but must never corrupt state. That
holds for Tributary for a specific reason worth writing down: delivery is
idempotent by construction (`DESIGN.md` §3), so shipping a batch to a
node that has just left the cluster costs a retry and nothing else. The
determinism that makes retries free is the same property that makes
discovery failures harmless.

One honest alternative: in a **Consul Connect** service mesh, the sidecar
already terminates mTLS, and Tributary would speak plaintext to a local
proxy instead of managing its own certificates. That is a legitimate
deployment and it makes L4 unnecessary for those users — worth supporting
and documenting rather than pretending everyone wants agent-managed
certs.

---

## 5. Cross-repo dependencies, as issues to file

| Tributary phase | TimeLakeDB work | Where it is already specified |
|---|---|---|
| L4 | Client-certificate verification on both listeners, `RootCertStore` behind `ArcSwap`, dual-CA overlap | SEC-3, "v2 mTLS" |
| L4 (stretch) | Map a verified client identity to granted SEC-2 authorizations | SEC-4 "phased"; SECURITY.md exposure 7 |
| L6 | Flight `DoPut` ingest: accept RecordBatches into the write buffer | Known gap in the reference docs |
| L2 (optional) | Nothing — watermarks are an ordinary table | — |

The mTLS item is the one to file first: it is designed, it is small, and
it unblocks both L4 and the most-cited security exposure in the project.

---

## 6. Sequencing

```
L0 ── L1 ── L2 ── L3 ──┬── L4(mTLS)* ── L5(cloud)
                       └── L6(Flight)*   ← only if L3's measurement says so

* blocked on TimeLakeDB work; file those issues at L2 so they are ready
```

L0–L3 are independent and should be built in order. L4 and L6 both
depend on the database, so their issues are filed early and their
Tributary-side work begins when the server side lands. L5 follows L4
because cloud identity is the same mechanism as mTLS, sourced from a
platform instead of a file.

---

## 7. Open questions

1. **Lateness allowance for watermarks** — a fixed configured value, or
   observed from the stream's actual out-of-orderness? Start fixed;
   revisit with real corpora at L2.
2. **Queue durability on ephemeral nodes** — is node-local disk
   acceptable, or does a spot-instance deployment need the agent to ship
   synchronously with a smaller batch? Measure at L5 before recommending.
3. **One Tributary per node or per pod** — DaemonSet is the default, but
   a sidecar gives per-tenant identity under mTLS. Decide with L4's
   identity model, not before.
4. **Backfill** — reading a large existing file from the start is a
   burst with no rate limit. Does it share the L3 drain budget, or get
   its own? Probably its own, so a backfill cannot starve live tailing.
