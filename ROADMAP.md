# Tributary — Roadmap

**Status:** v1 · updated 2026-08-18 (L0–L4, P0-5, P1-7, T-1 shipped) · companion to `DESIGN.md`
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
| Optional (want-mode) mTLS | A client-certificate verifier in *allow-unauthenticated* mode, with the verified identity reaching the query session | **Shipped** (SEC-3 v2) — want-mode client certs, dual-CA overlap, identity plumbed into the query session. Tributary presenting a client certificate is L4. |
| Authenticated identity for SEC-2 labels | Data-plane authentication | **Shipped both sides.** TimeLakeDB has token auth (`TIMELAKE_DATA_AUTH=off\|optional\|required`, SEC-4 phased); **Tributary presents the token (P0-5, done 2026-08-10)** — `bench/results/p05-data-auth.log`. Token grants intersect a caller's SEC-2 claims, so a token *is* the authenticated identity. |
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

### L4 · Identity and *optional* mTLS — **SHIPPED 2026-08-17**

**Gate met**, `bench/results/l4-mtls-rotation.log` — 10/10 against a live
TLS node: the agent ships presenting `CN=tributary-node-1`, both the server
and the client certificate rotate under sustained shipping, a rejected
renewal keeps the last-good pair shipping, an anonymous caller is served
throughout (AT-6 not regressed), and 15,000 written lines read back exactly
15,000.

What landed: `[output.tls]` with `ca_file` / `cert_file` / `key_file` /
`refresh_secs` (`src/config.rs`), a `CredentialSource` seam whose files
backend is `src/credential.rs`, and rotation wired through `src/ship.rs`.
The discipline is the server's, deliberately: validate before swap, refuse
an expired or inconsistent pair, keep the last-good identity on a bad
renewal, adopt atomically. The two repositories cannot share
`timelake-tls` itself — nothing is published and Tributary is its own
workspace — so they share the rules and the dependency versions instead;
`credential.rs` mirrors `load_pair` check for check.

Deviations from the sketch below, both deliberate: the seam returns
validated PEM rather than a rustls `CertifiedKey`, because reqwest wants
PEM; and `refresh_before()` is not implemented yet, because the files
backend does not *request* a renewal — something else writes the file and
the agent notices — so the cadence is `refresh_secs`. It arrives with the
Vault backend that actually needs it.

**Known limitation, TimeLakeDB-side.** The certificate is verified at the
TLS layer, but its CN reaches no HTTP handler: TimeLakeDB extracts the peer
identity only on the Flight listener, and records `/api/sql` identity as
NOT DONE (a custom `Accept` is needed, because axum-server owns that accept
loop). Tributary writes over HTTP, so today the certificate buys
handshake-level verification, **not** identity-based authorization on the
write path — no SEC-2 grant intersection, no per-identity attribution.
That is consistent with the "not, by itself, a security control" caveat
below, and it is the piece to close before L4's identity half means what
its name suggests.

<details>
<summary>The original L4 plan, kept for the reasoning</summary>


**The server runs in "want" mode, not "require" (decided 2026-08-09).**
It requests a client certificate, verifies one if presented, and accepts
the connection either way. Grafana, Telegraf and the bench harness
connect exactly as they do today, with no configuration change and no
certificate; Tributary presents one and is identified.

In rustls this is `allow_unauthenticated()` on the client-verifier
builder, and after the handshake `peer_certificates()` is `Some` for an
authenticated peer and `None` for an anonymous one — so the server
learns *who* without ever refusing *whether*.

This is the same posture the project already takes one level down: TLS
itself is opt-in via `TIMELAKE_TLS_CERT`/`_KEY`, plaintext is the
default, and the fixtures are unchanged. Optional client auth extends
that discipline rather than inventing a new one.

Tributary presents the certificate; both sides reuse the `timelake-tls`
crate, so this inherits validate-before-swap loading, the `ArcSwap`
resolver consulted per handshake, and last-good-on-bad-renewal —
including on the client, which matters because SEC-3 assumes ~24 h
certificates.

**The caveat that keeps this honest:** want-mode mTLS is not, by itself,
a security control. An attacker simply declines to present a certificate
and takes the anonymous path. Its value is entirely in what the two
paths are *allowed to do differently* — which is the next paragraph, and
which is why this is worth building now rather than later.

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
caller says. A verified client certificate is exactly the credential
that turns those claims into grants, and want-mode is what lets that
arrive **without a flag day**:

| Connection | Today | With want-mode client auth |
|---|---|---|
| Grafana, Telegraf, bench (no cert) | claims trusted as asserted | **unchanged** — nothing breaks |
| Tributary (verified cert) | claims trusted as asserted | claims **intersected** with what that identity is granted |

So the migration is additive: authenticated clients get real
authorization immediately, anonymous ones keep today's documented
behaviour, and the decision to eventually *restrict* what anonymous can
do becomes a separate, deliberate step that no longer breaks Grafana on
the way. That is the cheapest available answer to the project's
most-cited security exposure, and it is why the TimeLakeDB-side issue
should be written as "optional client-cert verification **with the
verified identity plumbed into the query session**" rather than as
transport hardening — the plumbing is the point, the encryption is
incidental.

An operational corollary worth building at the same time: export
`timelake_tls_client_authenticated_total` alongside
`timelake_tls_client_anonymous_total`. The ratio is what tells an
operator when a deployment has actually finished migrating and it is
safe to flip a listener to `require` — a decision that should be made
from a metric rather than a guess.

*Gate:* an AT-7-style drill — rotate both server and client certificates
under sustained shipping, exact count, zero dropped connections, a
rejected renewal keeps the last-good pair serving, **and Grafana's
fixture dashboards keep rendering throughout without a client
certificate** (AT-6 must not regress).
**Met 2026-08-17** — `bench/drill-l4.sh`, evidence in
`bench/results/l4-mtls-rotation.log`.

</details>


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
   **Quantified 2026-08-17 (P1-7)**, and the mitigation turned out to be
   stated slightly wrong: the checkpoint interval governs *duplicates on
   restart*, not loss on node death. What bounds the loss is
   `batch_lines * (1 + max_inflight)` — the unacked window — plus the queue
   while the server is refusing writes. See §5 open question 2 and
   `bench/results/p17-queue-rpo.log`.

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

### 2.1 Structural first, statistical only where it must be

Out-of-orderness has two sources, and conflating them makes the
watermark far more conservative than it needs to be.

**Between files, it is structurally knowable.** Within one file, lines
are ordered — offset order is time order, which is the same assumption
`DESIGN.md` §3.1 already relies on. So per-file progress is *exact*: the
timestamp of the last line from that file whose batch returned `204`.
A stream's watermark is then

```
watermark(stream) = min over open files of acked_ts(file)  −  lateness
```

The `min` handles file skew — one file at T+5 s and another still at T —
without any estimation at all. A file discovered but not yet started
(backfill) holds the watermark at its first timestamp, which is correct
and occasionally surprising; §2.3 covers what that means for monotonicity.

**Within a file, it is not.** A multiline join that began at T completes
and is emitted seconds later; some sources stamp at write time rather
than event time. Only this residue needs estimating, and it is much
smaller than the naive whole-stream estimate would be.

### 2.2 Observing the lateness (decided 2026-08-09)

The allowance is **observed from the stream**, not configured. For every
emitted line, lateness is `max_ts_seen_so_far − this_ts`; Tributary keeps
a rolling high quantile (p99.9 over a bounded window) per stream and uses
that as the allowance.

Four properties keep it honest:

- **Quantile, not maximum.** One pathological line must not pin the
  watermark behind it forever. The cost is that a small fraction of lines
  legitimately arrive below the watermark — which is measured, not
  hidden (§2.3).
- **Floor and ceiling, both configured.** A perfectly ordered stream
  would observe zero lateness and produce a brittle watermark that any
  jitter violates, so the floor keeps a margin. The ceiling stops a
  pathological stream from stalling the watermark indefinitely; hitting
  it is an alarm, because it means the estimate has stopped being useful.
- **Cold start is conservative, and converges down.** This is the trap
  worth naming: an agent that restarts with no samples must *not* assume
  zero lateness, or its first published watermark over-claims
  completeness precisely when a reader is most likely to be checking
  after an incident. Tributary starts at the ceiling and tightens as
  samples accumulate, and the current estimate is persisted in the
  checkpoint so a restart resumes rather than re-learns.
- **It adapts.** A deployment that changes its logging (turning on stack
  traces, say) shifts the distribution, and the window follows it without
  anyone editing a config file. That is the whole reason to observe
  rather than declare.

### 2.3 Monotonicity, and what a violation means

The published watermark **never regresses**. A line arriving below it —
late data that the quantile did not cover, or a backfilled file
discovered after the fact — is *still written normally*; only the
completeness claim was optimistic. That event increments
`tributary_watermark_violations_total`.

This is the distinction that makes the feature trustworthy: a violation
is a measurable inaccuracy in a claim, **not** lost data. If the counter
is non-zero and rising, the floor is too tight or the window too short,
and both are tunable. If it is zero, the completeness claim is one a
dashboard can actually rely on.

**Two consumers, and the second is the interesting one:**

- *Operational* — `tributary_watermark_seconds`,
  `tributary_watermark_lag_seconds` and
  `tributary_watermark_lateness_seconds` (the observed allowance itself)
  per stream, alongside `tributary_checkpoint_lag_bytes`. The alert that
  matters is a watermark that stops advancing while lines are still
  being read; the second is the allowance sitting at its ceiling.
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

## 3. The fast wire: Arrow Flight, not a custom protocol (decided 2026-08-09)

The original goal was "a custom over-line protocol to send data faster".
**Decided: no custom protocol.** The fast path is **Arrow Flight
`DoPut`**, which is already a documented gap on the TimeLakeDB side
rather than a new surface to invent, secure and version.

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
| `CredentialSource` | files on disk — **shipped 2026-08-17** (`src/credential.rs`) | **Vault PKI** | SPIFFE / K8s projected tokens, cloud IAM |
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
| L4 | Client-certificate verification on both listeners in **want mode** (`allow_unauthenticated`), `RootCertStore` behind `ArcSwap`, dual-CA overlap, plus `timelake_tls_client_{authenticated,anonymous}_total` | SEC-3 "v2 mTLS", extended: SEC-3 assumes *required* mTLS |
| L4 | Intersect `X-TimeLake-Authorizations` with what the verified identity is granted; anonymous connections keep today's behaviour | SEC-4 "phased"; SECURITY.md exposure 7 |
| L6 | Flight `DoPut` ingest: accept RecordBatches into the write buffer | Known gap in the reference docs |
| L2 (optional) | Nothing — watermarks are an ordinary table | — |

The mTLS item is the one to file first: it is designed, it is small, and
it unblocks both L4 and the most-cited security exposure in the project.

---

## 6. Sequencing

```
L0 ── L1 ── L2 ── L3 ──┬── L4(mTLS)✓ ── L5(cloud)
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

1. ~~**Lateness allowance for watermarks**~~ — **decided 2026-08-09:
   observed from the stream** (§2.2). File skew is handled structurally
   by the `min` across open files, so only the genuinely unpredictable
   residue is estimated. Cold start begins at the ceiling and tightens,
   because the alternative over-claims completeness exactly when someone
   is checking after an incident. Remaining sub-question for L2, to
   settle against real corpora rather than argument: the window length
   and the floor.
2. ~~**Queue durability on ephemeral nodes**~~ — **ANSWERED 2026-08-17**
   (P1-7), measured rather than argued: `bench/results/p17-queue-rpo.log`.
   On a durable disk a process restart loses nothing (L1's property,
   re-verified). On node loss the RPO is bounded by
   `batch_lines * (1 + max_inflight)`, plus the queue if the server was
   refusing writes — at the shipped defaults, 25,000 lines. A smaller batch
   does help: 50x lower bound and 10x lower observed peak. It does **not**
   need to be synchronous. The agent now prints its live exposure every
   `rpo_report_secs`, so the trade is chosen rather than discovered.
   Worth recording: the first measurement said the opposite, because a
   single `kill -9` samples the flush sawtooth and says as much about
   timing as about configuration. See the log.
3. **One Tributary per node or per pod** — DaemonSet is the default, but
   a sidecar gives per-tenant identity under mTLS. Decide with L4's
   identity model, not before.
4. **Backfill** — reading a large existing file from the start is a
   burst with no rate limit. Does it share the L3 drain budget, or get
   its own? Probably its own, so a backfill cannot starve live tailing.
