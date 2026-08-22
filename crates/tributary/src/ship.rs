//! Shipping a batch to TimeLakeDB.
//!
//! Response handling follows the documented contract: 204 means
//! WAL-durable, 400 means a line was rejected and **nothing in the batch
//! was written**, 429 is explicit backpressure carrying `Retry-After`.
//!
//! Cloneable, with shared counters, so several batches can be in flight
//! at once (L3). `reqwest::Client` is itself a handle around a shared
//! pool, so cloning is cheap and connection reuse is preserved.

use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Default)]
pub struct Counters {
    pub shipped: AtomicU64,
    pub rejected: AtomicU64,
    pub bisects: AtomicU64,
    /// Nanoseconds spent waiting on HTTP. Paired with the agent's
    /// read/encode time, this is the breakdown that decides whether a
    /// faster wire (Arrow Flight, ROADMAP §3) is worth building.
    pub ship_ns: AtomicU64,
    pub requests: AtomicU64,
    /// Ships refused with 401/403. Non-zero means the token is wrong,
    /// missing, or unscoped for this database — visible, not inferred.
    pub unauthorized: AtomicU64,
    /// Consecutive transport-class failures. Reset by any response from a
    /// node that has a write path; drives the pool-rebuild backstop.
    pub transport_streak: AtomicU64,
    /// Times the client was rebuilt to shed its connection pool. Non-zero
    /// means the shipper decided its pooled connections could not be
    /// trusted — after a 501, or after enough consecutive transport
    /// failures. The counter exists because the failure it guards against
    /// is otherwise invisible: an agent retrying a wrong-but-answering
    /// peer looks, from every other number, like an ordinary outage.
    pub transport_rebuilds: AtomicU64,
}

#[derive(Clone)]
pub struct Shipper {
    /// The live client, behind an `ArcSwap` because an L4 rotation rebuilds
    /// it: reqwest bakes the client identity in at build time, so adopting a
    /// renewed certificate means a new `Client`, not a mutated one. Swapping
    /// a whole client is also why an in-flight batch never sees a
    /// half-applied rotation — it finished with the client it started on,
    /// and reqwest's pool keeps that connection alive until it drains.
    client: Arc<arc_swap::ArcSwap<reqwest::Client>>,
    /// How to rebuild the client when the identity rotates. `None` when no
    /// client certificate is configured, which is the unchanged path.
    tls: Option<Arc<TlsRuntime>>,
    url: Arc<String>,
    gzip: bool,
    /// Pre-built `Authorization: Bearer <token>` header, marked sensitive so
    /// reqwest's own diagnostics never print it. `None` = no credential (the
    /// node is presumably running `TIMELAKE_DATA_AUTH=off`). The `Secret` is
    /// consumed at construction and not retained in the clear.
    auth: Option<reqwest::header::HeaderValue>,
    pub counters: Arc<Counters>,
}

#[derive(Debug)]
pub enum ShipError {
    Rejected(String),
    Backpressure(Duration),
    /// TimeLakeDB refused the credential (401/403). Distinct from a
    /// transport error because retrying the wire will not fix a bad token —
    /// only a corrected `TRIBUTARY_TOKEN`/`token_file` will. The data is
    /// still spooled, never dropped.
    Unauthorized(String),
    /// The peer answered 501: it is a TimeLakeDB node with no write path —
    /// a querier, or whatever else inherited the address this shipper's
    /// pool is pinned to. Distinct from `Transport` because retrying the
    /// SAME CONNECTION cannot fix it, and that is exactly what a pooled
    /// retry does: DNS is consulted only on dial, and a connection that
    /// keeps answering HTTP is never redialed. Found live (C4,
    /// 2026-08-22): a reshape gave the router's old IP to a recreated
    /// querier, and four agents retried it at ~5 requests/second for half
    /// an hour, silently, while the real router sat healthy one address
    /// over. Raising this has already dropped the pool; the retry the
    /// caller schedules will dial — and resolve — fresh.
    WrongNode(String),
    Transport(String),
}

impl std::fmt::Display for ShipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShipError::Rejected(m) => write!(f, "batch rejected: {m}"),
            ShipError::Backpressure(d) => write!(f, "backpressure, retry in {d:?}"),
            ShipError::Unauthorized(m) => write!(
                f,
                "TimeLakeDB rejected the token ({m}) — check TRIBUTARY_TOKEN or \
                 [output].token_file, and that it is scoped to write this database"
            ),
            ShipError::WrongNode(m) => write!(
                f,
                "the peer holds no write path ({m}) — its connection pool was \
                 dropped so the next attempt dials and resolves fresh"
            ),
            ShipError::Transport(m) => write!(f, "transport: {m}"),
        }
    }
}

/// What a rotation needs to rebuild the client: the trust anchors (which do
/// not rotate here — the server's CA bundle is read once at startup) and the
/// rotating client identity.
pub struct TlsRuntime {
    pub roots: Vec<reqwest::Certificate>,
    pub identity: Arc<crate::credential::RotatingIdentity>,
}

/// Build a client, optionally with private trust anchors and a client
/// identity. One function so the startup path and every rotation produce
/// clients configured identically — a rotation that quietly dropped, say,
/// the timeout would be a very hard bug to see.
fn build_client(
    roots: &[reqwest::Certificate],
    identity: Option<reqwest::Identity>,
) -> anyhow::Result<reqwest::Client> {
    let mut b = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        // Enough idle connections for the in-flight pipeline.
        .pool_max_idle_per_host(16);
    for root in roots {
        b = b.add_root_certificate(root.clone());
    }
    if let Some(id) = identity {
        b = b.identity(id);
    }
    Ok(b.build()?)
}

impl Shipper {
    /// `tls` is `None` for plain HTTP or the public trust store — the path
    /// every pre-L4 deployment takes, unchanged.
    pub fn new(
        base_url: &str,
        database: &str,
        gzip: bool,
        token: Option<crate::auth::Secret>,
        tls: Option<Arc<TlsRuntime>>,
    ) -> anyhow::Result<Shipper> {
        // Build the header once. `set_sensitive(true)` tells reqwest to keep
        // it out of its own Debug/trace output — belt to the Secret's
        // suspenders, so the value cannot leak through the HTTP layer either.
        let auth = match token {
            Some(secret) => {
                let mut hv =
                    reqwest::header::HeaderValue::from_str(&format!("Bearer {}", secret.expose()))
                        .map_err(|_| {
                            anyhow::anyhow!("token contains bytes invalid for an HTTP header")
                        })?;
                hv.set_sensitive(true);
                Some(hv)
            }
            None => None,
        };
        let client = match &tls {
            None => build_client(&[], None)?,
            Some(rt) => {
                let id = match rt.identity.current() {
                    Some(i) => Some(i.reqwest_identity()?),
                    None => None,
                };
                build_client(&rt.roots, id)?
            }
        };

        Ok(Shipper {
            client: Arc::new(arc_swap::ArcSwap::from_pointee(client)),
            tls,
            url: Arc::new(format!(
                "{}/api/v3/write_lp?db={}",
                base_url.trim_end_matches('/'),
                database
            )),
            gzip,
            auth,
            counters: Arc::new(Counters::default()),
        })
    }

    /// Whether this shipper carries a credential (for a startup log line).
    pub fn is_authenticated(&self) -> bool {
        self.auth.is_some()
    }

    /// The client certificate's subject CN, if one is configured — the
    /// identity the server reads out of a verified chain.
    pub fn client_identity(&self) -> Option<String> {
        self.tls
            .as_ref()
            .and_then(|rt| rt.identity.current())
            .and_then(|i| i.common_name.clone())
    }

    /// Seconds until the client certificate expires, if one is configured.
    /// The alarm an operator watches: a renewal that silently stops landing
    /// shows up here long before the handshake starts failing.
    pub fn credential_expires_in_secs(&self) -> Option<i64> {
        self.tls
            .as_ref()
            .and_then(|rt| rt.identity.expires_in_secs())
    }

    /// False once a renewal has been refused, until one succeeds. True when
    /// no certificate is configured — nothing to be unhealthy about.
    pub fn credential_healthy(&self) -> bool {
        self.tls
            .as_ref()
            .map(|rt| rt.identity.last_reload_ok())
            .unwrap_or(true)
    }

    pub fn credential_reloads_refused(&self) -> u64 {
        self.tls
            .as_ref()
            .map(|rt| rt.identity.reloads_refused())
            .unwrap_or(0)
    }

    /// Check for a renewed client certificate and, if one validates, rebuild
    /// the client so subsequent batches present it.
    ///
    /// Returns `Ok(true)` when a rotation was adopted. A refused renewal is
    /// returned as an error for the caller to log — never propagated as a
    /// shipping failure, because the last-good identity is still working and
    /// a bad renewal must not stop a healthy agent.
    pub fn rotate_credentials(&self) -> anyhow::Result<bool> {
        let Some(rt) = &self.tls else {
            return Ok(false);
        };
        if !rt.identity.reload()? {
            return Ok(false);
        }
        // Validated already — this only fails if reqwest disagrees with the
        // gate, which would be a bug in the gate rather than a bad file.
        let id = rt
            .identity
            .current()
            .map(|i| i.reqwest_identity())
            .transpose()?;
        self.client.store(Arc::new(build_client(&rt.roots, id)?));
        Ok(true)
    }

    /// How many consecutive transport failures trigger a pool rebuild.
    ///
    /// Small on purpose. A rebuild is cheap — constructing a `reqwest`
    /// client is configuration, not I/O — and during a genuine outage the
    /// fresh pool's dials fail exactly like the old pool's did, so the
    /// only cost of rebuilding too eagerly is a warn line. The cost of
    /// rebuilding too late is the C4 wedge: a pool pinned to a peer that
    /// answers wrongly forever.
    const REBUILD_AFTER: u64 = 3;

    /// Drop the connection pool by swapping in a freshly built client.
    ///
    /// The one thing a shipper can do about a pool pinned to the wrong
    /// peer: hyper consults DNS only when it dials, and it only dials
    /// when it has no healthy pooled connection to reuse. Rebuilding
    /// reuses the `ArcSwap` machinery L4 rotation already established —
    /// in-flight batches finish on the client they started with.
    ///
    /// A rebuild that itself fails (an identity file gone missing at the
    /// wrong moment) keeps the old client: a poisoned pool that might
    /// recover beats no client at all, and the error is logged rather
    /// than allowed to turn a retry path into a crash.
    fn rebuild_transport(&self, why: &str) {
        let built = match &self.tls {
            None => build_client(&[], None),
            Some(rt) => rt
                .identity
                .current()
                .map(|i| i.reqwest_identity())
                .transpose()
                .map_err(anyhow::Error::from)
                .and_then(|id| build_client(&rt.roots, id)),
        };
        match built {
            Ok(c) => {
                self.client.store(Arc::new(c));
                // A fresh pool is a fresh start; the streak that earned the
                // rebuild should not immediately earn another.
                self.counters.transport_streak.store(0, Ordering::Relaxed);
                let n = self
                    .counters
                    .transport_rebuilds
                    .fetch_add(1, Ordering::Relaxed)
                    + 1;
                tracing::warn!(
                    rebuilds_total = n,
                    why,
                    "dropped the connection pool; the next attempt dials and \
                     resolves fresh"
                );
            }
            Err(e) => tracing::error!(
                error = %e,
                why,
                "could not rebuild the HTTP client; keeping the existing pool"
            ),
        }
    }

    /// A response arrived from a node that has a write path — whatever the
    /// status, the pool is pointed somewhere sane.
    fn note_write_path_answered(&self) {
        self.counters.transport_streak.store(0, Ordering::Relaxed);
    }

    /// A transport-class failure: no response, or a response that proves
    /// nothing about the peer being the right one. Every
    /// `REBUILD_AFTER`-th consecutive failure sheds the pool — the
    /// backstop for inheritors that answer 404, hang, or speak something
    /// other than HTTP, where no 501 ever names the problem.
    fn note_transport_failure(&self, what: &str) {
        let streak = self
            .counters
            .transport_streak
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        if streak.is_multiple_of(Self::REBUILD_AFTER) {
            self.rebuild_transport(what);
        }
    }

    /// Ship a batch, isolating anything the server rejects, and
    /// absorbing backpressure internally so the caller sees a batch
    /// either dealt with or genuinely undeliverable.
    ///
    /// The batch is atomic — one unparseable line writes zero of five
    /// thousand — so a 400 cannot simply be retried or the agent wedges
    /// on the poison line forever. Bisect instead: halve, ship each
    /// half, recurse into whichever half still fails.
    ///
    /// Returns the lines that could not be shipped, for quarantine.
    pub async fn send_lines(&self, lines: &[String]) -> Result<Vec<String>, ShipError> {
        let mut poison = Vec::new();
        // An explicit stack rather than recursion: an async fn cannot
        // recurse without boxing, and chunk order does not matter —
        // every line already carries its own timestamp.
        let mut stack: Vec<&[String]> = vec![lines];
        while let Some(chunk) = stack.pop() {
            if chunk.is_empty() {
                continue;
            }
            let body: String = chunk.concat();
            match self.send(&body).await {
                Ok(()) => {}
                Err(ShipError::Rejected(msg)) => {
                    if chunk.len() == 1 {
                        tracing::warn!(error = %msg, "quarantined by the server");
                        poison.push(chunk[0].clone());
                    } else {
                        let mid = chunk.len() / 2;
                        self.counters.bisects.fetch_add(1, Ordering::Relaxed);
                        stack.push(&chunk[mid..]);
                        stack.push(&chunk[..mid]);
                    }
                }
                Err(ShipError::Backpressure(d)) => {
                    // Explicit, named backpressure (RR-5): wait exactly
                    // as long as asked, then retry the same chunk.
                    tokio::time::sleep(d).await;
                    stack.push(chunk);
                }
                Err(other) => return Err(other),
            }
        }
        Ok(poison)
    }

    pub async fn send(&self, body: &str) -> Result<(), ShipError> {
        if body.is_empty() {
            return Ok(());
        }
        let started = std::time::Instant::now();
        // One load per request: the batch runs to completion on the client
        // it started with, so a rotation landing mid-flight cannot change
        // the identity underneath an open connection.
        let client = self.client.load();
        let mut req = client.post(self.url.as_str());
        if let Some(h) = &self.auth {
            req = req.header(reqwest::header::AUTHORIZATION, h.clone());
        }
        let bytes = if self.gzip {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            enc.write_all(body.as_bytes())
                .map_err(|e| ShipError::Transport(e.to_string()))?;
            req = req.header("content-encoding", "gzip");
            enc.finish()
                .map_err(|e| ShipError::Transport(e.to_string()))?
        } else {
            body.as_bytes().to_vec()
        };

        let res = match req
            .header("content-type", "text/plain; charset=utf-8")
            .body(bytes)
            .send()
            .await
        {
            Ok(res) => res,
            Err(e) => {
                self.note_transport_failure("request failed on the wire");
                return Err(ShipError::Transport(e.to_string()));
            }
        };

        let status = res.status().as_u16();
        // Any status below except the two transport-shaped ones came from
        // a node that has a write path, so the pool is pointed somewhere
        // sane and the failure streak resets.
        let out = match status {
            204 => {
                self.note_write_path_answered();
                self.counters
                    .shipped
                    .fetch_add(body.lines().count() as u64, Ordering::Relaxed);
                Ok(())
            }
            429 => {
                self.note_write_path_answered();
                let secs = res
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(1);
                Err(ShipError::Backpressure(Duration::from_secs(secs)))
            }
            400 => {
                self.note_write_path_answered();
                self.counters.rejected.fetch_add(1, Ordering::Relaxed);
                Err(ShipError::Rejected(res.text().await.unwrap_or_default()))
            }
            401 | 403 => {
                // The server's body carries a code/reason but never the
                // token, so it is safe to surface.
                self.note_write_path_answered();
                self.counters.unauthorized.fetch_add(1, Ordering::Relaxed);
                Err(ShipError::Unauthorized(format!(
                    "HTTP {status}: {}",
                    res.text().await.unwrap_or_default()
                )))
            }
            501 => {
                // The peer says so itself: "this node holds no write
                // path". A querier — or whatever inherited the address the
                // pool is pinned to. Retrying the same connection is the
                // one move guaranteed not to help, so drop the pool NOW
                // rather than after a streak: this response is not
                // ambiguous the way a 404 or a hang is.
                let msg = res.text().await.unwrap_or_default();
                self.rebuild_transport("the peer answered 501: no write path");
                Err(ShipError::WrongNode(format!("HTTP 501: {msg}")))
            }
            code => {
                // Includes an inheritor that answers 404 or some other
                // service's error page: a response proving nothing about
                // the peer being the right one counts toward the same
                // streak as no response at all.
                self.note_transport_failure("unrecognized response status");
                Err(ShipError::Transport(format!(
                    "HTTP {code}: {}",
                    res.text().await.unwrap_or_default()
                )))
            }
        };
        self.counters
            .ship_ns
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.counters.requests.fetch_add(1, Ordering::Relaxed);
        out
    }

    pub fn shipped(&self) -> u64 {
        self.counters.shipped.load(Ordering::Relaxed)
    }
    pub fn bisects(&self) -> u64 {
        self.counters.bisects.load(Ordering::Relaxed)
    }
    pub fn ship_ns(&self) -> u64 {
        self.counters.ship_ns.load(Ordering::Relaxed)
    }
    pub fn requests(&self) -> u64 {
        self.counters.requests.load(Ordering::Relaxed)
    }
    pub fn unauthorized(&self) -> u64 {
        self.counters.unauthorized.load(Ordering::Relaxed)
    }
    pub fn transport_rebuilds(&self) -> u64 {
        self.counters.transport_rebuilds.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The unchanged path: no TLS configuration at all. An agent that never
    /// heard of L4 must build exactly as before and report no identity.
    #[test]
    fn a_shipper_without_tls_presents_no_identity() {
        let s = Shipper::new("http://localhost:1963", "logs", true, None, None).unwrap();
        assert!(!s.is_authenticated());
        assert_eq!(s.client_identity(), None);
        assert_eq!(s.credential_expires_in_secs(), None);
        assert_eq!(s.credential_reloads_refused(), 0);
        // Rotation is a no-op rather than an error when nothing is configured.
        assert!(!s.rotate_credentials().unwrap());
    }

    /// CA-only: trust a private issuer without presenting a client
    /// certificate. This is Telegraf's shape, and it must not be mistaken
    /// for an identity.
    #[test]
    fn a_ca_only_shipper_trusts_without_identifying() {
        let rt = Arc::new(TlsRuntime {
            roots: Vec::new(),
            identity: crate::credential::RotatingIdentity::none(),
        });
        let s = Shipper::new("https://localhost:2963", "logs", true, None, Some(rt)).unwrap();
        assert_eq!(s.client_identity(), None, "CA-only carries no identity");
        assert!(!s.rotate_credentials().unwrap(), "nothing to rotate");
    }

    /// The URL is built once and carries the database, so a rotation cannot
    /// change where a batch is going.
    #[test]
    fn the_write_url_targets_the_configured_database() {
        let s = Shipper::new("http://localhost:1963/", "mydb", false, None, None).unwrap();
        assert_eq!(
            s.url.as_str(),
            "http://localhost:1963/api/v3/write_lp?db=mydb"
        );
    }

    // ---- the wrong-node / poisoned-pool behavior ----------------------
    //
    // These run against a real TCP listener rather than a mocked client,
    // because the property under test lives BELOW the status code: after
    // a 501 the next attempt must arrive on a NEW CONNECTION. The C4
    // wedge (FINDING_agent_pools_a_reused_ip.md in the TimeLakeDB repo)
    // was precisely a shipper whose statuses were all handled and whose
    // connection never changed.

    use std::io::Read as _;
    use std::sync::atomic::AtomicUsize;

    /// A scripted HTTP peer: answers the given statuses in order, over as
    /// many keep-alive connections as the client cares to open, and counts
    /// the connections. When the script runs dry it keeps answering the
    /// final status.
    fn scripted_peer(statuses: Vec<u16>) -> (String, Arc<AtomicUsize>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let conns = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&conns);
        let served = Arc::new(AtomicUsize::new(0));
        std::thread::spawn(move || {
            for sock in listener.incoming() {
                let Ok(mut sock) = sock else { return };
                seen.fetch_add(1, Ordering::Relaxed);
                let statuses = statuses.clone();
                let served = Arc::clone(&served);
                std::thread::spawn(move || {
                    let mut buf = vec![0u8; 65536];
                    let mut pending: Vec<u8> = Vec::new();
                    loop {
                        // Read until a full request (headers + body) is in.
                        let (mut header_end, mut content_len) = (None, 0usize);
                        loop {
                            if header_end.is_none()
                                && let Some(i) = pending.windows(4).position(|w| w == b"\r\n\r\n")
                            {
                                header_end = Some(i + 4);
                                let head = String::from_utf8_lossy(&pending[..i]).to_lowercase();
                                content_len = head
                                    .lines()
                                    .find_map(|l| l.strip_prefix("content-length:"))
                                    .and_then(|v| v.trim().parse().ok())
                                    .unwrap_or(0);
                            }
                            if let Some(h) = header_end
                                && pending.len() >= h + content_len
                            {
                                pending.drain(..h + content_len);
                                break;
                            }
                            match sock.read(&mut buf) {
                                Ok(0) | Err(_) => return,
                                Ok(n) => pending.extend_from_slice(&buf[..n]),
                            }
                        }
                        let i = served.fetch_add(1, Ordering::Relaxed);
                        let code = *statuses.get(i).or(statuses.last()).unwrap();
                        let resp = if code == 204 {
                            "HTTP/1.1 204 No Content\r\n\r\n".to_string()
                        } else {
                            let body = "no write path here";
                            format!(
                                "HTTP/1.1 {code} X\r\ncontent-length: {}\r\n\r\n{body}",
                                body.len()
                            )
                        };
                        if sock.write_all(resp.as_bytes()).is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (format!("http://{addr}"), conns)
    }

    /// The wedge, refused: a peer that answers every write with 501 must
    /// see a NEW connection per attempt, because each 501 drops the pool.
    /// Under the pre-fix behavior this test observes exactly one
    /// connection however many times the shipper retries — which is the
    /// C4 wedge in miniature, and how this test was verified red.
    #[tokio::test]
    async fn a_501_answering_peer_is_redialed_not_retried() {
        let (url, conns) = scripted_peer(vec![501, 501, 501]);
        let s = Shipper::new(&url, "logs", false, None, None).unwrap();
        for _ in 0..3 {
            match s.send("t v=1i 1").await {
                Err(ShipError::WrongNode(m)) => {
                    assert!(m.contains("501"), "got: {m}")
                }
                other => panic!("expected WrongNode, got {other:?}"),
            }
        }
        assert_eq!(
            conns.load(Ordering::Relaxed),
            3,
            "each 501 must drop the pool, so each attempt dials fresh"
        );
        assert_eq!(s.transport_rebuilds(), 3);
    }

    /// The backstop: an inheritor that answers something other than 501 —
    /// a 404-serving web app on the reused address, say — earns a rebuild
    /// on every third consecutive failure, without any status naming the
    /// problem.
    #[tokio::test]
    async fn consecutive_unrecognized_answers_shed_the_pool() {
        let (url, conns) = scripted_peer(vec![404; 6]);
        let s = Shipper::new(&url, "logs", false, None, None).unwrap();
        for _ in 0..6 {
            match s.send("t v=1i 1").await {
                Err(ShipError::Transport(_)) => {}
                other => panic!("expected Transport, got {other:?}"),
            }
        }
        assert_eq!(s.transport_rebuilds(), 2, "rebuild on the 3rd and 6th");
        // The first rebuild (after send 3) forces send 4 onto a fresh
        // dial: two connections. The second rebuild lands on the LAST
        // send, so its fresh dial is never taken — a third connection
        // would mean the pool was shed at the wrong time.
        assert_eq!(conns.load(Ordering::Relaxed), 2);
    }

    /// An answer from a node with a write path resets the streak: two
    /// failures, an acked write, two more failures — never three in a
    /// row, so the pool is never shed. Without the reset, intermittent
    /// flakes would churn perfectly good connections.
    #[tokio::test]
    async fn a_write_path_answer_resets_the_streak() {
        let (url, _conns) = scripted_peer(vec![404, 404, 204, 404, 404, 204]);
        let s = Shipper::new(&url, "logs", false, None, None).unwrap();
        for _ in 0..6 {
            let _ = s.send("t v=1i 1").await;
        }
        assert_eq!(
            s.transport_rebuilds(),
            0,
            "the streak never reached three consecutive failures"
        );
    }
}
