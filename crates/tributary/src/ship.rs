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
}

#[derive(Clone)]
pub struct Shipper {
    client: reqwest::Client,
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
            ShipError::Transport(m) => write!(f, "transport: {m}"),
        }
    }
}

impl Shipper {
    pub fn new(
        base_url: &str,
        database: &str,
        gzip: bool,
        token: Option<crate::auth::Secret>,
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
        Ok(Shipper {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                // Enough idle connections for the in-flight pipeline.
                .pool_max_idle_per_host(16)
                .build()?,
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
        let mut req = self.client.post(self.url.as_str());
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

        let res = req
            .header("content-type", "text/plain; charset=utf-8")
            .body(bytes)
            .send()
            .await
            .map_err(|e| ShipError::Transport(e.to_string()))?;

        let status = res.status().as_u16();
        let out = match status {
            204 => {
                self.counters
                    .shipped
                    .fetch_add(body.lines().count() as u64, Ordering::Relaxed);
                Ok(())
            }
            429 => {
                let secs = res
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(1);
                Err(ShipError::Backpressure(Duration::from_secs(secs)))
            }
            400 => {
                self.counters.rejected.fetch_add(1, Ordering::Relaxed);
                Err(ShipError::Rejected(res.text().await.unwrap_or_default()))
            }
            401 | 403 => {
                // The server's body carries a code/reason but never the
                // token, so it is safe to surface.
                self.counters.unauthorized.fetch_add(1, Ordering::Relaxed);
                Err(ShipError::Unauthorized(format!(
                    "HTTP {status}: {}",
                    res.text().await.unwrap_or_default()
                )))
            }
            code => Err(ShipError::Transport(format!(
                "HTTP {code}: {}",
                res.text().await.unwrap_or_default()
            ))),
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
}
