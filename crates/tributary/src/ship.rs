//! Shipping a batch to TimeLakeDB.
//!
//! Response handling follows the documented contract: 204 means
//! WAL-durable, 400 means a line was rejected and **nothing in the batch
//! was written**, 429 is explicit backpressure carrying `Retry-After`.

use std::io::Write as _;
use std::time::Duration;

pub struct Shipper {
    client: reqwest::Client,
    url: String,
    gzip: bool,
    pub shipped: u64,
    pub rejected: u64,
}

#[derive(Debug)]
pub enum ShipError {
    /// The server rejected a line; the batch wrote nothing. The caller
    /// bisects (L2) — for now the batch is reported and quarantined.
    Rejected(String),
    Backpressure(Duration),
    Transport(String),
}

impl std::fmt::Display for ShipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShipError::Rejected(m) => write!(f, "batch rejected: {m}"),
            ShipError::Backpressure(d) => write!(f, "backpressure, retry in {d:?}"),
            ShipError::Transport(m) => write!(f, "transport: {m}"),
        }
    }
}

impl Shipper {
    pub fn new(base_url: &str, database: &str, gzip: bool) -> anyhow::Result<Shipper> {
        Ok(Shipper {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
            url: format!(
                "{}/api/v3/write_lp?db={}",
                base_url.trim_end_matches('/'),
                database
            ),
            gzip,
            shipped: 0,
            rejected: 0,
        })
    }

    pub async fn send(&mut self, body: &str) -> Result<(), ShipError> {
        if body.is_empty() {
            return Ok(());
        }
        let mut req = self.client.post(&self.url);
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

        match res.status().as_u16() {
            204 => {
                self.shipped += body.lines().count() as u64;
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
                self.rejected += 1;
                let msg = res.text().await.unwrap_or_default();
                Err(ShipError::Rejected(msg))
            }
            code => {
                let msg = res.text().await.unwrap_or_default();
                Err(ShipError::Transport(format!("HTTP {code}: {msg}")))
            }
        }
    }
}
