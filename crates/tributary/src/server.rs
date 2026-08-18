//! The telemetry listener (T-1): `GET /metrics` and `GET /healthz`.
//!
//! Built on hyper directly rather than a web framework. Two routes with no
//! path parameters, no extractors and no shared-state plumbing do not need
//! a router, and hyper, hyper-util, http-body-util and bytes are already
//! compiled into this binary by reqwest — so this endpoint costs **no new
//! dependencies at all**, which matters for an agent whose argument is that
//! it is small enough to run on every node.
//!
//! ## What `/healthz` means, and what it deliberately does not
//!
//! It reports **liveness**: is the main loop still turning? That is the one
//! question a restart can answer. It does NOT go red when TimeLakeDB is
//! unreachable, and that is the important design decision here.
//!
//! A shipper whose liveness probe fails on database trouble is a shipper
//! that gets killed by its orchestrator exactly when the database is
//! already struggling — and the restart discards the in-memory batch and
//! the in-flight ones, turning a recoverable outage into data loss (P1-7:
//! everything unacked lives only on this node). The queue exists precisely
//! so an outage is survivable without restarting; a probe that restarts
//! anyway defeats it.
//!
//! So an outage shows up as `degraded` in the body and in the metrics —
//! `tributary_queue_bytes` climbing, `tributary_lines_shipped_total` flat —
//! where an operator and an alert can see it, while the process is left
//! alone to do its job. Use the body's `shipping` field for a *readiness*
//! probe if you want traffic-shaping behaviour; do not wire it to liveness.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;

use crate::telemetry::Telemetry;

/// A tick older than this and the loop is considered wedged. Generous: the
/// loop sleeps 200 ms when idle and a flush can take seconds under load, so
/// this only trips on something genuinely stuck.
const WEDGED_AFTER_SECS: u64 = 60;

/// Spawn the listener. Returns immediately; the server runs until the
/// process exits.
///
/// A bind failure is returned rather than swallowed: an operator who asked
/// for telemetry and did not get it should be told at startup, not discover
/// it when a dashboard stays empty.
pub async fn serve(addr: SocketAddr, tel: Arc<Telemetry>) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("[telemetry] cannot bind {addr}: {e}"))?;
    tracing::info!(%addr, "telemetry listening on /metrics and /healthz");

    tokio::spawn(async move {
        loop {
            let (stream, _peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    // A single failed accept is not worth tearing the
                    // listener down; log and keep serving.
                    tracing::warn!(error = %e, "telemetry accept failed");
                    continue;
                }
            };
            let tel = Arc::clone(&tel);
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req| handle(req, Arc::clone(&tel)));
                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await
                {
                    tracing::debug!(error = %e, "telemetry connection ended");
                }
            });
        }
    });
    Ok(())
}

async fn handle(
    req: Request<hyper::body::Incoming>,
    tel: Arc<Telemetry>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let is_get = req.method() == Method::GET || req.method() == Method::HEAD;
    Ok(match (is_get, req.uri().path()) {
        (true, "/metrics") => text(
            StatusCode::OK,
            "text/plain; version=0.0.4; charset=utf-8",
            tel.render_prometheus(),
        ),
        (true, "/healthz") => {
            let (code, body) = health(&tel);
            text(code, "application/json", body)
        }
        (true, "/") => text(
            StatusCode::OK,
            "text/plain; charset=utf-8",
            "tributary\n/metrics\n/healthz\n".to_string(),
        ),
        (false, _) => text(
            StatusCode::METHOD_NOT_ALLOWED,
            "text/plain; charset=utf-8",
            "only GET\n".to_string(),
        ),
        _ => text(
            StatusCode::NOT_FOUND,
            "text/plain; charset=utf-8",
            "not found\n".to_string(),
        ),
    })
}

/// Liveness, plus enough state to be useful. See the module docs for why
/// an unreachable database does not make this fail.
pub fn health(tel: &Telemetry) -> (StatusCode, String) {
    let stalled_secs = tel.since_tick_ms() / 1000;
    let live = stalled_secs < WEDGED_AFTER_SECS;

    let queue_bytes = tel.queue_bytes.load(Ordering::Relaxed);
    let queue_full = tel.queue_full.load(Ordering::Relaxed);
    let unauthorized = tel.ship.unauthorized.load(Ordering::Relaxed);
    let cert_healthy = tel.cert_healthy.load(Ordering::Relaxed);

    // `shipping` is the readiness-shaped signal: it says whether data is
    // actually moving, which is what an operator wants on a dashboard —
    // while `status` stays green so nothing restarts the process.
    let shipping = queue_bytes == 0 && !queue_full && unauthorized == 0;

    let status = if !live {
        "wedged"
    } else if shipping && cert_healthy {
        "ok"
    } else {
        "degraded"
    };

    let body = format!(
        concat!(
            "{{\"status\":\"{}\",",
            "\"live\":{},",
            "\"shipping\":{},",
            "\"uptime_seconds\":{},",
            "\"last_tick_age_seconds\":{},",
            "\"queue_bytes\":{},",
            "\"queue_full\":{},",
            "\"at_risk_lines\":{},",
            "\"unauthorized_total\":{},",
            "\"credential_healthy\":{},",
            "\"credential_expiry_seconds\":{}}}\n"
        ),
        status,
        live,
        shipping,
        tel.uptime_secs(),
        stalled_secs,
        queue_bytes,
        queue_full,
        tel.at_risk_lines(),
        unauthorized,
        cert_healthy,
        tel.cert_expires_in_secs.load(Ordering::Relaxed),
    );

    // Only wedged is a failure. Degraded is deliberately 200: it means the
    // agent is alive and holding data it has not been able to deliver,
    // which is the queue working, not the agent failing.
    let code = if live {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, body)
}

fn text(code: StatusCode, content_type: &str, body: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(code)
        .header("content-type", content_type)
        .header("cache-control", "no-store")
        .body(Full::new(Bytes::from(body)))
        .expect("static response builds")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ship::Counters;

    fn tel() -> Arc<Telemetry> {
        Telemetry::new(Arc::new(Counters::default()))
    }

    #[test]
    fn a_fresh_agent_is_ok() {
        let t = tel();
        let (code, body) = health(&t);
        assert_eq!(code, StatusCode::OK);
        assert!(body.contains("\"status\":\"ok\""), "{body}");
        assert!(body.contains("\"live\":true"));
    }

    /// The trap this endpoint exists to avoid: a database outage must not
    /// make liveness fail, or the orchestrator restarts the agent and
    /// discards exactly the data the queue was protecting.
    #[test]
    fn a_database_outage_is_degraded_but_still_live() {
        let t = tel();
        t.queue_bytes.store(64 * 1024 * 1024, Ordering::Relaxed);
        t.queue_full.store(true, Ordering::Relaxed);

        let (code, body) = health(&t);
        assert_eq!(
            code,
            StatusCode::OK,
            "an outage must NOT fail liveness — a restart would lose the queue"
        );
        assert!(body.contains("\"status\":\"degraded\""), "{body}");
        assert!(body.contains("\"live\":true"));
        assert!(body.contains("\"shipping\":false"));
    }

    /// A bad token is also not a liveness failure — restarting will not
    /// mint a correct one, and the data is still spooled.
    #[test]
    fn a_rejected_token_is_degraded_not_dead() {
        let t = tel();
        t.ship.unauthorized.store(3, Ordering::Relaxed);
        let (code, body) = health(&t);
        assert_eq!(code, StatusCode::OK);
        assert!(body.contains("\"status\":\"degraded\""), "{body}");
        assert!(body.contains("\"unauthorized_total\":3"));
    }

    #[test]
    fn a_refused_certificate_renewal_shows_as_degraded() {
        let t = tel();
        t.cert_healthy.store(false, Ordering::Relaxed);
        let (_, body) = health(&t);
        assert!(body.contains("\"status\":\"degraded\""), "{body}");
        assert!(body.contains("\"credential_healthy\":false"));
    }

    #[test]
    fn a_wedged_loop_is_the_one_thing_that_fails() {
        let t = tel();
        // Backdate the last tick past the threshold.
        let old = t.last_tick_ms.load(Ordering::Relaxed) - (WEDGED_AFTER_SECS + 5) * 1000;
        t.last_tick_ms.store(old, Ordering::Relaxed);

        let (code, body) = health(&t);
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("\"status\":\"wedged\""), "{body}");
        assert!(body.contains("\"live\":false"));
    }

    #[test]
    fn the_health_body_is_valid_json() {
        let t = tel();
        t.queue_bytes.store(17, Ordering::Relaxed);
        let (_, body) = health(&t);
        let v: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        assert_eq!(v["queue_bytes"], 17);
        assert_eq!(v["status"], "degraded");
    }
}
