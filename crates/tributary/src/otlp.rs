//! OTLP logs receiver (#12).
//!
//! Flips Tributary from pull-only to a push target: an OpenTelemetry
//! producer or Collector `POST`s an `ExportLogsServiceRequest` and each
//! `LogRecord` becomes a record on the same map → queue → ship path a file
//! tail uses.
//!
//! The four-level OTLP nesting (request → resourceLogs → scopeLogs →
//! logRecords) is flattened to one key→value map per record — the log body
//! under `body`, `severity_text`/`severity_number`, the scope name, and every
//! resource / scope / log **attribute** by its own key — and handed to the
//! shared [`crate::map::build_record`]. That is the whole cardinality defence:
//! attributes do not become tags by arriving, they become tags only if the
//! source's allowlist NAMES them (FR-2). Resource attributes especially
//! (`k8s.pod.uid`, `host.id`, a trace id per record) are unbounded; the
//! allowlist is what stops them rebuilding the failure this project exists to
//! avoid.
//!
//! The messages are hand-written prost (a stable subset of the OTLP protos, so
//! no `protoc`); OTLP metrics/traces are the same shape later.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;

use crate::config::{Otlp, Source};
use crate::lp::Record;
use crate::map::{MapError, build_record};
use crate::queue::Queue;
use crate::ship::Shipper;
use crate::stamp::{Resolution, Stamper};
use crate::telemetry::Telemetry;

/// `opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest`.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExportLogsServiceRequest {
    #[prost(message, repeated, tag = "1")]
    pub resource_logs: Vec<ResourceLogs>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceLogs {
    #[prost(message, optional, tag = "1")]
    pub resource: Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    pub scope_logs: Vec<ScopeLogs>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Resource {
    #[prost(message, repeated, tag = "1")]
    pub attributes: Vec<KeyValue>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScopeLogs {
    #[prost(message, optional, tag = "1")]
    pub scope: Option<InstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    pub log_records: Vec<LogRecord>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstrumentationScope {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub version: String,
    #[prost(message, repeated, tag = "3")]
    pub attributes: Vec<KeyValue>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogRecord {
    #[prost(fixed64, tag = "1")]
    pub time_unix_nano: u64,
    #[prost(int32, tag = "2")]
    pub severity_number: i32,
    #[prost(string, tag = "3")]
    pub severity_text: String,
    #[prost(message, optional, tag = "5")]
    pub body: Option<AnyValue>,
    #[prost(message, repeated, tag = "6")]
    pub attributes: Vec<KeyValue>,
    #[prost(fixed64, tag = "11")]
    pub observed_time_unix_nano: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyValue {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(message, optional, tag = "2")]
    pub value: Option<AnyValue>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyValue {
    #[prost(oneof = "any_value::Value", tags = "1, 2, 3, 4, 5, 6, 7")]
    pub value: Option<any_value::Value>,
}

pub mod any_value {
    // The variant names are OTLP's own (string_value, bool_value, ...);
    // they don't touch the wire (the prost tag does), so keep them faithful.
    #[allow(clippy::enum_variant_names)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(string, tag = "1")]
        StringValue(String),
        #[prost(bool, tag = "2")]
        BoolValue(bool),
        #[prost(int64, tag = "3")]
        IntValue(i64),
        #[prost(double, tag = "4")]
        DoubleValue(f64),
        #[prost(message, tag = "5")]
        ArrayValue(super::ArrayValue),
        #[prost(message, tag = "6")]
        KvlistValue(super::KeyValueList),
        #[prost(bytes = "vec", tag = "7")]
        BytesValue(Vec<u8>),
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrayValue {
    #[prost(message, repeated, tag = "1")]
    pub values: Vec<AnyValue>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyValueList {
    #[prost(message, repeated, tag = "1")]
    pub values: Vec<KeyValue>,
}

/// Render an OTLP `AnyValue` as JSON, so the shared field coercion (declared
/// type wins) and tag stringification treat it exactly like a parsed line.
fn any_to_json(v: &AnyValue) -> serde_json::Value {
    use any_value::Value as V;
    use serde_json::Value as J;
    match &v.value {
        None => J::Null,
        Some(V::StringValue(s)) => J::String(s.clone()),
        Some(V::BoolValue(b)) => J::Bool(*b),
        Some(V::IntValue(i)) => J::Number((*i).into()),
        Some(V::DoubleValue(d)) => serde_json::Number::from_f64(*d).map_or(J::Null, J::Number),
        // Bytes are not a log value we tag or field on; render hex so nothing panics.
        Some(V::BytesValue(b)) => J::String(b.iter().map(|x| format!("{x:02x}")).collect()),
        Some(V::ArrayValue(a)) => J::Array(a.values.iter().map(any_to_json).collect()),
        Some(V::KvlistValue(kv)) => J::Object(
            kv.values
                .iter()
                .map(|e| (e.key.clone(), e.value.as_ref().map_or(J::Null, any_to_json)))
                .collect(),
        ),
    }
}

/// The timestamp OTLP means: the event time, then the observed time, then
/// "stamp on read" when a producer sent neither.
fn record_ts_ns(lr: &LogRecord) -> i64 {
    if lr.time_unix_nano != 0 {
        lr.time_unix_nano as i64
    } else if lr.observed_time_unix_nano != 0 {
        lr.observed_time_unix_nano as i64
    } else {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }
}

/// Flatten one log record — with its resource and scope context — to the
/// key→value map [`build_record`] consumes. Resource attributes are laid
/// down first, then scope, then the log's own, so the most specific wins on
/// a key collision, the way OTLP semantics intend.
fn flatten(
    resource: Option<&Resource>,
    scope: Option<&InstrumentationScope>,
    lr: &LogRecord,
) -> BTreeMap<String, serde_json::Value> {
    let mut m: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    let put_attrs = |attrs: &[KeyValue], m: &mut BTreeMap<String, serde_json::Value>| {
        for kv in attrs {
            if let Some(v) = &kv.value {
                m.insert(kv.key.clone(), any_to_json(v));
            }
        }
    };
    if let Some(r) = resource {
        put_attrs(&r.attributes, &mut m);
    }
    if let Some(s) = scope {
        if !s.name.is_empty() {
            m.insert(
                "scope.name".into(),
                serde_json::Value::String(s.name.clone()),
            );
        }
        put_attrs(&s.attributes, &mut m);
    }
    put_attrs(&lr.attributes, &mut m);

    if let Some(body) = &lr.body {
        m.insert("body".into(), any_to_json(body));
    }
    if !lr.severity_text.is_empty() {
        m.insert(
            "severity_text".into(),
            serde_json::Value::String(lr.severity_text.clone()),
        );
    }
    if lr.severity_number != 0 {
        m.insert(
            "severity_number".into(),
            serde_json::Value::Number(lr.severity_number.into()),
        );
    }
    m
}

/// Map a whole `ExportLogsServiceRequest` to records, one `Result` per log so
/// a single un-coercible record is quarantined rather than failing the batch.
pub fn map_export_request(
    src: &Source,
    req: &ExportLogsServiceRequest,
) -> Vec<Result<(Record, i64), MapError>> {
    let mut out = Vec::new();
    for rl in &req.resource_logs {
        for sl in &rl.scope_logs {
            for lr in &sl.log_records {
                let flat = flatten(rl.resource.as_ref(), sl.scope.as_ref(), lr);
                out.push(build_record(src, &flat, record_ts_ns(lr)));
            }
        }
    }
    out
}

/// Shared state for the receiver's connection handlers.
struct Recv {
    source: Source,
    queue: Arc<Mutex<Queue>>,
    stamper: Arc<Mutex<Stamper>>,
    tel: Arc<Telemetry>,
}

/// Run the OTLP receiver until shutdown: a durable queue, a drain task that
/// ships it, and an HTTP listener that accepts `POST /v1/logs`. An independent
/// pipeline — its own queue, shipper and stamper — so the file-tail loop is
/// untouched.
pub async fn run(
    cfg: Otlp,
    state_dir: PathBuf,
    queue_max_bytes: u64,
    tel: Arc<Telemetry>,
    shipper: Shipper,
) -> anyhow::Result<()> {
    let addr: SocketAddr = cfg.listen.parse().map_err(|_| {
        anyhow::anyhow!("[otlp].listen {:?} is not a host:port address", cfg.listen)
    })?;
    let queue = Arc::new(Mutex::new(Queue::open(
        &state_dir.join("otlp-queue"),
        queue_max_bytes,
    )?));
    // OTLP `time_unix_nano` is nanosecond; the stamper only disambiguates
    // records that land on the same tick.
    let stamper = Arc::new(Mutex::new(Stamper::new(
        Resolution::parse("ns").expect("ns is a valid resolution"),
    )));
    let recv = Arc::new(Recv {
        source: cfg.as_source(),
        queue: Arc::clone(&queue),
        stamper,
        tel,
    });

    // Ship queued batches FIFO, retrying transport failures. The queue is
    // durable, so nothing acknowledged is lost across a crash.
    tokio::spawn(drain(Arc::clone(&queue), shipper));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("[otlp] cannot bind {addr}: {e}"))?;
    tracing::info!(%addr, table = %recv.source.table, "OTLP receiver on POST /v1/logs");

    tokio::spawn(async move {
        loop {
            let (stream, _peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "otlp accept failed");
                    continue;
                }
            };
            let recv = Arc::clone(&recv);
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req| handle(req, Arc::clone(&recv)));
                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await
                {
                    tracing::debug!(error = %e, "otlp connection ended");
                }
            });
        }
    });

    shutdown_signal().await;
    tracing::info!("OTLP receiver shutting down");
    Ok(())
}

async fn drain(queue: Arc<Mutex<Queue>>, shipper: Shipper) {
    loop {
        let front = queue.lock().expect("otlp queue lock").front();
        match front {
            None => tokio::time::sleep(Duration::from_millis(200)).await,
            Some(path) => {
                let body = match Queue::read(&path) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::error!(error = %e, "otlp queue segment unreadable");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                let lines: Vec<String> = body.split_inclusive('\n').map(str::to_string).collect();
                match shipper.send_lines(&lines).await {
                    Ok(_poison) => {
                        let _ = queue.lock().expect("otlp queue lock").pop(&path);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "otlp batch not shipped; retrying");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
}

async fn handle(
    req: Request<hyper::body::Incoming>,
    recv: Arc<Recv>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if req.method() != Method::POST || req.uri().path() != "/v1/logs" {
        return Ok(text(
            StatusCode::NOT_FOUND,
            "not found; POST OTLP logs to /v1/logs\n",
        ));
    }
    let body = match req.into_body().collect().await {
        Ok(b) => b.to_bytes(),
        Err(e) => {
            return Ok(text(
                StatusCode::BAD_REQUEST,
                &format!("body read failed: {e}\n"),
            ));
        }
    };
    Ok(ingest(&recv, body.as_ref()))
}

/// Decode one request body, map + stamp + encode its records, and enqueue
/// them DURABLY before returning the ack. Split from the hyper plumbing so
/// the ingest path is tested without constructing an `Incoming` body.
fn ingest(recv: &Recv, body: &[u8]) -> Response<Full<Bytes>> {
    let request = match <ExportLogsServiceRequest as ::prost::Message>::decode(body) {
        Ok(r) => r,
        Err(e) => {
            return text(
                StatusCode::BAD_REQUEST,
                &format!("not a valid OTLP ExportLogsServiceRequest: {e}\n"),
            );
        }
    };

    // Map + stamp + encode. A record that cannot be coerced or stamped is
    // dropped and counted, never shipped — the batch stays atomic.
    let mut lines = String::new();
    let mut accepted = 0u64;
    let mut rejected = 0u64;
    {
        let mut stamper = recv.stamper.lock().expect("otlp stamper lock");
        for result in map_export_request(&recv.source, &request) {
            match result {
                Ok((mut record, source_ts)) => match stamper.stamp(source_ts) {
                    Ok(ts) => {
                        record.ts_ns = ts;
                        if record.encode(&mut lines).is_ok() {
                            accepted += 1;
                        } else {
                            rejected += 1;
                        }
                    }
                    Err(_) => rejected += 1,
                },
                Err(_) => rejected += 1,
            }
        }
    }
    recv.tel
        .otlp_received
        .fetch_add(accepted, Ordering::Relaxed);
    recv.tel
        .otlp_rejected
        .fetch_add(rejected, Ordering::Relaxed);

    if accepted == 0 {
        // Well-formed but nothing to store (e.g. every record a bad type):
        // a successful no-op, not an error.
        return proto_ok();
    }

    // THE ACK TRAP (#12): durable BEFORE the response. `push` fsyncs, so a
    // 200 means the records survive a restart; acking first would let a
    // Collector believe it delivered something a crash then dropped.
    match recv.queue.lock().expect("otlp queue lock").push(&lines) {
        Ok(true) => proto_ok(),
        Ok(false) => text(StatusCode::SERVICE_UNAVAILABLE, "queue full; retry\n"),
        Err(e) => text(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("could not queue: {e}\n"),
        ),
    }
}

/// An empty `ExportLogsServiceResponse` (zero bytes) is a full-success ack.
fn proto_ok() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-protobuf")
        .body(Full::new(Bytes::new()))
        .expect("static response")
}

fn text(code: StatusCode, msg: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(code)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from(msg.to_string())))
        .expect("static response")
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FieldType, Parser, Timestamp};
    use crate::lp::Value;

    fn otlp_source() -> Source {
        Source {
            name: "otlp".into(),
            path: String::new(),
            table: "logs".into(),
            parser: Parser::Plain,
            timestamp: Timestamp {
                field: None,
                format: "unix_ms".into(),
                resolution: "ms".into(),
            },
            // Only these become tags — k8s.pod.uid below must NOT.
            tags: vec!["host.name".into(), "level".into()],
            tags_static: Default::default(),
            fields: [("body".to_string(), FieldType::String)].into(),
            visibility: None,
            multiline: None,
            filter: Vec::new(),
            sample: Vec::new(),
            redact: Vec::new(),
            kubernetes: None,
        }
    }

    fn kv(key: &str, s: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(s.into())),
            }),
        }
    }

    fn str_val(s: &str) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::StringValue(s.into())),
        }
    }

    fn request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("host.name", "node1"), kv("service.name", "api")],
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        severity_number: 9,
                        severity_text: "WARN".into(),
                        body: Some(str_val("disk 91% full")),
                        attributes: vec![
                            kv("level", "warn"),
                            // The cardinality trap: unbounded, must be dropped.
                            kv("k8s.pod.uid", "a1b2c3-d4e5"),
                        ],
                        observed_time_unix_nano: 0,
                    }],
                }],
            }],
        }
    }

    #[test]
    fn a_log_record_maps_body_to_a_field_and_allowlisted_attributes_to_tags() {
        let src = otlp_source();
        let results = map_export_request(&src, &request());
        assert_eq!(results.len(), 1);
        let (rec, ts) = results.into_iter().next().unwrap().unwrap();

        assert_eq!(rec.table, "logs");
        // host.name (resource) and level (log) are allowlisted; stream is the
        // identity. service.name and k8s.pod.uid are NOT named, so absent.
        let tag_keys: Vec<&str> = rec.tags.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(tag_keys, vec!["host.name", "level", "stream"]);
        assert_eq!(
            rec.tags.iter().find(|(k, _)| k == "host.name").unwrap().1,
            "node1"
        );
        assert_eq!(
            rec.fields,
            vec![("body".to_string(), Value::Str("disk 91% full".into()))]
        );
        assert_eq!(ts, 1_700_000_000_000_000_000);
    }

    #[test]
    fn an_unbounded_resource_attribute_never_becomes_a_tag_by_arriving() {
        // The whole point of the allowlist (FR-2): a trace id per record must
        // not explode the primary key just because OTLP carried it.
        let mut req = request();
        req.resource_logs[0].scope_logs[0].log_records[0]
            .attributes
            .push(kv("trace_id", "0af7651916cd43dd8448eb211c80319c"));
        let src = otlp_source();
        let (rec, _) = map_export_request(&src, &req).pop().unwrap().unwrap();
        assert!(
            rec.tags.iter().all(|(k, _)| k != "trace_id"),
            "an un-allowlisted attribute must not become a tag"
        );
    }

    fn recv_for_test() -> (Recv, Arc<Mutex<Queue>>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let queue = Arc::new(Mutex::new(
            Queue::open(&dir.path().join("q"), 1 << 20).unwrap(),
        ));
        let stamper = Arc::new(Mutex::new(Stamper::new(Resolution::parse("ns").unwrap())));
        // A shipper only to mint a Counters for Telemetry; the receiver path
        // under test never ships.
        let shipper =
            crate::ship::Shipper::new("http://127.0.0.1:1", "logs", false, None, None).unwrap();
        let tel = crate::telemetry::Telemetry::new(shipper.counters.clone());
        let recv = Recv {
            source: otlp_source(),
            queue: Arc::clone(&queue),
            stamper,
            tel,
        };
        (recv, queue, dir)
    }

    #[test]
    fn ingest_queues_the_expected_line_protocol_and_acks() {
        let (recv, queue, _dir) = recv_for_test();
        let bytes = ::prost::Message::encode_to_vec(&request());

        let resp = ingest(&recv, &bytes);
        assert_eq!(resp.status(), StatusCode::OK, "a good batch acks 200");

        // The record is DURABLE before the ack: it is already on disk.
        let seg = queue.lock().unwrap().front().expect("a segment was queued");
        let lp = Queue::read(&seg).unwrap();
        assert!(lp.starts_with("logs,"), "measurement is the table: {lp}");
        assert!(lp.contains("host.name=node1"), "allowlisted tag: {lp}");
        assert!(lp.contains("level=warn"), "allowlisted tag: {lp}");
        assert!(lp.contains("stream=otlp"), "stream identity: {lp}");
        assert!(lp.contains("body=\"disk 91% full\""), "body field: {lp}");
        assert!(
            !lp.contains("k8s.pod.uid"),
            "un-allowlisted attr must not appear: {lp}"
        );
        assert_eq!(recv.tel.otlp_received.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_body_that_is_not_a_valid_request_is_a_400() {
        let (recv, queue, _dir) = recv_for_test();
        // Wire type 7 in the first tag: not a decodable protobuf message.
        let resp = ingest(&recv, &[0x0f]);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(
            queue.lock().unwrap().is_empty(),
            "nothing is queued from a bad body"
        );
    }

    #[test]
    fn the_wire_round_trips_protobuf() {
        // Prove the codec, not just the mapping: encode as a producer would,
        // decode, and map.
        let bytes = ::prost::Message::encode_to_vec(&request());
        let decoded =
            <ExportLogsServiceRequest as ::prost::Message>::decode(bytes.as_slice()).unwrap();
        let (rec, ts) = map_export_request(&otlp_source(), &decoded)
            .pop()
            .unwrap()
            .unwrap();
        assert_eq!(rec.table, "logs");
        assert_eq!(ts, 1_700_000_000_000_000_000);
        assert_eq!(rec.fields[0].1, Value::Str("disk 91% full".into()));
    }
}
