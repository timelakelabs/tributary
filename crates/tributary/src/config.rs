//! Configuration. Every field here exists because of a property of
//! TimeLakeDB's write contract (DESIGN.md §1), which is why the safe
//! choices are the defaults and the dangerous ones must be spelled out.

use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub output: Output,
    #[serde(rename = "source", default)]
    pub sources: Vec<Source>,
    /// T-1 self-telemetry. Absent = no listener.
    #[serde(default)]
    pub telemetry: Option<Telemetry>,
    /// The agent's own log file. Absent = stdout only.
    #[serde(default)]
    pub log: Option<Log>,
    /// OTLP logs receiver (#12). Absent = no receiver, exactly the pull-only
    /// agent as before. Present = a push endpoint on its own port.
    #[serde(default)]
    pub otlp: Option<Otlp>,
    /// Host-metrics collector (#25). Absent = no metrics, a log-only agent
    /// exactly as before. Present = Telegraf-shaped cpu/mem/disk/net/system/
    /// swap sampled on an interval and shipped like any other data.
    #[serde(default)]
    pub metrics: Option<Metrics>,
}

#[derive(Debug, Deserialize)]
pub struct Output {
    pub url: String,
    #[serde(default = "default_db")]
    pub database: String,
    #[serde(default = "default_batch_lines")]
    pub batch_lines: usize,
    #[serde(default = "default_true")]
    pub gzip: bool,

    /// Path to a file holding the data-plane token (SEC-4). For a
    /// Kubernetes secret mount or a systemd credential; the
    /// `TRIBUTARY_TOKEN` environment variable takes precedence over it.
    /// **There is deliberately no inline token field** — a secret in a
    /// committed config is a secret leaked (see `auth::resolve_token`).
    #[serde(default)]
    pub token_file: Option<std::path::PathBuf>,

    /// Spool cap. Reaching it pauses reading and alarms - it never drops.
    #[serde(default = "default_queue_bytes")]
    pub queue_max_bytes: u64,

    /// Batches in flight at once. Bounded on purpose: unbounded
    /// concurrency only moves the queue into memory and hides it.
    #[serde(default = "default_max_inflight")]
    pub max_inflight: usize,

    /// Bounds on the OBSERVED lateness allowance (ROADMAP section 2.2):
    /// the floor stops a perfectly ordered stream producing a brittle
    /// watermark, the ceiling stops a pathological one stalling it.
    #[serde(default = "default_wm_floor_ms")]
    pub watermark_floor_ms: u64,
    #[serde(default = "default_wm_ceiling_ms")]
    pub watermark_ceiling_ms: u64,
    #[serde(default = "default_wm_table")]
    pub watermark_table: String,
    #[serde(default = "default_wm_every")]
    pub watermark_every_secs: u64,

    /// Transport security (L4). Absent means plain HTTP or the public
    /// trust store, exactly as before — this is additive.
    #[serde(default)]
    pub tls: Option<Tls>,

    /// How often to log the "at risk if this node is lost now" line (P1-7).
    /// This is the deployment's live RPO: everything the server has not
    /// acked lives only on this node. `0` turns the line off.
    #[serde(default = "default_rpo_report_secs")]
    pub rpo_report_secs: u64,
}

fn default_rpo_report_secs() -> u64 {
    60
}

/// The agent's OWN diagnostic log. Absent means stdout only, exactly as
/// before — right under systemd or Docker, where stdout is captured and
/// rotated for you. Set it for a bare-process deployment, where stdout
/// redirected to a file otherwise grows until the disk fills.
///
/// This sink owns the file: do not also point logrotate at the same path.
#[derive(Debug, Deserialize)]
pub struct Log {
    pub file: std::path::PathBuf,
    /// Rotate once the live file passes this. `"100MiB"`, `"512KB"`, or a
    /// bare byte count. Note `KiB` (1024) and `KB` (1000) are both accepted
    /// and are different numbers.
    #[serde(default)]
    pub rotate_size: Option<String>,
    /// Rotate this long after the current file was opened: `"1d"`, `"12h"`,
    /// `"30m"`. Elapsed since open, not aligned to midnight.
    #[serde(default)]
    pub rotate_every: Option<String>,
    /// Rotated files to keep. Omit to keep everything, which is the safe
    /// default for anything someone may need after an incident.
    #[serde(default)]
    pub keep: Option<usize>,
}

impl Log {
    /// Parse the human-readable fields, refusing junk at startup rather
    /// than silently never rotating.
    pub fn parsed(
        &self,
    ) -> anyhow::Result<(Option<u64>, Option<std::time::Duration>, Option<usize>)> {
        let size = match &self.rotate_size {
            Some(s) => Some(crate::logfile::parse_size(s).ok_or_else(|| {
                anyhow::anyhow!("[log].rotate_size {s:?} is not a size like 100MiB")
            })?),
            None => None,
        };
        let every = match &self.rotate_every {
            Some(s) => Some(crate::logfile::parse_duration(s).ok_or_else(|| {
                anyhow::anyhow!("[log].rotate_every {s:?} is not a duration like 1d")
            })?),
            None => None,
        };
        if size.is_none() && every.is_none() {
            anyhow::bail!(
                "[log] sets neither rotate_size nor rotate_every — the file would grow \
                 without bound, which is the thing this section exists to prevent"
            );
        }
        Ok((size, every, self.keep))
    }
}

/// Self-telemetry (T-1). Absent means no listener at all — the same
/// additive posture as `[output.tls]`: an agent that never configured this
/// behaves exactly as it did before the endpoint existed, and no port is
/// opened that the operator did not ask for.
#[derive(Debug, Deserialize)]
pub struct Telemetry {
    /// Where to serve `GET /metrics` and `GET /healthz`.
    ///
    /// `127.0.0.1:9109` is the safe starting point. A Prometheus running
    /// elsewhere — a DaemonSet being scraped across the pod network — needs
    /// `0.0.0.0:9109`, and that is a deliberate choice rather than a
    /// default, because the endpoint carries no authentication and reports
    /// file paths and volumes.
    pub addr: String,
}

/// TLS for the connection to TimeLakeDB (L4).
///
/// The two halves are independent on purpose, because they answer
/// different questions and deployments genuinely need them apart:
///
/// * `ca_file` — whose server certificate do we trust? Needed whenever the
///   server presents a private CA's certificate. Telegraf's shipped TLS
///   config does exactly this and nothing more.
/// * `cert_file` + `key_file` — who are we? The client certificate the
///   server verifies in want mode. Both or neither; one alone is a
///   configuration mistake worth refusing at startup rather than
///   discovering as a handshake failure.
#[derive(Debug, Deserialize)]
pub struct Tls {
    /// PEM CA bundle used to verify the server. A bundle, not one
    /// certificate: dual-CA overlap is how the server rotates its trust
    /// anchors, and a client that accepts only one breaks mid-rotation.
    #[serde(default)]
    pub ca_file: Option<std::path::PathBuf>,

    /// The client certificate chain, PEM.
    #[serde(default)]
    pub cert_file: Option<std::path::PathBuf>,

    /// Its private key, PEM. Never inline, for the same reason there is no
    /// inline token field.
    #[serde(default)]
    pub key_file: Option<std::path::PathBuf>,

    /// How often to check whether the certificate on disk changed. SEC-3
    /// assumes ~24 h certificates, so a renewal lands while the agent is
    /// shipping and has to be picked up without a restart.
    #[serde(default = "default_cert_refresh_secs")]
    pub refresh_secs: u64,
}

fn default_cert_refresh_secs() -> u64 {
    30
}

impl Tls {
    /// Both-or-neither, checked at load so a half-configured identity fails
    /// with a sentence instead of a TLS error 20 minutes later.
    pub fn validate(&self) -> anyhow::Result<()> {
        match (&self.cert_file, &self.key_file) {
            (Some(_), None) => anyhow::bail!(
                "[output.tls] has cert_file but no key_file — a client certificate needs both"
            ),
            (None, Some(_)) => anyhow::bail!(
                "[output.tls] has key_file but no cert_file — a client certificate needs both"
            ),
            _ => Ok(()),
        }
    }

    /// Whether a client identity is configured at all.
    pub fn has_identity(&self) -> bool {
        self.cert_file.is_some() && self.key_file.is_some()
    }
}

fn default_max_inflight() -> usize {
    4
}
fn default_queue_bytes() -> u64 {
    2 << 30
}
fn default_wm_floor_ms() -> u64 {
    100
}
fn default_wm_ceiling_ms() -> u64 {
    60_000
}
fn default_wm_table() -> String {
    "tributary_watermarks".into()
}
fn default_wm_every() -> u64 {
    10
}

fn default_db() -> String {
    "logs".into()
}
fn default_batch_lines() -> usize {
    5_000
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct Source {
    /// Stream identity. Becomes the `stream` tag, and scopes the
    /// timestamp sequence (DESIGN.md §3.1).
    pub name: String,
    /// Filesystem path to tail. Empty/omitted for a journald source, which
    /// reads the journal, not a file.
    #[serde(default)]
    pub path: String,
    pub table: String,
    #[serde(default)]
    pub parser: Parser,
    #[serde(default)]
    pub timestamp: Timestamp,

    /// An ALLOWLIST. Never "promote every parsed key" — tags are in the
    /// primary key, so an accidental high-cardinality tag changes what
    /// "duplicate" means (DESIGN.md §2).
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub tags_static: BTreeMap<String, String>,

    /// Declared types. The database fixes a field's type on first write,
    /// permanently, so ingestion order must not be what chooses it
    /// (DESIGN.md §1.3).
    #[serde(default)]
    pub fields: BTreeMap<String, FieldType>,

    /// SEC-2 row visibility label, attached as the `_visibility` tag.
    #[serde(default)]
    pub visibility: Option<String>,

    /// Join continuation lines into one record (stack traces).
    #[serde(default)]
    pub multiline: Option<Multiline>,
}

#[derive(Debug, Deserialize)]
pub struct Multiline {
    /// A line matching this begins a record; anything else continues the
    /// one above it.
    pub starts_with: String,
    /// Bounds, so an unterminated record cannot pin memory.
    #[serde(default = "default_ml_lines")]
    pub max_lines: usize,
    #[serde(default = "default_ml_bytes")]
    pub max_bytes: usize,
    /// Emits the last record in a quiet file, which has no successor.
    #[serde(default = "default_ml_timeout")]
    pub timeout_ms: u64,
}

fn default_ml_lines() -> usize {
    500
}
fn default_ml_bytes() -> usize {
    64 * 1024
}
fn default_ml_timeout() -> u64 {
    1000
}

#[derive(Debug, Default, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Parser {
    Json,
    #[default]
    Plain,
    /// Docker's json-file driver: `{"log","stream","time"}` per line,
    /// with >16 KB lines split across frames. Reassembled upstream, then
    /// parsed as JSON — see [`crate::docker`].
    #[serde(rename = "docker_json")]
    DockerJson,
    /// systemd journal entries (#23) — parsed as JSON after the journald
    /// source turns each entry into a JSON object.
    Journald,
    /// Windows Event Log events (#11) — parsed as JSON after the winlog
    /// source renders each event and pulls the kept fields out of its XML.
    /// The source's `path` names the channel (`System`, `Application`, …).
    Winlog,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Integer,
    Float,
    Boolean,
}

#[derive(Debug, Deserialize)]
pub struct Timestamp {
    /// Parsed key holding the timestamp. Absent means "stamp on read".
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default = "default_format")]
    pub format: String,
    /// Drives the disambiguator. Declaring this too fine is the one
    /// configuration mistake that loses data silently.
    #[serde(default = "default_resolution")]
    pub resolution: String,
}

fn default_format() -> String {
    "unix_ms".into()
}
fn default_resolution() -> String {
    "ms".into()
}

impl Default for Timestamp {
    fn default() -> Self {
        Timestamp {
            field: None,
            format: default_format(),
            resolution: default_resolution(),
        }
    }
}

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Config> {
        let text = std::fs::read_to_string(path)?;
        let cfg: Config = toml::from_str(&text)?;
        if cfg.sources.is_empty() && cfg.otlp.is_none() && cfg.metrics.is_none() {
            anyhow::bail!(
                "nothing to do: configure at least one [[source]], an [otlp] receiver, or [metrics]"
            );
        }
        if let Some(m) = &cfg.metrics {
            m.validate()?;
        }
        if let Some(o) = &cfg.otlp {
            if o.listen.trim().is_empty() {
                anyhow::bail!("[otlp].listen is required (host:port to receive on)");
            }
            if o.table.trim().is_empty() {
                anyhow::bail!("[otlp].table is required");
            }
            if o.name.trim().is_empty() {
                anyhow::bail!("[otlp].name is required (it becomes the `stream` tag)");
            }
        }
        for src in &cfg.sources {
            if src.parser == Parser::Journald {
                #[cfg(not(feature = "journald"))]
                anyhow::bail!(
                    "source '{}' is journald, but this binary was built without the                      `journald` feature (rebuild with --features journald on a systemd host)",
                    src.name
                );
            } else if src.parser == Parser::Winlog {
                #[cfg(not(feature = "winlog"))]
                anyhow::bail!(
                    "source '{}' is winlog, but this binary was built without the `winlog` \
                     feature (rebuild with --features winlog for a Windows host)",
                    src.name
                );
                // The channel to read rides in `path` (`System`, `Application`,
                // or a custom channel path). Empty is a mistake, not a tail.
                #[cfg(feature = "winlog")]
                if src.path.trim().is_empty() {
                    anyhow::bail!(
                        "source '{}' is winlog but names no channel — set `path` to a \
                         channel like \"System\" or \"Application\"",
                        src.name
                    );
                }
            } else if src.path.trim().is_empty() {
                anyhow::bail!("source '{}' has no `path` to tail", src.name);
            }
        }
        if let Some(tls) = &cfg.output.tls {
            tls.validate()?;
            // A client certificate over plain HTTP is never presented — the
            // handshake it belongs to does not happen. Refusing here turns a
            // silent downgrade to anonymous into a startup error.
            if tls.has_identity() && cfg.output.url.starts_with("http://") {
                anyhow::bail!(
                    "[output.tls] configures a client certificate but [output].url is http:// — \
                     the certificate would never be presented. Use https://."
                );
            }
        }
        for s in &cfg.sources {
            if s.parser == Parser::DockerJson && s.multiline.is_some() {
                anyhow::bail!(
                    "source '{}': parser = \"docker_json\" cannot also set \n                     [source.multiline] — the docker reassembler owns framing",
                    s.name
                );
            }
            if crate::stamp::Resolution::parse(&s.resolution_str()).is_none() {
                anyhow::bail!(
                    "source '{}': resolution {:?} is not s|ms|us|ns",
                    s.name,
                    s.timestamp.resolution
                );
            }
        }
        Ok(cfg)
    }
}

impl Source {
    pub fn resolution_str(&self) -> String {
        self.timestamp.resolution.clone()
    }
    pub fn resolution(&self) -> crate::stamp::Resolution {
        crate::stamp::Resolution::parse(&self.timestamp.resolution).expect("validated at load")
    }
}

/// OTLP logs receiver (#12). A push source: an OpenTelemetry producer or
/// Collector `POST`s to `listen`, and each log record maps onto a record on
/// the SAME map -> queue -> ship path a file tail uses. The tag `allowlist`
/// is the whole cardinality defence (FR-2): a resource attribute becomes a
/// tag only if named here, never by arriving.
#[derive(Debug, Deserialize, Clone)]
pub struct Otlp {
    /// host:port to receive OTLP/HTTP on (the OTLP default is 4318).
    pub listen: String,
    /// Stream identity — becomes the `stream` tag, like a source's name.
    pub name: String,
    pub table: String,
    /// The tag ALLOWLIST over resource/scope/log attributes (plus `body`,
    /// `severity_text`, `scope.name`). Never "every attribute": OTLP resource
    /// attributes are unbounded, and each becoming a tag rebuilds the FR-2
    /// failure this project exists to avoid.
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub tags_static: BTreeMap<String, String>,
    /// Declared field types (usually `body = "string"`). Same rule as a
    /// source: an undeclared key is dropped, not guessed.
    #[serde(default)]
    pub fields: BTreeMap<String, FieldType>,
    #[serde(default)]
    pub visibility: Option<String>,
}

impl Otlp {
    /// A synthetic [`Source`] so the receiver reuses `map::build_record` and
    /// inherits the same allowlist and declared-field rules. OTLP timestamps
    /// are nanosecond (`time_unix_nano`), so the stamper runs at ns.
    pub fn as_source(&self) -> Source {
        Source {
            name: self.name.clone(),
            path: String::new(),
            table: self.table.clone(),
            parser: Parser::Plain,
            timestamp: Timestamp {
                field: None,
                format: default_format(),
                resolution: "ns".into(),
            },
            tags: self.tags.clone(),
            tags_static: self.tags_static.clone(),
            fields: self.fields.clone(),
            visibility: self.visibility.clone(),
            multiline: None,
        }
    }
}

/// A constant value for `[metrics.static_fields]`. TOML's own scalar types
/// map straight onto a line-protocol field type — an untagged enum so
/// `deployment = "prod"`, `weight = 3`, `ratio = 0.5`, `canary = true` each
/// land as the type they look like. Order matters for `serde(untagged)`:
/// bool and integer are tried before float and string so `3` stays an
/// integer field (`3i`), not a float.
#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum FieldValue {
    Bool(bool),
    Integer(i64),
    Float(f64),
    Str(String),
}

/// Host-metrics collector (#25). Samples the machine every `interval` and
/// writes Telegraf's measurements (`cpu`/`mem`/`disk`/`net`/`system`/`swap`)
/// with Telegraf's exact names, so dashboards survive a migration off
/// InfluxDB + Telegraf. `global_tags`/`static_fields` are the "add your own
/// fields" half — stamped on every point (mirrors [`Source::tags_static`]).
#[derive(Debug, Deserialize, Clone)]
pub struct Metrics {
    #[serde(default = "default_metrics_interval")]
    pub interval: String,
    #[serde(default = "default_collectors")]
    pub collectors: Vec<String>,
    /// The `host` tag value. Absent = the OS hostname.
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub global_tags: BTreeMap<String, String>,
    #[serde(default)]
    pub static_fields: BTreeMap<String, FieldValue>,
}

fn default_metrics_interval() -> String {
    "10s".into()
}
fn default_collectors() -> Vec<String> {
    ["cpu", "mem", "disk", "diskio", "net", "system", "swap"]
        .iter()
        .map(|s| s.to_string())
        .collect()
}

/// The collectors this build knows how to run. A name outside this set is a
/// typo worth refusing at startup, not a silently ignored line in a config.
pub const KNOWN_COLLECTORS: [&str; 7] = ["cpu", "mem", "disk", "diskio", "net", "system", "swap"];

impl Metrics {
    pub fn interval_parsed(&self) -> anyhow::Result<std::time::Duration> {
        crate::logfile::parse_duration(&self.interval).ok_or_else(|| {
            anyhow::anyhow!(
                "[metrics].interval {:?} is not a duration like 10s or 1m",
                self.interval
            )
        })
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        self.interval_parsed()?;
        if self.collectors.is_empty() {
            anyhow::bail!(
                "[metrics].collectors is empty — omit the [metrics] section to disable metrics, \
                 or name at least one of: {}",
                KNOWN_COLLECTORS.join(", ")
            );
        }
        for c in &self.collectors {
            if !KNOWN_COLLECTORS.contains(&c.as_str()) {
                anyhow::bail!(
                    "[metrics].collectors has unknown collector {c:?}; known: {}",
                    KNOWN_COLLECTORS.join(", ")
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_str(toml: &str) -> anyhow::Result<Config> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.toml");
        std::fs::write(&path, toml).unwrap();
        Config::load(&path)
    }

    #[test]
    fn an_otlp_only_config_is_valid_without_a_source() {
        let cfg = load_str(
            "[output]\nurl = \"http://localhost:1963\"\n\n\
             [otlp]\nlisten = \"0.0.0.0:4318\"\nname = \"otlp\"\ntable = \"logs\"\n",
        )
        .expect("an OTLP-only agent is a valid configuration");
        assert!(cfg.sources.is_empty());
        assert_eq!(cfg.otlp.unwrap().table, "logs");
    }

    #[test]
    fn otlp_without_listen_is_refused() {
        let err = load_str(
            "[output]\nurl = \"http://localhost:1963\"\n\n\
             [otlp]\nlisten = \"\"\nname = \"otlp\"\ntable = \"logs\"\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("listen"), "got: {err}");
    }

    #[test]
    fn nothing_configured_is_refused() {
        let err = load_str("[output]\nurl = \"http://localhost:1963\"\n").unwrap_err();
        assert!(
            err.to_string().contains("source") || err.to_string().contains("otlp"),
            "got: {err}"
        );
    }

    #[test]
    fn a_metrics_only_config_is_valid_without_a_source() {
        let cfg = load_str(
            "[output]\nurl = \"http://localhost:1963\"\n\n\
             [metrics]\ninterval = \"5s\"\n\n\
             [metrics.global_tags]\nregion = \"us-east\"\n\n\
             [metrics.static_fields]\ndeployment = \"prod\"\nweight = 3\nratio = 0.5\n",
        )
        .expect("a metrics-only agent is a valid configuration");
        let m = cfg.metrics.expect("metrics present");
        assert_eq!(m.interval, "5s");
        // Untagged FieldValue keeps TOML's types apart.
        assert_eq!(
            m.static_fields["deployment"],
            FieldValue::Str("prod".into())
        );
        assert_eq!(m.static_fields["weight"], FieldValue::Integer(3));
        assert_eq!(m.static_fields["ratio"], FieldValue::Float(0.5));
    }

    #[test]
    fn an_unknown_collector_is_refused() {
        let err = load_str(
            "[output]\nurl = \"http://localhost:1963\"\n\n\
             [metrics]\ncollectors = [\"cpu\", \"gpu\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("gpu"), "got: {err}");
    }

    #[test]
    fn diskio_is_a_known_collector() {
        let cfg = load_str(
            "[output]\nurl = \"http://localhost:1963\"\n\n\
             [metrics]\ncollectors = [\"diskio\"]\n",
        )
        .expect("diskio is a valid collector");
        assert_eq!(cfg.metrics.unwrap().collectors, vec!["diskio"]);
    }

    #[test]
    fn a_bad_metrics_interval_is_refused_at_load() {
        let err = load_str(
            "[output]\nurl = \"http://localhost:1963\"\n\n\
             [metrics]\ninterval = \"soon\"\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("interval"), "got: {err}");
    }
}

#[cfg(test)]
mod cfg_journald_tests {
    #[cfg(not(feature = "journald"))]
    #[test]
    fn a_journald_source_is_refused_without_the_feature() {
        let dir = std::env::temp_dir().join(format!("trib-jrnl-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("j.toml");
        std::fs::write(
            &path,
            "[output]
url = \"http://localhost:1963\"

[[source]]
name = \"j\"
table = \"syslog\"
parser = \"journald\"
",
        )
        .unwrap();
        let err = super::Config::load(&path).unwrap_err().to_string();
        assert!(err.contains("journald"), "got: {err}");
    }
}

#[cfg(test)]
mod cfg_winlog_tests {
    #[cfg(not(feature = "winlog"))]
    #[test]
    fn a_winlog_source_is_refused_without_the_feature() {
        let dir = std::env::temp_dir().join(format!("trib-winlog-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("w.toml");
        std::fs::write(
            &path,
            "[output]
url = \"http://localhost:1963\"

[[source]]
name = \"winsys\"
path = \"System\"
table = \"eventlog\"
parser = \"winlog\"
",
        )
        .unwrap();
        let err = super::Config::load(&path).unwrap_err().to_string();
        assert!(err.contains("winlog"), "got: {err}");
    }
}
