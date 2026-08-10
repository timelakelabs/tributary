//! Configuration. Every field here exists because of a property of
//! TimeLakeDB's write contract (DESIGN.md §1), which is why the safe
//! choices are the defaults and the dangerous ones must be spelled out.

use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub output: Output,
    #[serde(rename = "source")]
    pub sources: Vec<Source>,
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
    pub path: String,
    pub table: String,
    #[serde(default)]
    pub parser: Parser,
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

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Config> {
        let text = std::fs::read_to_string(path)?;
        let cfg: Config = toml::from_str(&text)?;
        if cfg.sources.is_empty() {
            anyhow::bail!("no [[source]] configured");
        }
        for s in &cfg.sources {
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
