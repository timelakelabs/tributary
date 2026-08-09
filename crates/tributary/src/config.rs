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
