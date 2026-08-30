//! Allowlisted pod-label enrichment (#8 phase 4, #66).
//!
//! pod/namespace/container come free from the log path (#63). Labels
//! (`app`, `team`, `version`) do not — they live in the API server — and they
//! are the cardinality bomb the whole allowlist exists to contain: a
//! default-everything enrichment turns one Deployment rollout into thousands of
//! dead series through the unbounded `pod-template-hash` label. So a label
//! becomes a tag ONLY if the operator named it in `[source.kubernetes].labels`.
//!
//! Two rules keep this from being a self-inflicted outage (#8):
//!
//! 1. **Allowlist first.** Nothing is stamped by default; the resolver filters
//!    to the named subset before anything reaches a tag.
//! 2. **Resolve once per pod, never per line.** A per-line API call would
//!    rate-limit the agent off the node under load. A glob child resolves its
//!    pod's labels ONCE at startup and holds them; a shared TTL cache collapses
//!    the several containers of one pod into a single request. The child's held
//!    labels vanish when its log file disappears — that IS the invalidation.
//!
//! The label SOURCE is pluggable: the in-cluster API server by default, or a
//! static JSON file (`label_file`) for air-gapped deployments and for the
//! offline cardinality drill, which cannot reach an API server.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::config::Kubernetes;

/// Where a glob source resolves pod labels from.
pub enum LabelResolver {
    /// No allowlist, or no reachable source — resolve to nothing.
    Disabled,
    /// A static `namespace/pod -> {label: value}` JSON map. For air-gapped
    /// deployments and the drill.
    File(BTreeMap<String, BTreeMap<String, String>>),
    /// The in-cluster Kubernetes API server.
    Api(ApiResolver),
}

impl LabelResolver {
    /// Build the resolver for a kubernetes source. `Disabled` when the labels
    /// allowlist is empty (nothing to resolve) or when there is no source to
    /// resolve from. A configured `label_file` that will not load is a hard
    /// error — a misconfigured enrichment should fail at startup, not silently
    /// stamp nothing.
    pub fn from_kubernetes(k: &Kubernetes) -> anyhow::Result<LabelResolver> {
        if k.labels.is_empty() {
            return Ok(LabelResolver::Disabled);
        }
        if let Some(path) = &k.label_file {
            let bytes = std::fs::read(path).map_err(|e| {
                anyhow::anyhow!("could not read [source.kubernetes].label_file {path:?}: {e}")
            })?;
            let map: BTreeMap<String, BTreeMap<String, String>> = serde_json::from_slice(&bytes)
                .map_err(|e| {
                    anyhow::anyhow!("label_file {path:?} is not a namespace/pod -> labels map: {e}")
                })?;
            tracing::info!(file = %path.display(), pods = map.len(), "pod labels from a static file");
            return Ok(LabelResolver::File(map));
        }
        match ApiResolver::in_cluster() {
            Some(api) => {
                tracing::info!("pod labels from the in-cluster API server");
                Ok(LabelResolver::Api(api))
            }
            None => {
                tracing::warn!(
                    "[source.kubernetes].labels is set but there is no label source: not \
                     running in a cluster and no label_file — labels will NOT be stamped"
                );
                Ok(LabelResolver::Disabled)
            }
        }
    }

    /// Resolve a pod's labels, filtered to the allowlist. The filter is applied
    /// HERE, so an unbounded label the operator did not name can never reach a
    /// tag — the cardinality guarantee holds regardless of what the pod carries.
    pub async fn resolve_allowlisted(
        &self,
        namespace: &str,
        pod: &str,
        allowlist: &[String],
    ) -> BTreeMap<String, String> {
        let all = match self {
            LabelResolver::Disabled => return BTreeMap::new(),
            LabelResolver::File(map) => map
                .get(&format!("{namespace}/{pod}"))
                .cloned()
                .unwrap_or_default(),
            LabelResolver::Api(api) => api.labels(namespace, pod).await,
        };
        allowlist
            .iter()
            .filter_map(|k| all.get(k).map(|v| (k.clone(), v.clone())))
            .collect()
    }
}

/// A pod's resolved labels and the moment they were fetched.
type CachedLabels = (Instant, BTreeMap<String, String>);

/// In-cluster API-server client with a short-TTL per-pod cache.
pub struct ApiResolver {
    client: reqwest::Client,
    base: String,
    token: String,
    cache: Mutex<BTreeMap<String, CachedLabels>>,
}

/// How long a resolved pod stays cached. Long enough to collapse the containers
/// of one pod into one request; short enough that a departed pod's entry ages
/// out rather than accumulating across a rollout.
const CACHE_TTL: Duration = Duration::from_secs(120);

const SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

impl ApiResolver {
    /// Build from the in-cluster service-account files, or `None` if this is not
    /// running in a cluster (no `KUBERNETES_SERVICE_HOST`).
    pub fn in_cluster() -> Option<ApiResolver> {
        let host = std::env::var("KUBERNETES_SERVICE_HOST").ok()?;
        let port = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".into());
        let token = std::fs::read_to_string(format!("{SA_DIR}/token")).ok()?;
        let ca_pem = std::fs::read(format!("{SA_DIR}/ca.crt")).ok()?;
        let ca = reqwest::Certificate::from_pem(&ca_pem).ok()?;
        let client = reqwest::Client::builder()
            .add_root_certificate(ca)
            .build()
            .ok()?;
        Some(ApiResolver {
            client,
            base: format!("https://{host}:{port}"),
            token: token.trim().to_string(),
            cache: Mutex::new(BTreeMap::new()),
        })
    }

    /// All labels for a pod (unfiltered — the caller applies the allowlist).
    /// A cache hit within the TTL returns without touching the network; a miss,
    /// or any error, resolves to an empty map so enrichment degrades to
    /// pod/namespace/container rather than failing the tail.
    async fn labels(&self, namespace: &str, pod: &str) -> BTreeMap<String, String> {
        let key = format!("{namespace}/{pod}");
        if let Ok(cache) = self.cache.lock()
            && let Some((at, labels)) = cache.get(&key)
            && at.elapsed() < CACHE_TTL
        {
            return labels.clone();
        }
        let labels = match self.fetch(namespace, pod).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(pod = %key, error = %e, "could not resolve pod labels — stamping none");
                BTreeMap::new()
            }
        };
        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(key, (Instant::now(), labels.clone()));
        }
        labels
    }

    async fn fetch(&self, namespace: &str, pod: &str) -> anyhow::Result<BTreeMap<String, String>> {
        let url = format!("{}/api/v1/namespaces/{namespace}/pods/{pod}", self.base);
        let resp = self
            .client
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await?;
        if !resp.status().is_success() {
            anyhow::bail!("API server returned {}", resp.status());
        }
        let body = resp.bytes().await?;
        Ok(parse_pod_labels(&body))
    }
}

/// Pull `metadata.labels` out of a pod JSON. A missing or malformed labels
/// object is an empty map, not an error — a pod with no labels is normal.
pub fn parse_pod_labels(body: &[u8]) -> BTreeMap<String, String> {
    let v: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return BTreeMap::new(),
    };
    v.get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(|l| l.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kube(labels: &[&str], file: Option<&str>) -> Kubernetes {
        Kubernetes {
            labels: labels.iter().map(|s| s.to_string()).collect(),
            label_file: file.map(std::path::PathBuf::from),
        }
    }

    #[test]
    fn an_empty_allowlist_is_disabled() {
        assert!(matches!(
            LabelResolver::from_kubernetes(&kube(&[], None)).unwrap(),
            LabelResolver::Disabled
        ));
    }

    #[tokio::test]
    async fn a_file_source_resolves_only_allowlisted_labels() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("labels.json");
        std::fs::write(
            &p,
            r#"{"shop/web-abc":{"app":"web","team":"pay","pod-template-hash":"7d9c8b6f5d"}}"#,
        )
        .unwrap();
        let r = LabelResolver::from_kubernetes(&kube(&["app", "team"], Some(p.to_str().unwrap())))
            .unwrap();

        let got = r
            .resolve_allowlisted("shop", "web-abc", &["app".into(), "team".into()])
            .await;
        assert_eq!(got.get("app").map(String::as_str), Some("web"));
        assert_eq!(got.get("team").map(String::as_str), Some("pay"));
        // The unbounded label is present in the source but NOT allowlisted, so
        // it must never become a tag — this is the whole cardinality defence.
        assert!(!got.contains_key("pod-template-hash"));
    }

    #[tokio::test]
    async fn an_unknown_pod_resolves_to_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("labels.json");
        std::fs::write(&p, r#"{"shop/web-abc":{"app":"web"}}"#).unwrap();
        let r = LabelResolver::from_kubernetes(&kube(&["app"], Some(p.to_str().unwrap()))).unwrap();
        assert!(
            r.resolve_allowlisted("shop", "missing", &["app".into()])
                .await
                .is_empty()
        );
    }

    #[test]
    fn a_missing_label_file_is_a_startup_error() {
        assert!(
            LabelResolver::from_kubernetes(&kube(&["app"], Some("/no/such/labels.json"))).is_err()
        );
    }

    #[test]
    fn parses_labels_out_of_a_pod_json() {
        let body = br#"{"metadata":{"name":"web-abc","labels":{"app":"web","version":"3"}}}"#;
        let m = parse_pod_labels(body);
        assert_eq!(m.get("app").map(String::as_str), Some("web"));
        assert_eq!(m.get("version").map(String::as_str), Some("3"));
        // A pod with no labels object is empty, not an error.
        assert!(parse_pod_labels(br#"{"metadata":{"name":"x"}}"#).is_empty());
        assert!(parse_pod_labels(b"not json").is_empty());
    }
}
