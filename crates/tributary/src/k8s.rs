//! Kubernetes enrichment for the DaemonSet deployment (#8).
//!
//! A container log under a CRI runtime (containerd, CRI-O, the old docker
//! shim) is written to `/var/log/pods/…` and symlinked into a flat directory
//! the agent actually tails:
//!
//! ```text
//! /var/log/containers/<pod>_<namespace>_<container>-<container-id>.log
//! ```
//!
//! The pod, namespace and container are right there in the filename, so a
//! sidecar-less DaemonSet gets them for free — no apiserver call, no watch,
//! no per-record lookup. Phase 1 stamps exactly those three, which are
//! bounded by the node's pod count and safe as tags (FR-2). Pod labels are
//! unbounded and wait for the phase-4 allowlist ([#66]).
//!
//! ## The parse is not `split('-')`
//!
//! Every field here contains hyphens — a pod is `<deploy>-<rs-hash>-<pod-hash>`,
//! a container is `nginx-ingress-controller`. The ONE thing that doesn't is
//! the underscore: pod, namespace and container are DNS-1123 names, which
//! forbid `_`. So the three fields split cleanly on `_`, and the container-id
//! is peeled off the tail by the LAST hyphen, then checked for being a real
//! id. Splitting on the first hyphen, or trusting the split without checking
//! the id, turns `istio-proxy-<id>` into container `istio` every time.

/// The three bounded identifiers carried by a CRI log path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CriMeta {
    pub pod: String,
    pub namespace: String,
    pub container: String,
}

/// A CRI container-id is the full 64-hex container hash (containerd, CRI-O and
/// the docker shim all write it into the symlink name). Requiring the full
/// width is what disambiguates the id from a container name whose last
/// hyphen-segment happens to look hex-ish — `web-cafe` must not read as
/// container `web`, id `cafe`.
fn looks_like_container_id(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Parse a CRI container-log path into its pod/namespace/container. Returns
/// `None` for anything that is not that exact shape, so a non-Kubernetes path
/// (a plain `/var/log/app.log`) simply isn't enriched rather than being
/// stamped with garbage.
pub fn parse_cri_path(path: &str) -> Option<CriMeta> {
    // Work on the basename: the tailer may hand us the symlink under
    // /var/log/containers, or the resolved target — only the filename matters.
    let file = path.rsplit(['/', '\\']).next()?;
    let stem = file.strip_suffix(".log")?;

    // Exactly two underscores separate pod / namespace / container-and-id.
    // splitn(3) leaves any extra underscore inside the third field; since none
    // of the three names may contain `_`, an underscore surviving there means
    // this wasn't a CRI filename.
    let mut parts = stem.splitn(3, '_');
    let pod = parts.next()?;
    let namespace = parts.next()?;
    let container_and_id = parts.next()?;
    if pod.is_empty() || namespace.is_empty() || container_and_id.contains('_') {
        return None;
    }

    // Peel the id off the LAST hyphen — container names contain hyphens, the
    // id does not (it's hex), so rsplit_once is the only correct cut.
    let (container, id) = container_and_id.rsplit_once('-')?;
    if container.is_empty() || !looks_like_container_id(id) {
        return None;
    }

    Some(CriMeta {
        pod: pod.to_string(),
        namespace: namespace.to_string(),
        container: container.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "3f8b2c1d4e5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c"; // 64 hex

    #[test]
    fn parses_a_real_hyphenated_path() {
        // Every field carries hyphens: this is the case a naive split breaks on.
        let path = format!(
            "/var/log/containers/nginx-ingress-controller-7d9c8b6f5d-abc12_ingress-nginx_controller-{ID}.log"
        );
        let m = parse_cri_path(&path).expect("a well-formed CRI path parses");
        assert_eq!(m.pod, "nginx-ingress-controller-7d9c8b6f5d-abc12");
        assert_eq!(m.namespace, "ingress-nginx");
        assert_eq!(m.container, "controller");
    }

    #[test]
    fn keeps_hyphens_inside_the_container_name() {
        // `istio-proxy` must not collapse to `istio` — the id is peeled off
        // the last hyphen only, and only because it's a 64-hex run.
        let path = format!(
            "/var/log/containers/reviews-v3-6c98bcbf89-2xk4p_bookinfo_istio-proxy-{ID}.log"
        );
        let m = parse_cri_path(&path).unwrap();
        assert_eq!(m.container, "istio-proxy");
        assert_eq!(m.namespace, "bookinfo");
    }

    #[test]
    fn a_non_cri_path_is_not_enriched() {
        assert_eq!(parse_cri_path("/var/log/app.log"), None);
        assert_eq!(parse_cri_path("/var/log/syslog"), None);
        // Right prefix, wrong shape (no id): must not stamp a bogus container.
        assert_eq!(
            parse_cri_path("/var/log/containers/pod_ns_container.log"),
            None
        );
    }

    #[test]
    fn a_hexish_container_suffix_is_not_mistaken_for_an_id() {
        // `web-cafe`: `cafe` is hex but 4 chars, not a container id, so the
        // whole thing stays the container name.
        let path = format!("/var/log/containers/shop-abc_default_web-cafe-{ID}.log");
        let m = parse_cri_path(&path).unwrap();
        assert_eq!(m.container, "web-cafe");
    }

    #[test]
    fn a_short_or_nonhex_id_is_rejected() {
        // Truncated id (32 hex, not 64) — not the CRI format, so no enrichment.
        let half = &ID[..32];
        assert_eq!(
            parse_cri_path(&format!("/var/log/containers/p_ns_c-{half}.log")),
            None
        );
    }
}
