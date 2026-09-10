//! The DaemonSet manifest pins the agent image to a version, and that
//! version is this crate's (#77).
//!
//! `:latest` in `deploy/k8s/daemonset.yaml` meant "whatever main last
//! built", so a node that rebooted pulled a different agent than its
//! neighbours. Pinned, the tag has to move when the version does, and a tag
//! somebody remembers to bump by hand is a tag that eventually does not get
//! bumped — so the release commit that bumps `Cargo.toml` fails this test
//! until it bumps the manifest too. `deploy/k8s/validate.py` makes the same
//! check for a human without cargo.

use std::path::Path;

#[test]
fn the_daemonset_image_tag_is_the_crate_version() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/k8s/daemonset.yaml");
    let text = std::fs::read_to_string(&manifest)
        .unwrap_or_else(|e| panic!("read {}: {e}", manifest.display()));
    let images: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter_map(|l| l.strip_prefix("image: "))
        .collect();
    assert_eq!(
        images.len(),
        1,
        "exactly one `image:` line in the manifest: {images:?}"
    );
    let image = images[0].trim();
    let want = format!(
        "ghcr.io/timelakelabs/tributary:{}",
        env!("CARGO_PKG_VERSION")
    );
    assert_eq!(
        image,
        want,
        "deploy/k8s/daemonset.yaml pins `{image}`; the crate is {}. Bump the manifest \
         with the version (the release commit does both), never back to a floating tag.",
        env!("CARGO_PKG_VERSION")
    );
}
