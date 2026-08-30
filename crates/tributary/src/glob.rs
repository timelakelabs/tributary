//! A deliberately small path glob, for tailing a directory of container logs
//! (`/var/log/containers/*.log`, #8/#64).
//!
//! This is not a general glob library. The wildcard lives in the FINAL path
//! segment only — `<literal dir>/<name pattern>` — which is exactly the CRI
//! layout and nothing more. `*` matches a run of any characters, `?` one
//! character; there is no `**`, no brace expansion, no character classes. A
//! path with no wildcard in its last segment is not a glob at all, and a
//! source with such a path tails the single file as it always has.
//!
//! Keeping it this narrow is a choice, not a shortcut: a DaemonSet points at
//! one flat directory of symlinks, and a matcher that can only do that can't
//! surprise anyone by recursing into `/var/log` or following `..`.

use std::path::{Path, PathBuf};

/// Does this path's final segment carry a glob wildcard? Only then is the
/// source tailed as a directory of files rather than as one file — so an
/// ordinary path (no config with a literal `*` in a log filename exists in
/// practice) keeps the single-file behaviour untouched.
pub fn is_glob(path: &str) -> bool {
    let last = path.rsplit(['/', '\\']).next().unwrap_or(path);
    last.contains('*') || last.contains('?')
}

/// The files a glob pattern currently matches, sorted for a deterministic
/// discovery order. The directory part is literal; only the filename is
/// matched. A missing directory is not an error — it's an empty match, because
/// `/var/log/containers` may simply not exist yet on a node with no pods.
pub fn matches(pattern: &str) -> Vec<PathBuf> {
    let (dir, name_pat) = split(pattern);
    let read = match std::fs::read_dir(&dir) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    let mut out: Vec<PathBuf> = read
        .filter_map(|e| e.ok())
        .filter(|e| {
            wildcard(
                name_pat.as_bytes(),
                e.file_name().to_string_lossy().as_bytes(),
            )
        })
        .map(|e| dir.join(e.file_name()))
        .collect();
    out.sort();
    out
}

/// Split `dir/name-pattern` at the last separator. A pattern with no separator
/// matches names in the current directory.
fn split(pattern: &str) -> (PathBuf, String) {
    match pattern.rfind(['/', '\\']) {
        Some(i) => (PathBuf::from(&pattern[..i]), pattern[i + 1..].to_string()),
        None => (PathBuf::from("."), pattern.to_string()),
    }
}

/// A stable stream identity for one matched file, namespaced under the glob
/// source. It keys the file's checkpoint and queue on disk, so it MUST be the
/// same string every run for a given container log — hence it's derived from
/// the filename, which for a CRI symlink includes the container id and so is
/// stable for that container's whole life. When the container restarts the id
/// changes, the filename changes, and this correctly becomes a new stream that
/// resumes from zero rather than inheriting a dead container's offset.
pub fn stream_id(source_name: &str, path: &Path) -> String {
    let file = path
        .file_name()
        .map(|f| f.to_string_lossy().into_owned())
        .unwrap_or_default();
    let stem = file.strip_suffix(".log").unwrap_or(&file);
    format!("{source_name}.{stem}")
}

/// Classic iterative wildcard match with `*` backtracking. Bytes, so a
/// non-UTF-8 filename can't panic it (CRI names are ASCII, but the tailer
/// should never trust that).
fn wildcard(pat: &[u8], s: &[u8]) -> bool {
    let (mut p, mut c) = (0usize, 0usize);
    let (mut star, mut mark) = (usize::MAX, 0usize);
    while c < s.len() {
        if p < pat.len() && (pat[p] == b'?' || pat[p] == s[c]) {
            p += 1;
            c += 1;
        } else if p < pat.len() && pat[p] == b'*' {
            star = p;
            mark = c;
            p += 1;
        } else if star != usize::MAX {
            p = star + 1;
            mark += 1;
            c = mark;
        } else {
            return false;
        }
    }
    while p < pat.len() && pat[p] == b'*' {
        p += 1;
    }
    p == pat.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_a_wildcard_only_in_the_last_segment() {
        assert!(is_glob("/var/log/containers/*.log"));
        assert!(is_glob("/var/log/containers/app-?.log"));
        // A literal path is not a glob, even one with a dir that looks starred.
        assert!(!is_glob("/var/log/app.log"));
        assert!(!is_glob("/var/log/containers/pod_ns_ctr-abc.log"));
    }

    #[test]
    fn wildcard_matches_and_rejects() {
        assert!(wildcard(b"*.log", b"anything.log"));
        assert!(wildcard(b"*.log", b".log"));
        assert!(wildcard(b"app-?.log", b"app-3.log"));
        assert!(!wildcard(b"app-?.log", b"app-33.log"));
        assert!(!wildcard(b"*.log", b"file.txt"));
        // '*' must be able to match the middle, not just a suffix.
        assert!(wildcard(b"pod_*_ctr-*.log", b"pod_ns_ctr-deadbeef.log"));
    }

    #[test]
    fn matches_reads_a_directory_and_sorts() {
        let dir = tempfile::tempdir().unwrap();
        for n in ["a.log", "b.log", "c.txt"] {
            std::fs::write(dir.path().join(n), "x").unwrap();
        }
        let pat = format!("{}/*.log", dir.path().display());
        let got: Vec<String> = matches(&pat)
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(got, vec!["a.log", "b.log"], "only .log, sorted, no .txt");
    }

    #[test]
    fn a_missing_directory_is_an_empty_match_not_an_error() {
        assert!(matches("/no/such/dir/on/this/box/*.log").is_empty());
    }

    #[test]
    fn stream_id_is_stable_and_namespaced() {
        let p = Path::new("/var/log/containers/web-7d9_shop_server-deadbeef.log");
        assert_eq!(stream_id("k8s", p), "k8s.web-7d9_shop_server-deadbeef");
        // Same file, same id every time — that's what makes resume work.
        assert_eq!(stream_id("k8s", p), stream_id("k8s", p));
    }
}
