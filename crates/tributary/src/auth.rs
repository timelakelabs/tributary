//! The data-plane credential Tributary presents to TimeLakeDB.
//!
//! TimeLakeDB's data plane authenticates by token (SEC-4 phased): one bearer
//! token on the `Authorization` header, accepted as `Bearer <token>`. This
//! resolves that token and wraps it so it cannot reach a log line by
//! accident.
//!
//! WHERE THE TOKEN COMES FROM — never the config file. A secret written into
//! a committed TOML is a secret leaked, so the token is sourced only from the
//! environment (`TRIBUTARY_TOKEN`, the 12-factor path) or a file
//! (`token_file`, for a Kubernetes secret mount or a systemd credential).
//! This mirrors TimeLakeDB's own `TIMELAKE_ENCRYPTION_KEY` / `_KEY_FILE`
//! split. A later `CredentialSource` backend (Vault, ROADMAP §4) slots in at
//! this same seam.

use std::path::Path;

/// A secret that refuses to print itself.
///
/// `Debug` and `Display` both redact, so `tracing::error!(?token)` or a
/// `{token}` in a format string yields the placeholder, never the value.
/// The bytes are reachable only through [`Secret::expose`], which names what
/// it does at every call site — the one place a reviewer has to look.
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    /// The raw secret. Call sites are deliberately conspicuous: this is the
    /// only door out of the wrapper.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(redacted)")
    }
}

impl std::fmt::Display for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Resolve the data-plane token.
///
/// Precedence: the `env_var` value wins (a running process's environment is
/// the most direct source); otherwise `token_file` is read and trimmed, so a
/// trailing newline in a mounted secret is not part of the token; otherwise
/// `None` — the node is presumably running `TIMELAKE_DATA_AUTH=off` and no
/// credential is needed. An empty env var is treated as unset (a common shell
/// accident) so it does not silently become an empty, always-rejected token.
pub fn resolve_token(env_var: &str, token_file: Option<&Path>) -> anyhow::Result<Option<Secret>> {
    if let Ok(v) = std::env::var(env_var) {
        let v = v.trim();
        if !v.is_empty() {
            return Ok(Some(Secret(v.to_string())));
        }
    }
    if let Some(path) = token_file {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading token_file {}: {e}", path.display()))?;
        let v = raw.trim();
        if v.is_empty() {
            anyhow::bail!("token_file {} is empty", path.display());
        }
        return Ok(Some(Secret(v.to_string())));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_secret_never_prints_its_value() {
        let s = Secret("tldb_supersecret".into());
        assert!(!format!("{s:?}").contains("supersecret"), "Debug leaked");
        assert!(!format!("{s}").contains("supersecret"), "Display leaked");
        assert_eq!(s.expose(), "tldb_supersecret", "expose still returns it");
    }

    #[test]
    fn env_wins_and_is_trimmed() {
        // A unique var name so the test never collides with a real env.
        let var = "TRIBUTARY_TOKEN_TEST_ENV_WINS";
        unsafe { std::env::set_var(var, "  tldb_fromenv\n") };
        let got = resolve_token(var, None).unwrap().unwrap();
        assert_eq!(got.expose(), "tldb_fromenv");
        unsafe { std::env::remove_var(var) };
    }

    #[test]
    fn file_is_read_and_trailing_newline_stripped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "tldb_fromfile\n").unwrap();
        let var = "TRIBUTARY_TOKEN_TEST_FILE_ONLY";
        let got = resolve_token(var, Some(&path)).unwrap().unwrap();
        assert_eq!(
            got.expose(),
            "tldb_fromfile",
            "trailing newline is not part of the token"
        );
    }

    #[test]
    fn empty_env_falls_through_rather_than_becoming_an_empty_token() {
        let var = "TRIBUTARY_TOKEN_TEST_EMPTY";
        unsafe { std::env::set_var(var, "   ") };
        // No file either -> None, not Some("").
        assert!(resolve_token(var, None).unwrap().is_none());
        unsafe { std::env::remove_var(var) };
    }

    #[test]
    fn missing_everything_is_none_not_an_error() {
        assert!(
            resolve_token("TRIBUTARY_TOKEN_TEST_ABSENT_VAR", None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn an_empty_token_file_is_a_loud_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty");
        std::fs::write(&path, "\n\n").unwrap();
        assert!(resolve_token("TRIBUTARY_TOKEN_TEST_EMPTY_FILE", Some(&path)).is_err());
    }
}
