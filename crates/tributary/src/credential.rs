//! Client identity for mTLS (L4).
//!
//! TimeLakeDB runs client-certificate verification in **want** mode: a
//! caller that presents no certificate is served exactly as before, and one
//! that presents a certificate has it verified against the configured CA.
//! Tributary is the caller that presents one.
//!
//! The discipline here is deliberately the same as the server's
//! `timelake-tls`, because SEC-3 assumes ~24 h certificates and a renewal
//! lands while the agent is shipping:
//!
//! * **validate before swap** — a replacement pair is parsed, checked for
//!   key↔certificate consistency and rejected if already expired, all before
//!   anything adopts it;
//! * **last-good on a bad renewal** — a replacement that fails any of that
//!   is refused and the previous identity keeps shipping, with a named
//!   alarm rather than a silent outage;
//! * **atomic adoption** — the live HTTP client is swapped in one store, so
//!   an in-flight batch never sees a half-applied rotation.
//!
//! The two repositories cannot share `timelake-tls` itself — nothing is
//! published, and Tributary is its own workspace — so they share the rules
//! and the dependency versions instead. Where this differs from the server
//! is the shape of the artifact: rustls hands the server a `CertifiedKey`,
//! while reqwest wants PEM bytes, so [`Identity`] carries the validated PEM
//! rather than a parsed key.
//!
//! ROADMAP §4 sketches this as a `CredentialSource` trait whose first two
//! backends are files and Vault PKI. [`FileCredentials`] is the files
//! backend; the trait is the seam Vault slots into without touching
//! `ship.rs`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// A validated client identity, ready to hand to reqwest.
pub struct Identity {
    /// Certificate chain and private key concatenated, which is the form
    /// `reqwest::Identity::from_pem` accepts.
    pem: Vec<u8>,
    /// Leaf expiry, epoch seconds. Drives the renewal alarm.
    pub not_after_epoch: i64,
    /// The leaf's subject common name — the identity the server reads out
    /// of a verified chain, and what a principal's grants are matched on.
    pub common_name: Option<String>,
}

impl Identity {
    pub fn reqwest_identity(&self) -> Result<reqwest::Identity, CredentialError> {
        reqwest::Identity::from_pem(&self.pem)
            .map_err(|e| CredentialError::Invalid(format!("reqwest rejected the pair: {e}")))
    }

    pub fn expires_in_secs(&self) -> i64 {
        self.not_after_epoch - epoch_now()
    }
}

impl std::fmt::Debug for Identity {
    /// Never prints `pem` — it carries the private key.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Identity")
            .field("common_name", &self.common_name)
            .field("not_after_epoch", &self.not_after_epoch)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub enum CredentialError {
    Io(std::io::Error),
    Invalid(String),
    Missing(&'static str),
    Expired { not_after_epoch: i64 },
}

impl std::fmt::Display for CredentialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CredentialError::Io(e) => write!(f, "reading credential: {e}"),
            CredentialError::Invalid(m) => write!(f, "invalid credential: {m}"),
            CredentialError::Missing(what) => write!(f, "credential is missing a {what}"),
            CredentialError::Expired { not_after_epoch } => write!(
                f,
                "certificate already expired (notAfter {not_after_epoch}); refusing to adopt it"
            ),
        }
    }
}

impl std::error::Error for CredentialError {}

impl From<std::io::Error> for CredentialError {
    fn from(e: std::io::Error) -> Self {
        CredentialError::Io(e)
    }
}

/// Where the client identity comes from (ROADMAP §4).
///
/// v1 is files on disk; Vault PKI and SPIFFE are later backends behind this
/// same seam. Implementors must validate before returning — a source that
/// hands back an unusable pair defeats the whole point of the gate.
pub trait CredentialSource: Send + Sync {
    /// Read and fully validate the current material.
    fn load(&self) -> Result<Identity, CredentialError>;

    /// A cheap change detector, so a rotation check that finds nothing new
    /// costs a stat rather than a parse. `None` means "cannot tell — always
    /// re-read".
    fn changed_token(&self) -> Option<CredentialToken>;
}

// The ROADMAP sketch also gives this trait a `refresh_before() -> Duration`,
// for a source that must request a renewal ahead of expiry. It is not here
// yet, deliberately: the files backend does not request anything — something
// else writes the file and the agent notices — so the cadence is the
// operator's `[output.tls].refresh_secs`, not the source's opinion. Vault
// PKI is the backend that actually needs it, and it should arrive with that
// backend rather than sit here as an uncalled method that the next reader
// has to work out is unused.

/// Opaque change marker (file mtimes for the files backend).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialToken(pub Vec<(PathBuf, SystemTime)>);

/// The files backend: a certificate and key on disk, re-read on rotation.
pub struct FileCredentials {
    cert_path: PathBuf,
    key_path: PathBuf,
}

impl FileCredentials {
    pub fn new(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> FileCredentials {
        FileCredentials {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        }
    }
}

impl CredentialSource for FileCredentials {
    fn load(&self) -> Result<Identity, CredentialError> {
        load_pair(&self.cert_path, &self.key_path)
    }

    fn changed_token(&self) -> Option<CredentialToken> {
        let mut out = Vec::with_capacity(2);
        for p in [&self.cert_path, &self.key_path] {
            let m = std::fs::metadata(p).ok()?.modified().ok()?;
            out.push((p.clone(), m));
        }
        Some(CredentialToken(out))
    }
}

/// Read and fully validate a PEM certificate chain and key. Nothing is
/// adopted unless every check passes — this is the validate-before-swap
/// gate, and it mirrors `timelake-tls`'s `load_pair` check for check.
pub fn load_pair(cert_path: &Path, key_path: &Path) -> Result<Identity, CredentialError> {
    let cert_pem = std::fs::read(cert_path)?;
    let key_pem = std::fs::read(key_path)?;

    let certs: Vec<_> = rustls_pemfile::certs(&mut &cert_pem[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| CredentialError::Invalid(format!("certificate PEM: {e}")))?;
    if certs.is_empty() {
        return Err(CredentialError::Missing("certificate"));
    }
    // Parsed only to prove the key is present and well-formed; the bytes
    // handed to reqwest are the original PEM.
    let key = rustls_pemfile::private_key(&mut &key_pem[..])
        .map_err(|e| CredentialError::Invalid(format!("private key PEM: {e}")))?;
    if key.is_none() {
        return Err(CredentialError::Missing("private key"));
    }

    // Leaf expiry: refuse an already-expired renewal outright rather than
    // discovering it as a handshake failure mid-batch.
    let (_, parsed) = x509_parser::parse_x509_certificate(certs[0].as_ref())
        .map_err(|e| CredentialError::Invalid(format!("x509 parse: {e}")))?;
    let not_after_epoch = parsed.validity().not_after.timestamp();
    if not_after_epoch <= epoch_now() {
        return Err(CredentialError::Expired { not_after_epoch });
    }
    let common_name = parsed
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(str::to_string);

    // Certificate and key concatenated is what reqwest's PEM identity
    // wants, and building it here is also the key↔certificate consistency
    // check — the counterpart of the server's `CertifiedKey::from_der`.
    let mut pem = cert_pem.clone();
    if !pem.ends_with(b"\n") {
        pem.push(b'\n');
    }
    pem.extend_from_slice(&key_pem);

    let identity = Identity {
        pem,
        not_after_epoch,
        common_name,
    };

    // Build the thing we are actually going to use, not just the parts.
    //
    // `reqwest::Identity::from_pem` parses; it does NOT check that the key
    // belongs to the certificate — a test that paired node-a's certificate
    // with node-b's key sailed straight through it. The consistency check
    // happens when rustls assembles a client config, i.e. at
    // `ClientBuilder::build()`. So the gate builds a throwaway client here:
    // if the pair cannot produce a working client now, it must not replace
    // one that works, and finding out at the next handshake instead would be
    // exactly the silent swap-to-broken this gate exists to stop.
    let probe = identity.reqwest_identity()?;
    reqwest::Client::builder()
        .identity(probe)
        .build()
        .map_err(|e| CredentialError::Invalid(format!("certificate and key do not match: {e}")))?;

    Ok(identity)
}

/// Read and validate a CA bundle used to verify the *server*. Separate from
/// the client identity: trusting the server's issuer and proving who we are
/// answer different questions, and a deployment can need one without the
/// other (Telegraf's TLS config does exactly that — CA only, no client
/// certificate).
pub fn load_ca_bundle(ca_path: &Path) -> Result<Vec<reqwest::Certificate>, CredentialError> {
    let pem = std::fs::read(ca_path)?;
    let ders: Vec<_> = rustls_pemfile::certs(&mut &pem[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| CredentialError::Invalid(format!("CA PEM: {e}")))?;
    if ders.is_empty() {
        return Err(CredentialError::Missing("CA certificate"));
    }
    // A bundle, not a single certificate: dual-CA overlap is how the server
    // rotates its trust anchors without a flag day, and the client has to
    // accept the same overlap or it breaks at exactly the wrong moment.
    ders.iter()
        .map(|d| {
            reqwest::Certificate::from_der(d.as_ref())
                .map_err(|e| CredentialError::Invalid(format!("CA certificate: {e}")))
        })
        .collect()
}

fn epoch_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// The live identity, swapped atomically when a renewal validates.
///
/// `current` is `None` only when no client certificate is configured at all;
/// once one is adopted it is never removed by a failed reload — that is the
/// last-good rule.
pub struct RotatingIdentity {
    /// `None` for the CA-only deployment: trust a private issuer without
    /// presenting an identity. Every rotation call is then a no-op rather
    /// than a special case at each site.
    source: Option<Box<dyn CredentialSource>>,
    current: arc_swap::ArcSwapOption<Identity>,
    token: std::sync::Mutex<Option<CredentialToken>>,
    /// False after a renewal was refused, until one succeeds. Surfaces as
    /// the alarm an operator watches; a silently stale certificate is the
    /// failure this exists to prevent.
    last_reload_ok: std::sync::atomic::AtomicBool,
    reloads_refused: std::sync::atomic::AtomicU64,
}

impl RotatingIdentity {
    /// Load once at startup. Unlike a reload, this fails hard: there is no
    /// last-good pair to fall back to, and starting without the identity the
    /// operator asked for would be a silent downgrade to anonymous.
    pub fn load(
        source: Box<dyn CredentialSource>,
    ) -> Result<Arc<RotatingIdentity>, CredentialError> {
        let identity = source.load()?;
        let token = source.changed_token();
        Ok(Arc::new(RotatingIdentity {
            source: Some(source),
            current: arc_swap::ArcSwapOption::from_pointee(identity),
            token: std::sync::Mutex::new(token),
            last_reload_ok: std::sync::atomic::AtomicBool::new(true),
            reloads_refused: std::sync::atomic::AtomicU64::new(0),
        }))
    }

    /// No client certificate: the CA-only deployment. Reloading is a no-op,
    /// so the shipping path needs no branch for it.
    pub fn none() -> Arc<RotatingIdentity> {
        Arc::new(RotatingIdentity {
            source: None,
            current: arc_swap::ArcSwapOption::empty(),
            token: std::sync::Mutex::new(None),
            last_reload_ok: std::sync::atomic::AtomicBool::new(true),
            reloads_refused: std::sync::atomic::AtomicU64::new(0),
        })
    }

    pub fn current(&self) -> Option<Arc<Identity>> {
        self.current.load_full()
    }

    pub fn last_reload_ok(&self) -> bool {
        self.last_reload_ok
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn reloads_refused(&self) -> u64 {
        self.reloads_refused
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn expires_in_secs(&self) -> Option<i64> {
        self.current().map(|i| i.expires_in_secs())
    }

    /// Re-read the source if it looks changed, and adopt the result only if
    /// it validates. Returns `Ok(true)` when a new identity was adopted.
    ///
    /// On failure the previous identity keeps shipping and the error is
    /// returned for the caller to log — never propagated as a shipping
    /// failure, because a bad renewal must not stop a working agent.
    pub fn reload(&self) -> Result<bool, CredentialError> {
        let Some(source) = &self.source else {
            return Ok(false); // CA-only: nothing to rotate
        };
        let fresh_token = source.changed_token();
        {
            let seen = self.token.lock().expect("credential token lock");
            if fresh_token.is_some() && *seen == fresh_token {
                return Ok(false); // nothing changed on disk
            }
        }

        match source.load() {
            Ok(identity) => {
                self.current.store(Some(Arc::new(identity)));
                *self.token.lock().expect("credential token lock") = fresh_token;
                self.last_reload_ok
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                Ok(true)
            }
            Err(e) => {
                self.last_reload_ok
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                self.reloads_refused
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // Deliberately do NOT update the token: the next tick should
                // try again rather than treat the bad file as seen.
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// A CA-signed client pair, minted with openssl the way the drill does.
    /// Skipped when openssl is unavailable, so the suite still runs on a box
    /// without it rather than failing for an unrelated reason.
    fn mint(dir: &Path, cn: &str, days: &str) -> Option<(PathBuf, PathBuf)> {
        let ca_key = dir.join("ca.key");
        let ca_crt = dir.join("ca.crt");
        let key = dir.join(format!("{cn}.key"));
        let csr = dir.join(format!("{cn}.csr"));
        let crt = dir.join(format!("{cn}.crt"));

        let run = |args: &[&str]| -> bool {
            std::process::Command::new("openssl")
                .args(args)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
        };
        if !run(&["version"]) {
            return None;
        }
        let ok = run(&[
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:P-256",
            "-nodes",
            "-keyout",
            ca_key.to_str()?,
            "-out",
            ca_crt.to_str()?,
            "-days",
            "10",
            "-subj",
            "/CN=tributary-test-ca",
        ]) && run(&[
            "req",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:P-256",
            "-nodes",
            "-keyout",
            key.to_str()?,
            "-out",
            csr.to_str()?,
            "-subj",
            &format!("/CN={cn}"),
        ]) && run(&[
            "x509",
            "-req",
            "-in",
            csr.to_str()?,
            "-CA",
            ca_crt.to_str()?,
            "-CAkey",
            ca_key.to_str()?,
            "-CAcreateserial",
            "-out",
            crt.to_str()?,
            "-days",
            days,
        ]);
        ok.then_some((crt, key))
    }

    #[test]
    fn a_valid_pair_loads_and_carries_its_common_name() {
        let dir = tempfile::tempdir().unwrap();
        let Some((crt, key)) = mint(dir.path(), "tributary-node-1", "1") else {
            eprintln!("openssl unavailable — skipping");
            return;
        };
        let id = load_pair(&crt, &key).expect("a freshly minted pair must load");
        assert_eq!(id.common_name.as_deref(), Some("tributary-node-1"));
        assert!(id.expires_in_secs() > 0);
        // The identity reqwest will actually use must build.
        id.reqwest_identity().expect("reqwest must accept the pair");
    }

    #[test]
    fn a_mismatched_key_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let Some((crt, _key)) = mint(dir.path(), "node-a", "1") else {
            eprintln!("openssl unavailable — skipping");
            return;
        };
        let Some((_crt_b, key_b)) = mint(dir.path(), "node-b", "1") else {
            return;
        };
        // node-a's certificate with node-b's key: the consistency check is
        // the whole point of validating before swapping.
        let err = load_pair(&crt, &key_b).expect_err("a mismatched pair must be refused");
        assert!(
            matches!(err, CredentialError::Invalid(_)),
            "expected an Invalid pair, got {err:?}"
        );
    }

    #[test]
    fn a_corrupt_certificate_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let crt = dir.path().join("bad.crt");
        let key = dir.path().join("bad.key");
        std::fs::write(&crt, b"-----BEGIN CERTIFICATE-----\nnot base64\n").unwrap();
        std::fs::write(&key, b"-----BEGIN PRIVATE KEY-----\nalso not\n").unwrap();
        assert!(load_pair(&crt, &key).is_err());
    }

    #[test]
    fn a_missing_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let err = load_pair(&dir.path().join("nope.crt"), &dir.path().join("nope.key"))
            .expect_err("a missing file must not load");
        assert!(matches!(err, CredentialError::Io(_)));
    }

    /// The rule SEC-3 leans on: a bad renewal must never take the agent's
    /// working identity away. This is the client-side mirror of the server's
    /// `corrupt_renewal_keeps_last_good_then_recovers`.
    #[test]
    fn a_bad_renewal_keeps_the_last_good_identity_then_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let Some((crt, key)) = mint(dir.path(), "tributary-node-1", "1") else {
            eprintln!("openssl unavailable — skipping");
            return;
        };
        // Copy to stable paths the "renewal" will overwrite.
        let live_crt = dir.path().join("live.crt");
        let live_key = dir.path().join("live.key");
        std::fs::copy(&crt, &live_crt).unwrap();
        std::fs::copy(&key, &live_key).unwrap();

        let rot = RotatingIdentity::load(Box::new(FileCredentials::new(&live_crt, &live_key)))
            .expect("initial load");
        let first = rot.current().expect("an identity after load");
        assert!(rot.last_reload_ok());

        // A corrupt renewal lands on disk.
        std::thread::sleep(std::time::Duration::from_millis(1100)); // mtime granularity
        let mut f = std::fs::File::create(&live_crt).unwrap();
        f.write_all(b"-----BEGIN CERTIFICATE-----\ngarbage\n")
            .unwrap();
        drop(f);

        let err = rot.reload().expect_err("a corrupt renewal must be refused");
        assert!(matches!(
            err,
            CredentialError::Invalid(_) | CredentialError::Missing(_)
        ));
        assert!(!rot.last_reload_ok(), "the alarm must be raised");
        assert_eq!(rot.reloads_refused(), 1);

        // ...and the agent is still holding the working identity.
        let still = rot.current().expect("last-good identity still present");
        assert_eq!(still.not_after_epoch, first.not_after_epoch);
        assert_eq!(still.common_name, first.common_name);

        // A good renewal recovers, and clears the alarm.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        std::fs::copy(&crt, &live_crt).unwrap();
        assert!(rot.reload().expect("a good renewal must be adopted"));
        assert!(rot.last_reload_ok(), "the alarm must clear on recovery");
    }

    #[test]
    fn an_unchanged_source_is_not_reparsed() {
        let dir = tempfile::tempdir().unwrap();
        let Some((crt, key)) = mint(dir.path(), "tributary-node-1", "1") else {
            eprintln!("openssl unavailable — skipping");
            return;
        };
        let rot = RotatingIdentity::load(Box::new(FileCredentials::new(&crt, &key))).unwrap();
        assert!(
            !rot.reload().expect("an unchanged reload must succeed"),
            "nothing changed on disk, so nothing should be adopted"
        );
    }
}
