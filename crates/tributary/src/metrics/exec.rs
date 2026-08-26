//! The exec metrics collector (#32): run a user command on an interval and
//! ship its line-protocol stdout, the way Telegraf's `inputs.exec` does.
//!
//! `command` is argv, never a shell string, so there is no shell-injection
//! surface — and it runs with the AGENT's privileges (documented). Each run
//! is bounded two ways, because a metrics agent must survive a hung or
//! runaway command. The timeout: the command runs in its OWN process group
//! (unix), and on timeout the whole GROUP is SIGKILLed, not just the child —
//! killing only the child would orphan a `sh -c "..."` wrapper's real work,
//! the trap this collector exists to avoid. The cap: stdout is read up to a
//! fixed cap and the rest DRAINED (so the child never blocks on a full pipe)
//! but discarded, so a command that floods stdout cannot OOM the agent.
//!
//! A non-zero exit ships nothing and logs; it never takes down the collector
//! or a sibling exec.

use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::AsyncReadExt;

use crate::config::Exec;
use crate::queue::Queue;

/// Cap on captured stdout per run. Enough for many thousands of metric lines;
/// a command that exceeds it is truncated (and still drained, so it exits).
const STDOUT_CAP: usize = 1 << 20; // 1 MiB

/// LP tag keys and values escape comma, space and equals with a backslash.
fn escape_tag(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if matches!(c, ',' | ' ' | '=') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Insert the metrics global tags (host + `[metrics.global_tags]`, pre-sorted)
/// into a line-protocol line's tag set. The boundary between `measurement
/// [,tags]` and ` fields` is the FIRST UNESCAPED space (LP backslash-escapes
/// spaces inside the measurement/tag section); the tags go in just before it.
/// A line with no such boundary — no field section, or empty/garbage — is
/// returned VERBATIM rather than mangled.
pub fn inject_tags(line: &str, tags: &[(String, String)]) -> String {
    if tags.is_empty() {
        return line.to_string();
    }
    let mut boundary = None;
    let mut escaped = false;
    for (i, &c) in line.as_bytes().iter().enumerate() {
        if escaped {
            escaped = false;
        } else if c == b'\\' {
            escaped = true;
        } else if c == b' ' {
            boundary = Some(i);
            break;
        }
    }
    let Some(pos) = boundary else {
        return line.to_string();
    };
    let mut out = String::with_capacity(line.len() + 32);
    out.push_str(&line[..pos]);
    for (k, v) in tags {
        out.push(',');
        out.push_str(&escape_tag(k));
        out.push('=');
        out.push_str(&escape_tag(v));
    }
    out.push_str(&line[pos..]);
    out
}

/// Run one exec on its interval until shutdown, pushing to the shared queue.
pub async fn run_exec(
    exec: Exec,
    global_tags: Vec<(String, String)>,
    default_interval: Duration,
    queue: Arc<Mutex<Queue>>,
) {
    let interval = match exec.interval_parsed(default_interval) {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(error = %e, "exec interval invalid; not running");
            return;
        }
    };
    let timeout = exec.timeout_parsed().unwrap_or(Duration::from_secs(5));
    let label = exec.command.first().cloned().unwrap_or_default();
    tracing::info!(
        command = label,
        interval_secs = interval.as_secs(),
        "exec collector started"
    );

    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        match run_once(&exec, timeout).await {
            Ok(Some(stdout)) => {
                let mut lines = String::new();
                for line in stdout.lines() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    lines.push_str(&inject_tags(line, &global_tags));
                    lines.push('\n');
                }
                if !lines.is_empty() {
                    match queue.lock().expect("metrics queue lock").push(&lines) {
                        Ok(true) => {}
                        Ok(false) => {
                            tracing::warn!(
                                command = label,
                                "metrics queue full; exec output dropped"
                            )
                        }
                        Err(e) => tracing::error!(error = %e, "could not queue exec output"),
                    }
                }
            }
            // Non-zero exit or timeout — already logged in run_once.
            Ok(None) => {}
            Err(e) => tracing::warn!(command = label, error = %e, "exec run failed"),
        }
    }
}

/// Spawn the command, capture capped stdout, enforce the timeout with a
/// process-group kill. `Ok(Some(stdout))` on a clean zero exit, `Ok(None)` on
/// a non-zero exit or a timeout, `Err` on a spawn/IO failure.
async fn run_once(exec: &Exec, timeout: Duration) -> anyhow::Result<Option<String>> {
    let mut cmd = tokio::process::Command::new(&exec.command[0]);
    cmd.args(&exec.command[1..]);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::null());
    cmd.kill_on_drop(true);
    // Own process group so the WHOLE tree can be signalled on timeout, not
    // just the direct child (which would orphan a shell wrapper's work).
    #[cfg(unix)]
    cmd.process_group(0);

    let mut child = cmd.spawn()?;
    let pid = child.id().unwrap_or(0);
    let mut stdout = child.stdout.take().expect("stdout is piped");
    let mut buf = Vec::new();

    let result = tokio::time::timeout(timeout, async {
        read_capped(&mut stdout, &mut buf).await;
        child.wait().await
    })
    .await;

    match result {
        Ok(Ok(status)) => {
            if status.success() {
                Ok(Some(String::from_utf8_lossy(&buf).into_owned()))
            } else {
                tracing::warn!(
                    command = exec.command[0],
                    code = status.code(),
                    "exec exited non-zero; shipping nothing this tick"
                );
                Ok(None)
            }
        }
        Ok(Err(e)) => Err(e.into()),
        Err(_elapsed) => {
            tracing::warn!(
                command = exec.command[0],
                timeout_secs = timeout.as_secs(),
                "exec timed out; killing its process group"
            );
            #[cfg(unix)]
            kill_group(pid);
            #[cfg(not(unix))]
            {
                let _ = pid;
                let _ = child.kill().await;
            }
            let _ = child.wait().await; // reap
            Ok(None)
        }
    }
}

/// Read `r` to EOF, keeping only the first [`STDOUT_CAP`] bytes but DRAINING
/// the rest — so a verbose-but-terminating command isn't wedged by a full
/// pipe, and a flooding one is truncated rather than buffered unbounded.
async fn read_capped<R: tokio::io::AsyncRead + Unpin>(r: &mut R, buf: &mut Vec<u8>) {
    let mut chunk = [0u8; 8192];
    loop {
        match r.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if buf.len() < STDOUT_CAP {
                    let take = (STDOUT_CAP - buf.len()).min(n);
                    buf.extend_from_slice(&chunk[..take]);
                }
                // Beyond the cap: read and discarded, so the child keeps going.
            }
        }
    }
}

/// SIGKILL the child's whole process group. The child leads its own group
/// (`process_group(0)` above), so its pgid equals its pid, and the negative
/// pid signals every member — the shell wrapper and whatever it spawned.
#[cfg(unix)]
fn kill_group(pid: u32) {
    if pid == 0 {
        return;
    }
    // SAFETY: kill(2) with a negative pid signals the process group; a stale
    // pid at worst signals nothing (ESRCH), which we ignore.
    unsafe {
        libc::kill(-(pid as i32), libc::SIGKILL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags() -> Vec<(String, String)> {
        // as `Ctx::tags(&[])` produces them: key-sorted, host included.
        vec![
            ("host".to_string(), "h1".to_string()),
            ("region".to_string(), "us".to_string()),
        ]
    }

    #[test]
    fn inject_tags_into_a_line_with_no_tags() {
        assert_eq!(
            inject_tags("myapp value=5", &tags()),
            "myapp,host=h1,region=us value=5"
        );
    }

    #[test]
    fn inject_tags_appends_to_existing_tags() {
        assert_eq!(
            inject_tags("myapp,dc=a value=5", &tags()),
            "myapp,dc=a,host=h1,region=us value=5"
        );
    }

    #[test]
    fn inject_tags_respects_an_escaped_space_in_the_measurement() {
        // The escaped space is part of the measurement; the real boundary is
        // the space before `value`.
        assert_eq!(
            inject_tags(r"my\ app value=5", &[("host".into(), "h1".into())]),
            r"my\ app,host=h1 value=5"
        );
    }

    #[test]
    fn a_line_without_a_field_section_is_shipped_verbatim() {
        // No unescaped space => no clean boundary => don't mangle it.
        assert_eq!(inject_tags("garbage_no_space", &tags()), "garbage_no_space");
        assert_eq!(inject_tags("", &tags()), "");
    }

    #[test]
    fn no_tags_is_a_passthrough() {
        assert_eq!(inject_tags("myapp value=5", &[]), "myapp value=5");
    }

    #[test]
    fn tag_values_with_specials_are_escaped() {
        assert_eq!(
            inject_tags("m v=1", &[("k".into(), "a b,c=d".into())]),
            r"m,k=a\ b\,c\=d v=1"
        );
    }

    // ---- live process tests (unix: sh/sleep + real process groups) ----------

    #[cfg(unix)]
    fn exec(cmd: &str) -> Exec {
        Exec {
            command: vec!["sh".into(), "-c".into(), cmd.into()],
            interval: None,
            timeout: None,
            data_format: None,
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_clean_command_returns_its_stdout() {
        let out = run_once(&exec("echo 'myapp value=5'"), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(out.as_deref(), Some("myapp value=5\n"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_nonzero_exit_ships_nothing() {
        let out = run_once(&exec("echo oops >&2; exit 1"), Duration::from_secs(5))
            .await
            .unwrap();
        assert!(out.is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oversized_stdout_is_capped() {
        // ~2 MiB of 'a'; expect the capture pinned at the cap, not 2 MiB.
        let out = run_once(
            &exec("head -c 2000000 /dev/zero | tr '\\0' a"),
            Duration::from_secs(10),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            out.len() <= STDOUT_CAP,
            "captured {} > cap {}",
            out.len(),
            STDOUT_CAP
        );
        assert!(
            out.len() >= STDOUT_CAP - 8192,
            "should have filled near the cap"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_hung_command_is_killed_at_timeout() {
        let start = std::time::Instant::now();
        let out = run_once(&exec("sleep 60"), Duration::from_secs(1))
            .await
            .unwrap();
        let elapsed = start.elapsed();
        assert!(out.is_none(), "a killed command ships nothing");
        assert!(
            elapsed < Duration::from_secs(5),
            "killed near the 1s timeout, took {elapsed:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_kills_the_whole_process_group_not_just_the_child() {
        // A BACKGROUNDED grandchild that would touch a marker at 2s. If only
        // the child (sh) were killed, the orphan would survive and touch it;
        // a process-GROUP kill removes it, so the marker must never appear.
        let marker = std::env::temp_dir().join(format!("trib-exec-pgroup-{}", std::process::id()));
        let _ = std::fs::remove_file(&marker);
        let script = format!("(sleep 2; touch {}) & wait", marker.display());
        let out = run_once(&exec(&script), Duration::from_secs(1))
            .await
            .unwrap();
        assert!(out.is_none());
        // Wait past when the orphan would have fired.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let survived = marker.exists();
        let _ = std::fs::remove_file(&marker);
        assert!(
            !survived,
            "the backgrounded grandchild survived the group kill"
        );
    }
}
