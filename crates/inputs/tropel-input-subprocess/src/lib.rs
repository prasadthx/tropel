//! # tropel-input-subprocess — Subprocess/JSON input adapter
//!
//! A complement to the WASM plugin tier: run an external process, pipe
//! input bytes on stdin, read a JSON-encoded `Scenario` from stdout.
//!
//! This is the escape hatch for languages/platforms where compiling to
//! WASM is impractical (Java/JMX, Python/Locust, Ruby, .NET, shell
//! scripts). The adapter itself is stateless Rust — the heavy lifting
//! happens in the subprocess.
//!
//! ## Protocol
//!
//! The subprocess command is invoked differently depending on the mode:
//!
//! **Detection:** The adapter calls `cmd --detect` (or `cmd` with
//! environment variable `TROPEL_DETECT=1`). The subprocess reads stdin,
//! writes `true\n` or `false\n` to stdout, and exits 0.
//!
//! **Parsing:** The adapter calls `cmd --parse` (or `cmd` with
//! environment variable `TROPEL_PARSE=1`). The subprocess reads stdin,
//! writes a JSON-encoded `Scenario` to stdout, and exits 0.
//!
//! ## Registration
//!
//! This adapter is **factory-only**: it takes a runtime argument — the
//! command to run — so it cannot be a compile-time `inventory::submit!`
//! registration. The CLI registers one factory per `--subprocess-adapter
//! <cmd>` via `ExtensionRegistry::register_adapter_factory` under the id
//! `subprocess:<cmd>`. There is deliberately **no** static registration:
//! a placeholder would be probed during content auto-detection on every
//! run (spawning a bogus `echo`) and listed as a real format.
//!
//! ## Safety
//!
//! The subprocess runs with the same privileges as the tropel process.
//! The command is configured by the user (via `--subprocess-adapter`),
//! so the user is responsible for trusting the command they specify.
//! Each call is bounded by a timeout (default 30s) and an output-size cap
//! (default 16 MiB) so a hanging or chatty subprocess can't stall or OOM
//! the host.

use std::collections::HashMap;

use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use tropel_sdk::InputAdapter;
use tropel_sdk::{Result, TropelError};
use tropel_sdk::{Scenario, ScenarioInfo};

/// Default per-call timeout for the subprocess. A child that outlives this
/// is killed (DoS guard — a hanging adapter must not hang the host).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
/// Cap on how many bytes we read from the subprocess stdout. Prevents a
/// misbehaving adapter from exhausting host memory.
const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;

/// A subprocess-based input adapter.
///
/// Created with a command string (e.g. `"python3 my-adapter.py"`).
/// The adapter calls the command for each `detect()` and `parse()` call.
#[derive(Debug)]
pub struct SubprocessAdapter {
    /// The command to run (e.g. "python3 my-adapter.py").
    command: String,
    /// Parsed command parts for spawning.
    program: String,
    args: Vec<String>,
    /// Per-call timeout; the child is killed when it expires.
    timeout: Duration,
    /// Max stdout bytes accepted per call.
    max_output: usize,
}

impl SubprocessAdapter {
    /// Create a new subprocess adapter for the given command.
    ///
    /// The command string is split into program and arguments using
    /// simple whitespace splitting (no shell parsing). For complex
    /// commands, wrap in a shell script.
    ///
    /// Returns a [`TropelError`] for an empty or whitespace-only command
    /// (`--subprocess-adapter ""`): the old `parts[1..]` slicing panicked on
    /// the empty split, which was reachable straight from the CLI.
    pub fn new(command: &str) -> Result<Self> {
        if command.trim().is_empty() {
            return Err(TropelError::Other(
                "subprocess adapter command cannot be empty".to_string(),
            ));
        }
        let parts: Vec<&str> = command.split_whitespace().collect();
        // Non-empty trim guarantees ≥1 part; skip(1) is panic-free regardless.
        let program = parts.first().unwrap().to_string();
        let args: Vec<String> = parts.iter().skip(1).map(|s| s.to_string()).collect();

        Ok(Self {
            command: command.to_string(),
            program,
            args,
            timeout: DEFAULT_TIMEOUT,
            max_output: MAX_OUTPUT_BYTES,
        })
    }

    /// Set the per-call timeout. The child is killed when it expires.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the per-call stdout byte cap (default 16 MiB).
    pub fn with_max_output(mut self, max_output: usize) -> Self {
        self.max_output = max_output;
        self
    }

    /// Run the subprocess with the given mode flag and input bytes.
    ///
    /// I/O is **concurrent**: stdin is fed by a dedicated writer thread and
    /// stdout is drained by a dedicated reader thread, while the caller
    /// waits on a channel. The old implementation did `write_all(stdin)`
    /// before reading stdout — with a large payload the child's stdout pipe
    /// fills while it's still reading stdin, and the parent's blocked
    /// `write_all` meets the child's blocked stdout write: a classic pipe
    /// deadlock.
    ///
    /// The call is bounded by [`Self::timeout`]: the caller blocks on
    /// `recv_timeout`, so even a **silent** child that merely holds stdout
    /// open (e.g. `sleep 60`) is killed on expiry — a deadline checked only
    /// *between* blocking reads would never fire for such a child. Stdout is
    /// also capped at [`Self::max_output`] bytes.
    fn run(&self, flag: &str, env_var: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        let mut cmd = Command::new(&self.program);
        cmd.args(&self.args)
            .arg(flag)
            .env(env_var, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        let mut child = cmd.spawn().map_err(|e| {
            TropelError::Other(format!(
                "Failed to spawn subprocess adapter '{}': {}. Is '{}' installed and on PATH?",
                self.command, e, self.program
            ))
        })?;

        // Take both pipes BEFORE spawning any helper thread, so no helper
        // can leak if a `take()` fails.
        let mut stdin = child.stdin.take().ok_or_else(|| {
            TropelError::Other(format!("Subprocess '{}' stdin unavailable", self.command))
        })?;
        let mut stdout = child.stdout.take().ok_or_else(|| {
            TropelError::Other(format!("Subprocess '{}' stdout unavailable", self.command))
        })?;

        // Hard overall budget for the whole call (spawn → child exit).
        let deadline = Instant::now() + self.timeout;
        let remaining = || deadline.saturating_duration_since(Instant::now());

        // Writer thread: feed stdin so the child can start emitting stdout
        // before it has drained stdin (no pipe deadlock on large I/O).
        let input = bytes.to_vec();
        let writer = thread::spawn(move || {
            if let Err(e) = stdin.write_all(&input) {
                // Broken pipe is normal when the child exits early or
                // ignores stdin — not an adapter failure.
                tracing::debug!("Subprocess stdin write failed: {}", e);
            }
        });

        // Reader thread: drain stdout (with a byte cap) and send the result
        // over a channel. Doing the read off the caller thread is what makes
        // the timeout real: `recv_timeout` fires even if the child writes
        // nothing and merely keeps the pipe open.
        let max_output = self.max_output;
        let command = self.command.clone();
        let (tx, rx) = mpsc::channel::<Result<Vec<u8>>>();
        let reader = thread::spawn(move || {
            let mut output = Vec::new();
            let mut chunk = [0u8; 8192];
            let result = loop {
                match stdout.read(&mut chunk) {
                    Ok(0) => break Ok(output), // EOF
                    Ok(n) => {
                        if output.len() + n > max_output {
                            break Err(TropelError::Other(format!(
                                "Subprocess '{}' output exceeded {} bytes",
                                command, max_output
                            )));
                        }
                        output.extend_from_slice(&chunk[..n]);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => {
                        break Err(TropelError::Other(format!(
                            "Failed to read subprocess '{}' stdout: {}",
                            command, e
                        )))
                    }
                }
            };
            let _ = tx.send(result);
        });

        // Wait for the reader with a hard timeout. The reader thread may
        // still be blocked in `read` when we time out — killing the child
        // closes the pipe, which unblocks it so we can join cleanly.
        // NOTE: `child.kill()` only terminates the DIRECT child process. If
        // the configured command spawns long-lived grandchildren (a shell
        // wrapper that does not `exec` its payload, or a fork-emulated exec
        // on MSYS/Windows), those can keep the pipe open and delay the join
        // until they exit; adapter commands should avoid such wrappers.
        let read_outcome = match rx.recv_timeout(remaining()) {
            Ok(res) => res,
            Err(mpsc::RecvTimeoutError::Timeout) => Err(TropelError::Other(format!(
                "Subprocess '{}' timed out after {:?}",
                self.command, self.timeout
            ))),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(TropelError::Other(format!(
                "Subprocess '{}' reader thread terminated unexpectedly",
                self.command
            ))),
        };

        match read_outcome {
            Err(e) => {
                // Timeout / cap / I/O failure: stop the child, join both
                // helper threads, and surface the error.
                kill_and_join(&mut child, writer, reader);
                Err(e)
            }
            Ok(output) => {
                // Normal EOF: reap the child within the remaining budget.
                let status = loop {
                    match child.try_wait() {
                        Ok(Some(status)) => break status,
                        Ok(None) => {
                            if Instant::now() >= deadline {
                                kill_and_join(&mut child, writer, reader);
                                return Err(TropelError::Other(format!(
                                    "Subprocess '{}' timed out after {:?}",
                                    self.command, self.timeout
                                )));
                            }
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(e) => {
                            kill_and_join(&mut child, writer, reader);
                            return Err(TropelError::Other(format!(
                                "Failed to wait for subprocess '{}': {}",
                                self.command, e
                            )));
                        }
                    }
                };
                let _ = writer.join();
                let _ = reader.join();

                if !status.success() {
                    return Err(TropelError::Other(format!(
                        "Subprocess '{}' exited with {}",
                        self.command, status
                    )));
                }

                Ok(output)
            }
        }
    }
}

/// Kill the child, reap it, and join both helper threads. Consumes the
/// handles so it can be called from any error path without borrow issues.
fn kill_and_join(
    child: &mut std::process::Child,
    writer: std::thread::JoinHandle<()>,
    reader: std::thread::JoinHandle<()>,
) {
    let _ = child.kill();
    let _ = child.wait();
    let _ = writer.join();
    let _ = reader.join();
}

impl InputAdapter for SubprocessAdapter {
    fn id(&self) -> &str {
        // Derive a stable ID from the command.
        // The registry key (set by the CLI) is `subprocess:<command>`; the
        // adapter's own id is the command itself. No phantom "subprocess"
        // id pointing at an "echo" adapter — factory-only registration.
        &self.command
    }

    fn detect(&self, bytes: &[u8]) -> bool {
        match self.run("--detect", "TROPEL_DETECT", bytes) {
            Ok(output) => {
                let text = String::from_utf8_lossy(&output);
                text.trim().eq_ignore_ascii_case("true")
                    || text.trim() == "1"
                    || text.trim() == "yes"
            }
            Err(e) => {
                tracing::warn!("Subprocess adapter detect failed: {}", e);
                false
            }
        }
    }

    fn parse(&self, bytes: &[u8]) -> Result<Scenario> {
        let output = self.run("--parse", "TROPEL_PARSE", bytes)?;

        let raw_scenario: serde_json::Value = serde_json::from_slice(&output).map_err(|e| {
            TropelError::Parse(format!(
                "Subprocess '{}' returned invalid JSON: {}. Raw output: {}",
                self.command,
                e,
                String::from_utf8_lossy(&output[..output.len().min(200)])
            ))
        })?;

        // Accept either a full Scenario or an array of items
        let scenario = if raw_scenario.get("info").is_some() || raw_scenario.get("items").is_some()
        {
            serde_json::from_value::<Scenario>(raw_scenario).map_err(|e| {
                TropelError::Parse(format!(
                    "Subprocess '{}' returned invalid Scenario: {}",
                    self.command, e
                ))
            })?
        } else if let Some(items) = raw_scenario.as_array() {
            // Treat a JSON array as items, auto-generate a name
            Scenario {
                info: ScenarioInfo {
                    name: format!("subprocess-{}", self.command),
                    description: Some(format!(
                        "Imported via subprocess adapter '{}'",
                        self.command
                    )),
                    schema: None,
                },
                items: items
                    .iter()
                    .map(|v| {
                        serde_json::from_value(v.clone()).unwrap_or_else(|_| {
                            tropel_sdk::ScenarioItem {
                                name: "Imported item".to_string(),
                                request: None,
                                prerequest: None,
                                test: None,
                                assertions: vec![],
                                items: vec![],
                            }
                        })
                    })
                    .collect(),
                variables: HashMap::new(),
                auth: None,
            }
        } else {
            return Err(TropelError::Parse(format!(
                "Subprocess '{}' returned JSON that is neither a Scenario nor an array of items. Got: {}",
                self.command,
                String::from_utf8_lossy(&output[..output.len().min(200)])
            )));
        };

        Ok(scenario)
    }

    fn parse_with_path(&self, bytes: &[u8], _source_path: Option<&Path>) -> Result<Scenario> {
        // The subprocess adapter doesn't need the file path — bytes are everything
        self.parse(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_adapter_splits_command() {
        let adapter = SubprocessAdapter::new("python3 my-adapter.py").unwrap();
        assert_eq!(adapter.program, "python3");
        assert_eq!(adapter.args, vec!["my-adapter.py"]);
    }

    #[test]
    fn test_new_adapter_simple() {
        let adapter = SubprocessAdapter::new("cat").unwrap();
        assert_eq!(adapter.program, "cat");
        assert!(adapter.args.is_empty());
    }

    #[test]
    fn test_new_adapter_complex() {
        let adapter = SubprocessAdapter::new("node /path/to/adapter.js --verbose").unwrap();
        assert_eq!(adapter.program, "node");
        assert_eq!(adapter.args, vec!["/path/to/adapter.js", "--verbose"]);
    }

    #[test]
    fn test_new_rejects_empty_command() {
        // Regression: `--subprocess-adapter ""` panicked on `parts[1..]`
        // (empty split slice). Must return a TropelError, not panic.
        for cmd in ["", "   ", "\t\n"] {
            let err = SubprocessAdapter::new(cmd).unwrap_err();
            let msg = format!("{}", err);
            assert!(
                msg.contains("cannot be empty"),
                "Expected empty-command error, got: {}",
                msg
            );
        }
    }

    #[test]
    fn test_id_is_command() {
        let adapter = SubprocessAdapter::new("python3 my-adapter.py").unwrap();
        assert_eq!(adapter.id(), "python3 my-adapter.py");
    }

    #[test]
    fn test_detect_fails_for_nonexistent_command() {
        let adapter = SubprocessAdapter::new("this-command-does-not-exist-hopefully").unwrap();
        // Should return false (not crash)
        assert!(!adapter.detect(b"hello"));
    }

    #[test]
    fn test_parse_fails_for_nonexistent_command() {
        let adapter = SubprocessAdapter::new("this-command-does-not-exist-hopefully").unwrap();
        let result = adapter.parse(b"hello");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("Failed to spawn"),
            "Expected spawn error, got: {}",
            msg
        );
    }

    #[test]
    fn test_parse_with_cat_returns_error_for_non_json() {
        let adapter = SubprocessAdapter::new("cat").unwrap();
        // cat echoes stdin to stdout — that won't be valid JSON
        let result = adapter.parse(br#"not json"#);
        assert!(result.is_err());
    }

    /// Write a small `sh` script to a per-test temp dir and return an
    /// adapter command that runs it. The adapter appends `--parse`/`--detect`
    /// to the command; these scripts **ignore** that argument, so we can test
    /// echo/timeout behaviour with plain tools (`cat`, `sleep`) that would
    /// otherwise reject the unknown flag and exit 1.
    fn script_adapter(name: &str, body: &str) -> SubprocessAdapter {
        // Per-test tag in the dir name (not just pid) so parallel tests in
        // the same process don't clobber each other's scripts (backlog 209).
        let dir = std::env::temp_dir().join(format!(
            "tropel-sub-tests-{}-{name}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join(name);
        std::fs::write(&path, body).expect("write script");
        let cmd = format!("sh {}", path.to_string_lossy().replace('\\', "/"));
        SubprocessAdapter::new(&cmd).unwrap()
    }

    #[test]
    fn test_large_io_no_deadlock() {
        // Regression: the old code wrote ALL stdin before reading stdout.
        // With a >pipe-buffer payload, the child's stdout pipe filled while
        // the parent was still blocked in write_all → deadlock. The writer
        // thread makes this safe; 1 MiB through `cat` must complete quickly.
        let adapter = script_adapter("echo1.sh", "#!/bin/sh\nexec cat\n");
        let start = std::time::Instant::now();
        let payload = vec![b'x'; 1024 * 1024];
        let result = adapter.parse(&payload);
        // The script echoes the bytes back; not valid JSON, but crucially it
        // must return an error (JSON parse) rather than hang.
        assert!(result.is_err(), "large I/O must not deadlock");
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("invalid JSON"), "unexpected error: {}", msg);
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "1 MiB echo must not deadlock (took {:?})",
            start.elapsed()
        );
    }

    #[test]
    fn test_timeout_kills_silent_subprocess() {
        // A child that never writes stdout and never exits must be killed by
        // the timeout. `sleep 60` holds the pipe open but is silent — the
        // reader thread + `recv_timeout` is what makes this terminate.
        // The script must NOT spawn a long-lived subprocess: `child.kill()`
        // only terminates the direct child, so if `sleep` were a grandchild
        // it would keep the stdout pipe open and the test would hang its
        // full duration (on MSYS even `exec sleep` is fork-emulated). A busy
        // loop runs inside `sh` itself — killing `sh` closes the pipe and
        // unblocks the reader thread promptly.
        let adapter = script_adapter("busy.sh", "#!/bin/sh\nwhile :; do :; done\n")
            .with_timeout(Duration::from_millis(300));
        let start = std::time::Instant::now();
        let result = adapter.parse(b"hello");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("timed out"),
            "expected timeout error, got: {}",
            msg
        );
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "timeout must kill the child promptly (took {:?})",
            start.elapsed()
        );
    }

    #[test]
    fn test_output_cap_limits_chatty_subprocess() {
        // A child that emits more than the cap must be stopped with an error.
        // The echo script returns its 1 MiB input on stdout; a 4 KiB cap trips.
        let adapter = script_adapter("echo2.sh", "#!/bin/sh\nexec cat\n").with_max_output(4 * 1024);
        let payload = vec![b'x'; 1024 * 1024];
        let result = adapter.parse(&payload);
        assert!(result.is_err(), "output cap must reject chatty subprocess");
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("exceeded"), "expected cap error, got: {}", msg);
    }
}
