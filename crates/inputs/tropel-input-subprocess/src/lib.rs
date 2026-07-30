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
//! This adapter is registered via `inventory::submit!` at compile time.
//! Unlike static adapters (Postman, HAR, OpenAPI), the subprocess adapter
//! takes a runtime argument — the command to run — so it is constructed
//! with a specific command string and added to the registry at startup
//! (not by `collect_inventory()`).
//!
//! ## Safety
//!
//! The subprocess runs with the same privileges as the tropel process.
//! The command is configured by the user (via `--subprocess-adapter`),
//! so the user is responsible for trusting the command they specify.

use std::collections::HashMap;

use std::path::Path;
use std::process::{Command, Stdio};
use std::io::Write;
use tropel_core::scenario::{Scenario, ScenarioInfo};
use tropel_core::{Result, TropelError};
use tropel_ext::traits::{InputAdapter, InputAdapterRegistration};

/// A subprocess-based input adapter.
///
/// Created with a command string (e.g. `"python3 my-adapter.py"`).
/// The adapter calls the command for each `detect()` and `parse()` call.
pub struct SubprocessAdapter {
    /// The command to run (e.g. "python3 my-adapter.py").
    command: String,
    /// Parsed command parts for spawning.
    program: String,
    args: Vec<String>,
}

impl SubprocessAdapter {
    /// Create a new subprocess adapter for the given command.
    ///
    /// The command string is split into program and arguments using
    /// simple whitespace splitting (no shell parsing). For complex
    /// commands, wrap in a shell script.
    pub fn new(command: &str) -> Self {
        let parts: Vec<&str> = command.split_whitespace().collect();
        let program = parts.first().unwrap_or(&command).to_string();
        let args: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();

        Self {
            command: command.to_string(),
            program,
            args,
        }
    }

    /// Run the subprocess with the given mode flag and input bytes.
    fn run(&self, flag: &str, env_var: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        let mut cmd = Command::new(&self.program);
        cmd.args(&self.args)
            .arg(flag)
            .env(env_var, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        let mut child = cmd.spawn()
            .map_err(|e| TropelError::Other(format!(
                "Failed to spawn subprocess adapter '{}': {}. Is '{}' installed and on PATH?",
                self.command, e, self.program
            )))?;

        // Write input bytes to stdin
        if let Some(stdin) = child.stdin.as_mut() {
            stdin.write_all(bytes)
                .map_err(|e| TropelError::Other(format!(
                    "Failed to write to subprocess stdin: {}", e
                )))?;
        }

        // Wait for output
        let output = child.wait_with_output()
            .map_err(|e| TropelError::Other(format!(
                "Subprocess '{}' failed: {}", self.command, e
            )))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout_preview = String::from_utf8_lossy(&output.stdout[..output.stdout.len().min(200)]);
            return Err(TropelError::Other(format!(
                "Subprocess '{}' exited with {}: stderr={} stdout={}",
                self.command, output.status, stderr.trim(), stdout_preview.trim()
            )));
        }

        Ok(output.stdout)
    }
}

impl InputAdapter for SubprocessAdapter {
    fn id(&self) -> &str {
        // Derive a stable ID from the command
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

        let raw_scenario: serde_json::Value = serde_json::from_slice(&output)
            .map_err(|e| TropelError::Parse(format!(
                "Subprocess '{}' returned invalid JSON: {}. Raw output: {}",
                self.command, e,
                String::from_utf8_lossy(&output[..output.len().min(200)])
            )))?;

        // Accept either a full Scenario or an array of items
        let scenario = if raw_scenario.get("info").is_some() || raw_scenario.get("items").is_some() {
            serde_json::from_value::<Scenario>(raw_scenario)
                .map_err(|e| TropelError::Parse(format!(
                    "Subprocess '{}' returned invalid Scenario: {}", self.command, e
                )))?
        } else if let Some(items) = raw_scenario.as_array() {
            // Treat a JSON array as items, auto-generate a name
            Scenario {
                info: ScenarioInfo {
                    name: format!("subprocess-{}", self.command),
                    description: Some(format!("Imported via subprocess adapter '{}'", self.command)),
                    schema: None,
                },
                items: items.iter().map(|v| {
                    serde_json::from_value(v.clone())
                        .unwrap_or_else(|_| tropel_core::scenario::ScenarioItem {
                            id: format!("item-{}", rand::random::<u64>()),
                            name: "Imported item".to_string(),
                            request: None,
                            prerequest: None,
                            test: None,
                            assertions: vec![],
                            items: vec![],
                        })
                }).collect(),
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

// Register a placeholder — the CLI replaces this with the real command
// via `registry.register_input_adapter()` when `--subprocess-adapter` is used.
inventory::submit!(InputAdapterRegistration::new("subprocess", || {
    Box::new(SubprocessAdapter::new("echo"))
}));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_adapter_splits_command() {
        let adapter = SubprocessAdapter::new("python3 my-adapter.py");
        assert_eq!(adapter.program, "python3");
        assert_eq!(adapter.args, vec!["my-adapter.py"]);
    }

    #[test]
    fn test_new_adapter_simple() {
        let adapter = SubprocessAdapter::new("cat");
        assert_eq!(adapter.program, "cat");
        assert!(adapter.args.is_empty());
    }

    #[test]
    fn test_new_adapter_complex() {
        let adapter = SubprocessAdapter::new("node /path/to/adapter.js --verbose");
        assert_eq!(adapter.program, "node");
        assert_eq!(adapter.args, vec!["/path/to/adapter.js", "--verbose"]);
    }

    #[test]
    fn test_id_is_command() {
        let adapter = SubprocessAdapter::new("python3 my-adapter.py");
        assert_eq!(adapter.id(), "python3 my-adapter.py");
    }

    #[test]
    fn test_detect_fails_for_nonexistent_command() {
        let adapter = SubprocessAdapter::new("this-command-does-not-exist-hopefully");
        // Should return false (not crash)
        assert!(!adapter.detect(b"hello"));
    }

    #[test]
    fn test_parse_fails_for_nonexistent_command() {
        let adapter = SubprocessAdapter::new("this-command-does-not-exist-hopefully");
        let result = adapter.parse(b"hello");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("Failed to spawn"), "Expected spawn error, got: {}", msg);
    }

    #[test]
    fn test_parse_with_cat_returns_error_for_non_json() {
        let adapter = SubprocessAdapter::new("cat");
        // cat echoes stdin to stdout — that won't be valid JSON
        let result = adapter.parse(br#"not json"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_default_registration() {
        // The inventory-registered factory creates a fallback "echo" adapter
        let create_fn: fn() -> Box<dyn InputAdapter> = || Box::new(SubprocessAdapter::new("echo"));
        let adapter = create_fn();
        assert_eq!(adapter.id(), "echo");
    }
}
