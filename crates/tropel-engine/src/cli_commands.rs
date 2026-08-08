//! Non-run CLI subcommands: `inspect`, `archive`, `extensions`, `build` and
//! `version`, plus the shared data-file loader. Split out of the former
//! `cli.rs` god-file.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tropel_core::scenario::{Scenario, ScenarioItem};
use tropel_core::{Result, TropelError};
use tropel_ext::traits::{Driver, InputAdapter};

use crate::cli_registry::build_registry;

/// `tropel inspect <input>` — show how an input resolves and what it contains
/// WITHOUT running a load test. Useful to verify a collection/HAR/script
/// parses correctly, which adapter/driver handles it, and what load profile a
/// k6 script declares.
pub(crate) async fn inspect_command(
    input: &Path,
    format: Option<&str>,
    plugins_dir: Option<&Path>,
    subprocess_adapter: &[String],
) -> Result<()> {
    let registry = build_registry(subprocess_adapter, plugins_dir)?;
    let bytes = std::fs::read(input)
        .map_err(|e| TropelError::Parse(format!("Failed to read '{}': {}", input.display(), e)))?;

    println!("Tropel Inspect — v{}", env!("CARGO_PKG_VERSION"));
    println!("Input: {}", input.display());

    // 1. Drivers first (same precedence as the engine).
    let driver: Option<Box<dyn Driver>> = if let Some(fmt) = format {
        registry.resolve_driver_by_id(fmt)
    } else {
        registry.resolve_driver(&bytes)
    };
    if let Some(driver) = driver {
        println!("Resolved by driver: {}", driver.id());
        println!("Kind: imperative (runs JS per iteration)");
        // Note: an empty env is passed here — scripts that derive their
        // options from `__ENV` will show the defaults rather than what a
        // configured `run` would apply. `inspect` is a dry-run verification
        // tool; threading real env/`-e` values here is a future enhancement.
        match driver
            .declared_options(&bytes, Some(input), &HashMap::new())
            .await
        {
            Ok(Some(opts)) => {
                println!("Declared options:");
                if let Some(exec) = &opts.execution {
                    println!("  execution: {} ({:?})", exec.executor_name(), exec);
                }
                if let Some(scenarios) = &opts.scenarios {
                    println!("  scenarios: {}", scenarios.len());
                    for (name, sc) in scenarios {
                        println!(
                            "    - {}: {} ({:?})",
                            name,
                            sc.execution.executor_name(),
                            sc.execution
                        );
                    }
                }
                if !opts.thresholds.is_empty() {
                    println!("  thresholds: {}", opts.thresholds.len());
                    for (name, t) in &opts.thresholds {
                        println!("    - {}: {}", name, t.expression);
                    }
                }
            }
            Ok(None) => {
                println!("Declared options: (none)");
            }
            Err(e) => {
                // Backlog line 153: the script DECLARES options but they are
                // malformed — surface the error rather than silently showing
                // nothing.
                println!("Declared options: ERROR — {}", e);
            }
        }
        return Ok(());
    }

    // 2. Fall back to input adapters (declarative).
    let adapter: Box<dyn InputAdapter> = if let Some(fmt) = format {
        registry.resolve_input_by_id(fmt).ok_or_else(|| {
            let available = registry.list_inputs();
            TropelError::Config(format!(
                "Unknown input format '{}'. Available formats: {}",
                fmt,
                available.join(", ")
            ))
        })?
    } else {
        registry.resolve_input(&bytes).ok_or_else(|| {
            let available = registry.list_inputs();
            TropelError::Parse(format!(
                "No input adapter recognized '{}'. Available adapters: {}",
                input.display(),
                if available.is_empty() {
                    "(none registered — check build configuration)".to_string()
                } else {
                    available.join(", ")
                }
            ))
        })?
    };

    println!("Resolved by adapter: {}", adapter.id());
    println!("Kind: declarative (static request list)");
    let scenario = adapter.parse_with_path(&bytes, Some(input))?;
    print_scenario_summary(&scenario);
    Ok(())
}

/// Recursively print a scenario's request tree plus totals.
pub(crate) fn print_scenario_summary(scenario: &Scenario) {
    println!("Scenario: {}", scenario.info.name);
    if let Some(desc) = &scenario.info.description {
        println!("  description: {}", desc);
    }
    if let Some(auth) = &scenario.auth {
        println!("  global auth: {:?}", auth);
    }
    println!("  variables: {} defined", scenario.variables.len());
    for (k, v) in &scenario.variables {
        println!("    {} = {}", k, v);
    }

    fn walk(items: &[ScenarioItem], depth: usize, out: &mut (usize, usize)) {
        for item in items {
            let indent = "  ".repeat(depth);
            match &item.request {
                Some(req) => {
                    out.0 += 1;
                    let scripted = item.test.is_some() || item.prerequest.is_some();
                    if scripted {
                        out.1 += 1;
                    }
                    println!(
                        "{}• {} — {} {}{}",
                        indent,
                        item.name,
                        req.method,
                        req.url,
                        if scripted { " (scripted)" } else { "" }
                    );
                }
                None => {
                    println!("{}▸ {} (folder)", indent, item.name);
                    walk(&item.items, depth + 1, out);
                }
            }
        }
    }

    let mut counts = (0usize, 0usize);
    walk(&scenario.items, 1, &mut counts);
    println!("Totals: {} requests ({} with scripts)", counts.0, counts.1);
}

/// `tropel archive <input> -o <dir>` — bundle a test into a self-contained
/// directory so it can be replayed on another machine (or after the original
/// files move). Copies the input plus any referenced data/env/config files and
/// writes a `tropel-archive.json` manifest describing how to re-run it.
pub(crate) async fn archive_command(
    input: &Path,
    format: Option<&str>,
    output: Option<&Path>,
    data_file: Option<&Path>,
    env_file: Option<&Path>,
    config: Option<&Path>,
) -> Result<()> {
    let out_dir = output.unwrap_or_else(|| Path::new("./tropel-archive"));
    std::fs::create_dir_all(out_dir).map_err(TropelError::Io)?;

    // Input file — the core of the bundle.
    let input_name = input
        .file_name()
        .ok_or_else(|| {
            TropelError::Config(format!("Input '{}' has no file name", input.display()))
        })?
        .to_string_lossy()
        .to_string();
    let bundled_input = out_dir.join(&input_name);
    std::fs::copy(input, &bundled_input).map_err(TropelError::Io)?;

    // Referenced dependency files, each copied next to the input. All files
    // land in ONE flat directory keyed by file name, so a dep sharing the
    // input's name (or another dep's name) would silently overwrite — guard
    // against collisions so the bundle stays deterministic.
    let mut deps: Vec<(String, PathBuf, PathBuf)> = Vec::new(); // (role, src, dest)
    let mut used_names: HashMap<String, String> = HashMap::new(); // name -> role
    used_names.insert(input_name.clone(), "input".to_string());
    let mut copy_dep = |role: &str, src: &Path, out: &Path| -> Result<()> {
        let name = src
            .file_name()
            .ok_or_else(|| {
                TropelError::Config(format!("{} '{}' has no file name", role, src.display()))
            })?
            .to_string_lossy()
            .to_string();
        if let Some(existing) = used_names.get(&name) {
            return Err(TropelError::Config(format!(
                "archive: '{}' (from {}) collides with {} — all bundled files \
                 share one directory, rename it or bundle separately",
                name,
                src.display(),
                existing
            )));
        }
        used_names.insert(name.clone(), role.to_string());
        let dest = out.join(&name);
        std::fs::copy(src, &dest).map_err(TropelError::Io)?;
        deps.push((role.to_string(), src.to_path_buf(), dest));
        Ok(())
    };
    if let Some(d) = data_file {
        copy_dep("data_file", d, out_dir)?;
    }
    if let Some(e) = env_file {
        copy_dep("env_file", e, out_dir)?;
    }
    if let Some(c) = config {
        copy_dep("config", c, out_dir)?;
    }

    // Manifest: how to re-run this bundle.
    let mut manifest = serde_json::Map::new();
    manifest.insert(
        "version".into(),
        serde_json::Value::String(env!("CARGO_PKG_VERSION").into()),
    );
    manifest.insert(
        "input".into(),
        serde_json::Value::String(input_name.clone()),
    );
    if let Some(fmt) = format {
        manifest.insert("format".into(), serde_json::Value::String(fmt.to_string()));
    }
    let mut dep_map = serde_json::Map::new();
    for (role, _src, dest) in &deps {
        dep_map.insert(
            role.clone(),
            serde_json::Value::String(
                dest.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string(),
            ),
        );
    }
    manifest.insert("bundled_files".into(), serde_json::Value::Object(dep_map));

    // Build the suggested re-run command relative to the bundle directory.
    let mut run_cmd = format!("tropel run {}", input_name);
    if let Some(fmt) = format {
        run_cmd.push_str(&format!(" --format {}", fmt));
    }
    for (role, _src, dest) in &deps {
        let flag = match role.as_str() {
            "data_file" => "--data-file",
            "env_file" => "--env-file",
            "config" => "--config",
            _ => continue,
        };
        run_cmd.push_str(&format!(
            " {} {}",
            flag,
            dest.file_name().unwrap_or_default().to_string_lossy()
        ));
    }
    manifest.insert("run".into(), serde_json::Value::String(run_cmd.clone()));

    let manifest_path = out_dir.join("tropel-archive.json");
    let manifest_json = serde_json::Value::Object(manifest);
    std::fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest_json)
            .map_err(|e| TropelError::Other(format!("manifest serialize: {}", e)))?,
    )
    .map_err(TropelError::Io)?;

    println!("Tropel Archive — v{}", env!("CARGO_PKG_VERSION"));
    println!("Bundle created in: {}", out_dir.display());
    println!(
        "  input:  {} (from {})",
        bundled_input.display(),
        input.display()
    );
    for (role, src, dest) in &deps {
        println!("  {}: {} (from {})", role, dest.display(), src.display());
    }
    println!("  manifest: {}", manifest_path.display());
    println!("Re-run from the bundle directory:");
    println!("  cd {} && {}", out_dir.display(), run_cmd);
    Ok(())
}

pub(crate) async fn list_extensions(plugins_dir: Option<&std::path::Path>) -> Result<()> {
    let registry = build_registry(&[], plugins_dir)?;

    let inputs = registry.list_inputs();

    println!("Tropel Extensions — v{}", env!("CARGO_PKG_VERSION"));
    println!();

    if inputs.is_empty() {
        println!("  No input adapters registered.");
        println!("  Use `tropel build --with <crate>` to build a custom binary with extensions.");
    } else {
        println!("  Input formats:");
        for fmt in &inputs {
            println!(
                "    - {}  (use: `tropel run input.{} --format {})",
                fmt, fmt, fmt
            );
        }
        println!();
        println!("  Use `tropel run <file> --format <name>` to select a specific format.");
        println!("  Without `--format`, the engine auto-detects from file content.");
    }

    let protocols = registry.list_protocols();
    if !protocols.is_empty() {
        println!();
        println!("  Protocols:");
        for p in &protocols {
            println!("    - {}", p);
        }
    }

    let outputs = registry.list_outputs();
    if !outputs.is_empty() {
        println!();
        println!("  Outputs:");
        for o in &outputs {
            println!("    - {}", o);
        }
    }

    Ok(())
}

pub(crate) async fn build_custom(
    with: &[String],
    output: &std::path::Path,
    release: bool,
) -> Result<()> {
    use tropel_build::{build, BuildConfig};

    // Each `--with` spec is parsed AND validated here — names/versions/URLs
    // are injected into the generated Cargo.toml, so a malformed or hostile
    // value must fail before any file is written (build-time code injection).
    let mut extensions = Vec::with_capacity(with.len());
    for spec in with {
        extensions.push(tropel_build::parse_dep_spec(spec)?);
    }

    let config = BuildConfig {
        extensions,
        output: output.to_path_buf(),
        release,
    };

    build(&config)
}

pub(crate) fn print_version() -> Result<()> {
    println!("Tropel v{}", env!("CARGO_PKG_VERSION"));
    println!("Repository: https://github.com/prasadthx/tropel");
    println!("License: MIT OR Apache-2.0");
    Ok(())
}

/// Load iteration data from a CSV or JSON file.
pub(crate) fn load_data_file(path: &PathBuf) -> Result<Vec<HashMap<String, serde_json::Value>>> {
    let content = std::fs::read_to_string(path).map_err(TropelError::Io)?;

    let trimmed = content.trim();

    if trimmed.starts_with('[') {
        let data: Vec<HashMap<String, serde_json::Value>> = serde_json::from_str(trimmed)
            .map_err(|e| TropelError::Parse(format!("JSON data-file parse error: {}", e)))?;
        return Ok(data);
    }

    if trimmed.contains(',') || trimmed.starts_with('"') {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(content.as_bytes());

        let headers: Vec<String> = reader
            .headers()
            .map_err(|e| TropelError::Parse(format!("CSV header error: {}", e)))?
            .iter()
            .map(|h| h.to_string())
            .collect();

        let mut rows = Vec::new();
        for result in reader.records() {
            let record =
                result.map_err(|e| TropelError::Parse(format!("CSV record error: {}", e)))?;
            let mut map = HashMap::new();
            for (i, field) in record.iter().enumerate() {
                if i < headers.len() {
                    map.insert(
                        headers[i].clone(),
                        serde_json::Value::String(field.to_string()),
                    );
                }
            }
            rows.push(map);
        }
        return Ok(rows);
    }

    Ok(vec![])
}
