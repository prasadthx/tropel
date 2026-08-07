//! Minimal structured YAML emitter for generated Kubernetes manifests.
//!
//! The previous implementation interpolated values (namespace, image,
//! embedded JSON) directly into a raw `format!` template, so a value
//! containing a YAML-significant character (`: `, `#`, a leading `-`,
//! quotes, a trailing space) silently corrupted the document. This module
//! emits scalars through a plain-safe check — quoting anything ambiguous
//! — and builds block scalars by indenting from the key column, so
//! arbitrary user input can never break the manifest.

/// Whether `s` can be emitted as a plain YAML scalar without quoting.
///
/// Plain scalars must not start with an indicator character, must not
/// contain `: ` (or end with `:`), ` #`, control characters, quotes, or
/// leading/trailing whitespace.
fn plain_safe(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let mut chars = s.char_indices();
    let first = chars.next().map(|(_, c)| c).unwrap();
    if first == ' ' {
        return false;
    }
    // Indicator characters that would change the node kind if leading.
    if matches!(
        first,
        '-' | '?'
            | ':'
            | ','
            | '['
            | ']'
            | '{'
            | '}'
            | '#'
            | '&'
            | '*'
            | '!'
            | '|'
            | '>'
            | '\''
            | '"'
            | '%'
            | '@'
            | '`'
            | '~'
    ) {
        return false;
    }
    if s.ends_with(' ') || s.contains(' ') {
        // A mid-string space is technically legal in a plain scalar, but
        // it reads as multiple tokens and makes a following `#` (comment)
        // or `:` ambiguous to readers — quote whitespace-bearing values.
        return false;
    }
    // YAML 1.1 type resolution: k8s (go-yaml) would reinterpret these
    // plain scalars as bool/int/null instead of strings, silently
    // changing a user-controlled namespace/image/name.
    if is_yaml_resolvable(s) {
        return false;
    }
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b':' => {
                // `:` is a mapping separator only when followed by space
                // or at the end of the scalar.
                if i + 1 >= bytes.len() || bytes[i + 1] == b' ' {
                    return false;
                }
            }
            b'#' => {
                // `#` starts a comment only after whitespace.
                if i == 0 || bytes[i - 1] == b' ' {
                    return false;
                }
            }
            b'"' | b'\'' | b'\\' => return false,
            b'\n' | b'\r' | b'\t' => return false,
            0x00..=0x1f => return false,
            _ => {}
        }
    }
    true
}

/// Escape and double-quote a string for single-line YAML.
fn double_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Whether YAML 1.1 would resolve `s` to a non-string type (bool, null,
/// or number) if emitted plain — these must be quoted so k8s reads them
/// as the strings the user actually provided.
///
/// Note: strings containing `:` (e.g. `0.0.0.0:17890`, `reg/tropel:v1`)
/// can never type-resolve, so they stay plain.
fn is_yaml_resolvable(s: &str) -> bool {
    if s == "~" {
        return true;
    }
    let lower = s.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "true" | "false" | "yes" | "no" | "on" | "off" | "null"
    ) {
        return true;
    }
    if s.contains(':') {
        return false;
    }
    // Numbers: 123, -1.5, 1e3, .inf — anything that parses as a float
    // would be read as a number by go-yaml.
    if s.parse::<f64>().is_ok() {
        return true;
    }
    // Hex: 0x1f, 0XFF.
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        return !hex.is_empty() && hex.chars().all(|c| c.is_ascii_hexdigit());
    }
    false
}

/// Render a value as a YAML scalar: plain when safe, double-quoted
/// otherwise. Never produces a broken document for arbitrary input.
pub fn scalar(s: &str) -> String {
    if plain_safe(s) {
        s.to_string()
    } else {
        double_quote(s)
    }
}

/// Line-oriented YAML document builder. Indentation is measured in
/// levels; each level is two spaces.
#[derive(Default)]
pub struct YamlDoc {
    lines: Vec<String>,
}

impl YamlDoc {
    pub fn new() -> Self {
        Self::default()
    }

    fn indent(level: usize) -> String {
        "  ".repeat(level)
    }

    /// `key: value` — the value is scalar-quoted.
    pub fn kv(&mut self, level: usize, key: &str, value: &str) {
        self.lines
            .push(format!("{}{}: {}", Self::indent(level), key, scalar(value)));
    }

    /// `key: <num>` — numeric values are never quoted.
    pub fn kv_num(&mut self, level: usize, key: &str, value: impl std::fmt::Display) {
        self.lines
            .push(format!("{}{}: {}", Self::indent(level), key, value));
    }

    /// `key: value` emitted verbatim — for fixed literals WE control that
    /// must stay unquoted (`readOnly: true`) regardless of YAML 1.1
    /// type-resolution. Never use with user-controlled values.
    pub fn kv_plain(&mut self, level: usize, key: &str, value: &str) {
        self.lines
            .push(format!("{}{}: {}", Self::indent(level), key, value));
    }

    /// A bare mapping key whose value is a nested mapping or list.
    pub fn key(&mut self, level: usize, key: &str) {
        self.lines.push(format!("{}{}:", Self::indent(level), key));
    }

    /// `- <value>` list item; the value is always double-quoted (these are
    /// argv strings in the manifests).
    pub fn item(&mut self, level: usize, value: &str) {
        self.lines
            .push(format!("{}- {}", Self::indent(level), double_quote(value)));
    }

    /// `- name: <value>` list item with a scalar-quoted value.
    pub fn item_kv(&mut self, level: usize, key: &str, value: &str) {
        self.lines.push(format!(
            "{}- {}: {}",
            Self::indent(level),
            key,
            scalar(value)
        ));
    }

    /// `key: |-` literal block scalar. Content is indented two levels past
    /// the key so the block is unambiguously nested under it.
    pub fn block(&mut self, level: usize, key: &str, content: &str) {
        self.lines
            .push(format!("{}{}: |-", Self::indent(level), key));
        let pad = Self::indent(level + 2);
        for line in content.lines() {
            self.lines.push(format!("{pad}{line}"));
        }
    }

    /// Emit a comment line (safe for any content).
    pub fn comment(&mut self, text: &str) {
        self.lines.push(format!("# {text}"));
    }

    /// `---` document separator.
    pub fn separator(&mut self) {
        self.lines.push("---".to_string());
    }

    pub fn finish(self) -> String {
        let mut out = self.lines.join("\n");
        out.push('\n');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_scalars_stay_unquoted() {
        for s in [
            "loadtest",
            "default",
            "tropel:latest",
            "reg/tropel:v1",
            "/etc/tropel",
            "None",
            "Indexed",
            "tropel-job",
            "0.0.0.0:17890",
        ] {
            assert_eq!(scalar(s), s, "expected {s:?} to stay plain");
        }
    }

    #[test]
    fn ambiguous_scalars_get_quoted() {
        for (input, expected) in [
            ("my ns", "\"my ns\""),
            ("-weird", "\"-weird\""),
            ("key: value", "\"key: value\""),
            ("trailing: ", "\"trailing: \""),
            ("has#comment", "has#comment"), // `#` not after space → plain
            ("has #comment", "\"has #comment\""),
            ("quote\"inside", "\"quote\\\"inside\""),
            ("new\nline", "\"new\\nline\""),
            ("", "\"\""),
            // YAML 1.1 type-resolution hazards: k8s would read these as
            // bool/null/number, not strings.
            ("true", "\"true\""),
            ("yes", "\"yes\""),
            ("null", "\"null\""),
            ("~", "\"~\""),
            ("123", "\"123\""),
            ("1e3", "\"1e3\""),
            ("-1.5", "\"-1.5\""),
            ("0x1f", "\"0x1f\""),
        ] {
            assert_eq!(scalar(input), expected, "input {input:?}");
        }
    }

    #[test]
    fn resolvable_lookalikes_stay_plain() {
        // `:`-bearing and multi-token strings never type-resolve.
        for s in [
            "0.0.0.0:17890",
            "reg/tropel:v1",
            "tropel:latest",
            "None",
            "loadtest",
        ] {
            assert_eq!(scalar(s), s, "expected {s:?} to stay plain");
        }
    }

    #[test]
    fn kv_plain_emits_fixed_literals_verbatim() {
        let mut d = YamlDoc::new();
        d.kv_plain(0, "readOnly", "true");
        assert_eq!(d.finish(), "readOnly: true\n");
    }

    #[test]
    fn block_indents_from_key_column() {
        let mut d = YamlDoc::new();
        d.key(0, "data");
        d.block(1, "job.json", "{\n  \"a\": 1\n}");
        let out = d.finish();
        assert_eq!(
            out,
            "data:\n  job.json: |-\n      {\n        \"a\": 1\n      }\n"
        );
    }

    #[test]
    fn args_are_always_quoted() {
        let mut d = YamlDoc::new();
        d.key(0, "args");
        d.item(1, "cloud-run");
        d.item(1, "3");
        let out = d.finish();
        assert_eq!(out, "args:\n  - \"cloud-run\"\n  - \"3\"\n");
    }
}
