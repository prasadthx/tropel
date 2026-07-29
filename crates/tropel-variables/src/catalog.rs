use rand::Rng;
use regex::Regex;
/// Dynamic variable catalog.
/// Generates values for built-in Postman dynamic variables like {{$guid}}, {{$timestamp}}, etc.
pub struct DynamicCatalog {
    // Uses direct string replacement and regex-based replacement internally
    // All patterns are matched by their literal strings
}

impl DynamicCatalog {
    pub fn new() -> Self {
        Self {}
    }

    /// Resolve all dynamic variables in a string.
    /// Each occurrence of a dynamic variable generates a fresh value.
    pub fn resolve(&self, s: &str) -> String {
        let mut result = s.to_string();
        let mut rng = rand::thread_rng();

        // {{$guid}} — fresh UUID per occurrence
        if result.contains("{{$guid}}") {
            let re = Regex::new(r"\{\{\$guid\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| uuid::Uuid::new_v4().to_string());
        }

        // {{$timestamp}} — fresh Unix timestamp per occurrence
        if result.contains("{{$timestamp}}") {
            let re = Regex::new(r"\{\{\$timestamp\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
                    .to_string()
            });
        }

        // {{$isoTimestamp}} — fresh ISO timestamp per occurrence
        if result.contains("{{$isoTimestamp}}") {
            let re = Regex::new(r"\{\{\$isoTimestamp\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| chrono_now_iso());
        }

        // {{$randomUUID}} — fresh UUID per occurrence
        if result.contains("{{$randomUUID}}") {
            let re = Regex::new(r"\{\{\$randomUUID\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| uuid::Uuid::new_v4().to_string());
        }

        // {{$randomInt}} — fresh random integer per occurrence
        if result.contains("{{$randomInt}}") {
            let re = Regex::new(r"\{\{\$randomInt\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| rng.gen_range(0..1000u32).to_string());
        }

        // {{$randomFloat}} — fresh random float per occurrence
        if result.contains("{{$randomFloat}}") {
            let re = Regex::new(r"\{\{\$randomFloat\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| format!("{:.6}", rng.gen::<f64>() * 1000.0));
        }

        // {{$randomString[:length]}}
        if result.contains("{{$randomString") {
            let re = Regex::new(r"\{\{\$randomString(?::(\d+))?\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |caps| {
                let len = caps.get(1).and_then(|m| m.as_str().parse().ok()).unwrap_or(10);
                random_string(&mut rng, len, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
            });
        }

        // {{$randomAlphabetic[:length]}}
        if result.contains("{{$randomAlphabetic") {
            let re = Regex::new(r"\{\{\$randomAlphabetic(?::(\d+))?\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |caps| {
                let len = caps.get(1).and_then(|m| m.as_str().parse().ok()).unwrap_or(10);
                random_string(&mut rng, len, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
            });
        }

        // {{$randomAlphanumeric[:length]}}
        if result.contains("{{$randomAlphanumeric") {
            let re = Regex::new(r"\{\{\$randomAlphanumeric(?::(\d+))?\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |caps| {
                let len = caps.get(1).and_then(|m| m.as_str().parse().ok()).unwrap_or(10);
                random_string(&mut rng, len, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
            });
        }

        // {{$randomBoolean}} — fresh random bool per occurrence
        if result.contains("{{$randomBoolean}}") {
            let re = Regex::new(r"\{\{\$randomBoolean\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| rng.gen_bool(0.5).to_string());
        }

        // {{$randomHex[:length]}}
        if result.contains("{{$randomHex") {
            let re = Regex::new(r"\{\{\$randomHex(?::(\d+))?\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |caps| {
                let len = caps.get(1).and_then(|m| m.as_str().parse().ok()).unwrap_or(8);
                random_string(&mut rng, len, "0123456789abcdef")
            });
        }

        // {{$randomEmail}} — fresh email per occurrence
        if result.contains("{{$randomEmail}}") {
            let re = Regex::new(r"\{\{\$randomEmail\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| {
                let name = random_string(&mut rng, 8, "abcdefghijklmnopqrstuvwxyz");
                let domain = random_string(&mut rng, 6, "abcdefghijklmnopqrstuvwxyz");
                format!("{}@{}.com", name, domain)
            });
        }

        // {{$randomIP}} — fresh IP per occurrence
        if result.contains("{{$randomIP}}") {
            let re = Regex::new(r"\{\{\$randomIP\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| {
                format!("{}.{}.{}.{}", rng.gen_range(1..255u32), rng.gen_range(0..255u32), rng.gen_range(0..255u32), rng.gen_range(1..255u32))
            });
        }

        // {{$randomCity}}, {{$randomCountry}}, {{$randomStreet}}, {{$randomPostcode}},
        // {{$randomNameFullName}}, {{$randomNameFirstName}}, {{$randomNameLastName}},
        // {{$randomName}}, {{$randomColor}}, {{$randomMAC}}
        if result.contains("{{$randomCity}}") {
            let re = Regex::new(r"\{\{\$randomCity\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| random_string(&mut rng, 8, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"));
        }
        if result.contains("{{$randomCountry}}") {
            let re = Regex::new(r"\{\{\$randomCountry\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| random_string(&mut rng, 8, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"));
        }
        if result.contains("{{$randomStreet}}") {
            let re = Regex::new(r"\{\{\$randomStreet\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| random_string(&mut rng, 8, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"));
        }
        if result.contains("{{$randomPostcode}}") {
            let re = Regex::new(r"\{\{\$randomPostcode\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| random_string(&mut rng, 5, "0123456789"));
        }
        if result.contains("{{$randomName}}") {
            // Note: {{$randomName}} is the base pattern; longer forms like
            // {{$randomNameFullName}} are handled later with more specific regexes.
            let re = Regex::new(r"\{\{\$randomName\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| random_string(&mut rng, 8, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"));
        }
        if result.contains("{{$randomNameFullName}}") {
            let re = Regex::new(r"\{\{\$randomNameFullName\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| {
                format!("{} {}", 
                    random_string(&mut rng, 6, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"),
                    random_string(&mut rng, 8, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
                )
            });
        }
        if result.contains("{{$randomNameFirstName}}") {
            let re = Regex::new(r"\{\{\$randomNameFirstName\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| random_string(&mut rng, 6, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"));
        }
        if result.contains("{{$randomNameLastName}}") {
            let re = Regex::new(r"\{\{\$randomNameLastName\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| random_string(&mut rng, 8, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"));
        }
        if result.contains("{{$randomColor}}") {
            let re = Regex::new(r"\{\{\$randomColor\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| random_string(&mut rng, 6, "0123456789abcdef"));
        }
        if result.contains("{{$randomMAC}}") {
            let re = Regex::new(r"\{\{\$randomMAC\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |_| {
                let hex = random_string(&mut rng, 12, "0123456789abcdef");
                hex.chars().collect::<Vec<_>>().chunks(2).map(|c| c.iter().collect::<String>()).collect::<Vec<_>>().join(":")
            });
        }

        // {{$randomPassword[:length]}}
        if result.contains("{{$randomPassword") {
            let re = Regex::new(r"\{\{\$randomPassword(?::(\d+))?\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |caps| {
                let len = caps.get(1).and_then(|m| m.as_str().parse().ok()).unwrap_or(12);
                random_string(&mut rng, len, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*")
            });
        }

        result
    }

    /// Replace regex matches using a closure with proper lifetime handling.
    fn replace_with_func<F>(&self, input: &str, re: &Regex, mut f: F) -> String
    where
        F: FnMut(&regex::Captures) -> String,
    {
        let mut result = String::new();
        let mut last_end = 0;

        for caps in re.captures_iter(input) {
            let m = caps.get(0).unwrap();
            // Append text before this match
            result.push_str(&input[last_end..m.start()]);
            // Append replacement
            result.push_str(&f(&caps));
            last_end = m.end();
        }

        // Append remaining text
        result.push_str(&input[last_end..]);
        result
    }
}

impl Default for DynamicCatalog {
    fn default() -> Self {
        Self::new()
    }
}

fn random_string(rng: &mut impl Rng, length: usize, charset: &str) -> String {
    let chars: Vec<char> = charset.chars().collect();
    (0..length).map(|_| chars[rng.gen_range(0..chars.len())]).collect()
}

fn chrono_now_iso() -> String {
    let now = chrono::Utc::now();
    now.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guid() {
        let catalog = DynamicCatalog::new();
        let result = catalog.resolve("prefix-{{$guid}}-suffix");
        assert!(result.starts_with("prefix-"));
        assert!(result.ends_with("-suffix"));
        let guid = result.trim_start_matches("prefix-").trim_end_matches("-suffix");
        assert_eq!(guid.len(), 36); // UUID v4 with hyphens
    }

    #[test]
    fn test_timestamp() {
        let catalog = DynamicCatalog::new();
        let result = catalog.resolve("ts={{$timestamp}}");
        assert!(result.starts_with("ts="));
        let ts: u64 = result[3..].parse().expect("Should be a number");
        assert!(ts > 1700000000); // Should be a reasonable recent timestamp
    }

    #[test]
    fn test_random_int() {
        let catalog = DynamicCatalog::new();
        let result = catalog.resolve("n={{$randomInt}}");
        assert!(result.starts_with("n="));
        let n: u32 = result[2..].parse().expect("Should be a number");
        assert!(n < 1000);
    }

    #[test]
    fn test_no_vars() {
        let catalog = DynamicCatalog::new();
        let result = catalog.resolve("just a string");
        assert_eq!(result, "just a string");
    }

    #[test]
    fn test_multiple_same_var_fresh_values() {
        let catalog = DynamicCatalog::new();
        // Use | as separator since neither UUIDs nor the placeholder contain it
        let result = catalog.resolve("{{$guid}}|{{$guid}}");
        assert!(!result.contains("{{$guid}}"));
        let parts: Vec<&str> = result.split('|').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].len(), 36);
        assert_eq!(parts[1].len(), 36);
        // They should be different UUIDs (extremely unlikely to collide)
        assert_ne!(parts[0], parts[1], "{{$guid}}-{{$guid}} should produce two different values");
    }

    #[test]
    fn test_repeated_timestamp_fresh_values() {
        let catalog = DynamicCatalog::new();
        let result = catalog.resolve("{{$timestamp}}-{{$timestamp}}");
        assert!(!result.contains("{{$timestamp}}"));
        let parts: Vec<&str> = result.split('-').collect();
        assert_eq!(parts.len(), 2);
        // Both should be valid timestamps
        let t1: u64 = parts[0].parse().expect("First should be a number");
        let t2: u64 = parts[1].parse().expect("Second should be a number");
        assert!(t1 > 1700000000);
        assert!(t2 > 1700000000);
    }

    #[test]
    fn test_repeated_random_int_fresh_values() {
        let catalog = DynamicCatalog::new();
        let result = catalog.resolve("{{$randomInt}}-{{$randomInt}}");
        assert!(!result.contains("{{$randomInt}}"));
        let parts: Vec<&str> = result.split('-').collect();
        assert_eq!(parts.len(), 2);
        // Both should be valid integers < 1000
        let n1: u32 = parts[0].parse().expect("First should be a number");
        let n2: u32 = parts[1].parse().expect("Second should be a number");
        assert!(n1 < 1000);
        assert!(n2 < 1000);
        // They may rarely collide (1/1000 chance), but that's OK — the important
        // thing is they're both parsed as valid ints and the placeholder is gone.
    }

    #[test]
    fn test_random_hex() {
        let catalog = DynamicCatalog::new();
        let result = catalog.resolve("hex={{$randomHex:16}}");
        assert!(result.starts_with("hex="));
        assert_eq!(result.len(), "hex=".len() + 16);
    }
}
