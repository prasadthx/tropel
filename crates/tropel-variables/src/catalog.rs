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
    pub fn resolve(&self, s: &str) -> String {
        let mut result = s.to_string();
        let mut rng = rand::thread_rng();

        // Simple sequential string replacement for each pattern
        // Using a simpler approach that avoids closure lifetime issues

        // {{$guid}}
        if result.contains("{{$guid}}") {
            let guid = uuid::Uuid::new_v4().to_string();
            result = result.replace("{{$guid}}", &guid);
        }

        // {{$timestamp}}
        if result.contains("{{$timestamp}}") {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            result = result.replace("{{$timestamp}}", &now.to_string());
        }

        // {{$isoTimestamp}}
        if result.contains("{{$isoTimestamp}}") {
            let iso = chrono_now_iso();
            result = result.replace("{{$isoTimestamp}}", &iso);
        }

        // {{$randomUUID}}
        if result.contains("{{$randomUUID}}") {
            let guid = uuid::Uuid::new_v4().to_string();
            result = result.replace("{{$randomUUID}}", &guid);
        }

        // {{$randomInt}}
        if result.contains("{{$randomInt}}") {
            let n: u32 = rng.gen_range(0..1000);
            result = result.replace("{{$randomInt}}", &n.to_string());
        }

        // {{$randomFloat}}
        if result.contains("{{$randomFloat}}") {
            let f = rng.gen::<f64>() * 1000.0;
            result = result.replace("{{$randomFloat}}", &format!("{:.6}", f));
        }

        // {{$randomString[:length]}}
        if result.contains("{{$randomString") {
            // Use a regex finder approach
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

        // {{$randomBoolean}}
        if result.contains("{{$randomBoolean}}") {
            let val = rng.gen_bool(0.5);
            result = result.replace("{{$randomBoolean}}", &val.to_string());
        }

        // {{$randomHex[:length]}}
        if result.contains("{{$randomHex") {
            let re = Regex::new(r"\{\{\$randomHex(?::(\d+))?\}\}").unwrap();
            result = self.replace_with_func(&result, &re, |caps| {
                let len = caps.get(1).and_then(|m| m.as_str().parse().ok()).unwrap_or(8);
                random_string(&mut rng, len, "0123456789abcdef")
            });
        }

        // {{$randomEmail}}
        if result.contains("{{$randomEmail}}") {
            let name = random_string(&mut rng, 8, "abcdefghijklmnopqrstuvwxyz");
            let domain = random_string(&mut rng, 6, "abcdefghijklmnopqrstuvwxyz");
            result = result.replace("{{$randomEmail}}", &format!("{}@{}.com", name, domain));
        }

        // {{$randomIP}}
        if result.contains("{{$randomIP}}") {
            let ip = format!("{}.{}.{}.{}", rng.gen_range(1..255), rng.gen_range(0..255), rng.gen_range(0..255), rng.gen_range(1..255));
            result = result.replace("{{$randomIP}}", &ip);
        }

        // {{$randomCity}}, {{$randomCountry}}, {{$randomStreet}}, {{$randomPostcode}},
        // {{$randomFullName}}, {{$randomName}}, {{$randomColor}}
        if result.contains("{{$randomCity}}") {
            result = result.replace("{{$randomCity}}", &random_string(&mut rng, 8, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"));
        }
        if result.contains("{{$randomCountry}}") {
            result = result.replace("{{$randomCountry}}", &random_string(&mut rng, 8, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"));
        }
        if result.contains("{{$randomStreet}}") {
            result = result.replace("{{$randomStreet}}", &random_string(&mut rng, 8, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"));
        }
        if result.contains("{{$randomPostcode}}") {
            result = result.replace("{{$randomPostcode}}", &random_string(&mut rng, 5, "0123456789"));
        }
        if result.contains("{{$randomName}}") {
            result = result.replace("{{$randomName}}", &random_string(&mut rng, 8, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"));
        }
        if result.contains("{{$randomNameFullName}}") {
            result = result.replace("{{$randomNameFullName}}", &format!("{} {}", 
                random_string(&mut rng, 6, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"),
                random_string(&mut rng, 8, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
            ));
        }
        if result.contains("{{$randomNameFirstName}}") {
            result = result.replace("{{$randomNameFirstName}}", &random_string(&mut rng, 6, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"));
        }
        if result.contains("{{$randomNameLastName}}") {
            result = result.replace("{{$randomNameLastName}}", &random_string(&mut rng, 8, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"));
        }
        if result.contains("{{$randomColor}}") {
            result = result.replace("{{$randomColor}}", &random_string(&mut rng, 6, "0123456789abcdef"));
        }
        if result.contains("{{$randomMAC}}") {
            let hex = random_string(&mut rng, 12, "0123456789abcdef");
            let mac = hex.chars().collect::<Vec<_>>().chunks(2).map(|c| c.iter().collect::<String>()).collect::<Vec<_>>().join(":");
            result = result.replace("{{$randomMAC}}", &mac);
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
    fn test_multiple_same_var() {
        let catalog = DynamicCatalog::new();
        let result = catalog.resolve("{{$guid}}-{{$guid}}");
        // Both should be replaced, but likely with the same value since we use replace()
        assert!(!result.contains("{{$guid}}"));
    }

    #[test]
    fn test_random_hex() {
        let catalog = DynamicCatalog::new();
        let result = catalog.resolve("hex={{$randomHex:16}}");
        assert!(result.starts_with("hex="));
        assert_eq!(result.len(), "hex=".len() + 16);
    }
}
