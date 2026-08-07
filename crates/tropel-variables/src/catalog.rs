use rand::RngExt;
use regex::Regex;

/// Compile a regex ONCE per process (lazily) and return a reference to the
/// cached instance. The old code called `Regex::new` on EVERY `resolve` for
/// every one of the ~30 patterns — plus ~30 full-string `contains` marker
/// scans — even when the input had no dynamic variables at all. Compiled
/// once, a `Regex` is reused by all threads/VUs (it is `Sync`); the `$` fast
/// path below skips even the marker scans for the common no-`$` case.
macro_rules! cached_re {
    ($name:ident, $pattern:literal) => {{
        static $name: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        $name.get_or_init(|| {
            regex::Regex::new($pattern).expect("valid dynamic-variable regex")
        })
    }};
}

/// Cap for `:length` / `:count` arguments on dynamic variables (backlog line
/// 162). Postman's own values are single-digit-to-low-hundreds; a collection
/// author typo like `{{$randomString:9999999999}}` previously parsed into an
/// UNBOUNDED `usize` and `(0..length)` attempted a ~10 GB allocation →
/// `handle_alloc_error` → process abort, not a recoverable error. Clamping to
/// this cap keeps resolution bounded (and the substitution complete) instead
/// of killing the run.
const MAX_DYNAMIC_LENGTH: usize = 10_000;

/// Parse a `:length` / `:count` capture, clamping to [`MAX_DYNAMIC_LENGTH`].
/// Unparseable or missing captures fall back to `default` (the variable's
/// built-in size), matching Postman.
fn capped_len(raw: Option<&str>, default: usize) -> usize {
    match raw {
        // The capture regex only feeds digits, so a parse failure here means
        // the value OVERFLOWED usize — the worst attack case. Clamp it to the
        // cap rather than silently falling back to a small default.
        Some(s) => s
            .parse::<usize>()
            .map(|n| n.min(MAX_DYNAMIC_LENGTH))
            .unwrap_or(MAX_DYNAMIC_LENGTH),
        None => default,
    }
}

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
        // Fast path: no `$` means no dynamic variable anywhere — return
        // unchanged. This is the common case (plain URLs, headers, bodies
        // with scoped `{{var}}` refs only) and it skips the ~30 marker
        // `contains` scans and all regex work entirely.
        if !s.contains('$') {
            return s.to_string();
        }
        let mut result = s.to_string();
        let mut rng = rand::rng();

        // {{$guid}} — fresh UUID per occurrence
        if result.contains("{{$guid}}") {
            let re = cached_re!(RE_GUID, r"\{\{\$guid\}\}");
            result = self.replace_with_func(&result, re, |_| uuid::Uuid::new_v4().to_string());
        }

        // {{$timestamp}} — fresh Unix timestamp per occurrence
        if result.contains("{{$timestamp}}") {
            let re = cached_re!(RE_TIMESTAMP, r"\{\{\$timestamp\}\}");
            result = self.replace_with_func(&result, re, |_| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
                    .to_string()
            });
        }

        // {{$isoTimestamp}} — fresh ISO timestamp per occurrence
        if result.contains("{{$isoTimestamp}}") {
            let re = cached_re!(RE_ISO_TIMESTAMP, r"\{\{\$isoTimestamp\}\}");
            result = self.replace_with_func(&result, re, |_| chrono_now_iso());
        }

        // {{$randomUUID}} — fresh UUID per occurrence
        if result.contains("{{$randomUUID}}") {
            let re = cached_re!(RE_RANDOM_UUID, r"\{\{\$randomUUID\}\}");
            result = self.replace_with_func(&result, re, |_| uuid::Uuid::new_v4().to_string());
        }

        // {{$randomInt}} — fresh random integer per occurrence
        if result.contains("{{$randomInt}}") {
            let re = cached_re!(RE_RANDOM_INT, r"\{\{\$randomInt\}\}");
            result =
                self.replace_with_func(&result, re, |_| rng.random_range(0..1000u32).to_string());
        }

        // {{$randomFloat}} — fresh random float per occurrence
        if result.contains("{{$randomFloat}}") {
            let re = cached_re!(RE_RANDOM_FLOAT, r"\{\{\$randomFloat\}\}");
            result = self.replace_with_func(&result, re, |_| {
                format!("{:.6}", rng.random::<f64>() * 1000.0)
            });
        }

        // {{$randomString[:length]}}
        if result.contains("{{$randomString") {
            let re = cached_re!(RE_RANDOM_STRING, r"\{\{\$randomString(?::(\d+))?\}\}");
            result = self.replace_with_func(&result, &re, |caps| {
                let len = capped_len(caps.get(1).map(|m| m.as_str()), 10);
                random_string(
                    &mut rng,
                    len,
                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
                )
            });
        }

        // {{$randomAlphabetic[:length]}}
        if result.contains("{{$randomAlphabetic") {
            let re = cached_re!(RE_RANDOM_ALPHABETIC, r"\{\{\$randomAlphabetic(?::(\d+))?\}\}");
            result = self.replace_with_func(&result, &re, |caps| {
                let len = capped_len(caps.get(1).map(|m| m.as_str()), 10);
                random_string(
                    &mut rng,
                    len,
                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
                )
            });
        }

        // {{$randomAlphanumeric[:length]}}
        if result.contains("{{$randomAlphanumeric") {
            let re = cached_re!(RE_RANDOM_ALPHANUMERIC, r"\{\{\$randomAlphanumeric(?::(\d+))?\}\}");
            result = self.replace_with_func(&result, &re, |caps| {
                let len = capped_len(caps.get(1).map(|m| m.as_str()), 10);
                random_string(
                    &mut rng,
                    len,
                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
                )
            });
        }

        // {{$randomBoolean}} — fresh random bool per occurrence
        if result.contains("{{$randomBoolean}}") {
            let re = cached_re!(RE_RANDOM_BOOLEAN, r"\{\{\$randomBoolean\}\}");
            result = self.replace_with_func(&result, &re, |_| rng.random_bool(0.5).to_string());
        }

        // {{$randomHex[:length]}}
        if result.contains("{{$randomHex") {
            let re = cached_re!(RE_RANDOM_HEX, r"\{\{\$randomHex(?::(\d+))?\}\}");
            result = self.replace_with_func(&result, &re, |caps| {
                let len = capped_len(caps.get(1).map(|m| m.as_str()), 8);
                random_string(&mut rng, len, "0123456789abcdef")
            });
        }

        // {{$randomEmail}} — fresh email per occurrence
        if result.contains("{{$randomEmail}}") {
            let re = cached_re!(RE_RANDOM_EMAIL, r"\{\{\$randomEmail\}\}");
            result = self.replace_with_func(&result, &re, |_| random_email(&mut rng));
        }

        // {{$randomPhone}} / {{$randomPhoneNumber}}
        if result.contains("{{$randomPhone") {
            let re = cached_re!(RE_RANDOM_PHONE, r"\{\{\$randomPhone(?:Number)?\}\}");
            result = self.replace_with_func(&result, &re, |_| random_phone_number(&mut rng));
        }

        // {{$randomCompany}} / {{$randomCompanyName}}
        if result.contains("{{$randomCompany") {
            let re = cached_re!(RE_RANDOM_COMPANY, r"\{\{\$randomCompany(?:Name)?\}\}");
            result = self.replace_with_func(&result, &re, |_| random_company_name(&mut rng));
        }

        // {{$randomLorem}} — one paragraph of lorem-style text
        if result.contains("{{$randomLorem}}") {
            let re = cached_re!(RE_RANDOM_LOREM, r"\{\{\$randomLorem\}\}");
            result = self.replace_with_func(&result, &re, |_| random_lorem_paragraph(&mut rng));
        }

        // {{$randomSentence}} / {{$randomWords[:count]}} / {{$randomWord}}
        if result.contains("{{$randomSentence}}") {
            let re = cached_re!(RE_RANDOM_SENTENCE, r"\{\{\$randomSentence\}\}");
            result = self.replace_with_func(&result, &re, |_| random_sentence(&mut rng));
        }
        if result.contains("{{$randomWords") {
            let re = cached_re!(RE_RANDOM_WORDS, r"\{\{\$randomWords(?::(\d+))?\}\}");
            result = self.replace_with_func(&result, &re, |caps| {
                let count = capped_len(caps.get(1).map(|m| m.as_str()), 5);
                random_words(&mut rng, count)
            });
        }
        if result.contains("{{$randomWord}}") {
            let re = cached_re!(RE_RANDOM_WORD, r"\{\{\$randomWord\}\}");
            result = self.replace_with_func(&result, &re, |_| random_word(&mut rng));
        }

        // {{$randomDate}} / {{$randomDatePast}} / {{$randomDateFuture}} / {{$randomTime}}
        if result.contains("{{$randomDatePast}}") {
            let re = cached_re!(RE_RANDOM_DATE_PAST, r"\{\{\$randomDatePast\}\}");
            result = self.replace_with_func(&result, &re, |_| random_date_past(&mut rng));
        }
        if result.contains("{{$randomDateFuture}}") {
            let re = cached_re!(RE_RANDOM_DATE_FUTURE, r"\{\{\$randomDateFuture\}\}");
            result = self.replace_with_func(&result, &re, |_| random_date_future(&mut rng));
        }
        if result.contains("{{$randomDate}}") {
            let re = cached_re!(RE_RANDOM_DATE, r"\{\{\$randomDate\}\}");
            result = self.replace_with_func(&result, &re, |_| random_date(&mut rng));
        }
        if result.contains("{{$randomTime}}") {
            let re = cached_re!(RE_RANDOM_TIME, r"\{\{\$randomTime\}\}");
            result = self.replace_with_func(&result, &re, |_| random_time(&mut rng));
        }

        // {{$randomIP}} — fresh IP per occurrence
        if result.contains("{{$randomIP}}") {
            let re = cached_re!(RE_RANDOM_IP, r"\{\{\$randomIP\}\}");
            result = self.replace_with_func(&result, &re, |_| {
                format!(
                    "{}.{}.{}.{}",
                    rng.random_range(1..255u32),
                    rng.random_range(0..255u32),
                    rng.random_range(0..255u32),
                    rng.random_range(1..255u32)
                )
            });
        }

        // {{$randomCity}}, {{$randomCountry}}, {{$randomStreet}}, {{$randomPostcode}},
        // {{$randomNameFullName}}, {{$randomNameFirstName}}, {{$randomNameLastName}},
        // {{$randomName}}, {{$randomColor}}, {{$randomMAC}}
        if result.contains("{{$randomCity}}") {
            let re = cached_re!(RE_RANDOM_CITY, r"\{\{\$randomCity\}\}");
            result = self.replace_with_func(&result, &re, |_| random_city(&mut rng));
        }
        if result.contains("{{$randomCountry}}") {
            let re = cached_re!(RE_RANDOM_COUNTRY, r"\{\{\$randomCountry\}\}");
            result = self.replace_with_func(&result, &re, |_| random_country(&mut rng));
        }
        if result.contains("{{$randomStreet}}") {
            let re = cached_re!(RE_RANDOM_STREET, r"\{\{\$randomStreet\}\}");
            result = self.replace_with_func(&result, &re, |_| random_street(&mut rng));
        }
        if result.contains("{{$randomPostcode}}") {
            let re = cached_re!(RE_RANDOM_POSTCODE, r"\{\{\$randomPostcode\}\}");
            result = self.replace_with_func(&result, &re, |_| random_postcode(&mut rng));
        }
        if result.contains("{{$randomName}}") {
            // Note: {{$randomName}} is the base pattern; longer forms like
            // {{$randomNameFullName}} are handled later with more specific regexes.
            let re = cached_re!(RE_RANDOM_NAME, r"\{\{\$randomName\}\}");
            result = self.replace_with_func(&result, &re, |_| random_full_name(&mut rng));
        }
        if result.contains("{{$randomNameFullName}}") {
            let re = cached_re!(RE_RANDOM_NAME_FULL, r"\{\{\$randomNameFullName\}\}");
            result = self.replace_with_func(&result, &re, |_| random_full_name(&mut rng));
        }
        if result.contains("{{$randomNameFirstName}}") {
            let re = cached_re!(RE_RANDOM_NAME_FIRST, r"\{\{\$randomNameFirstName\}\}");
            result = self.replace_with_func(&result, &re, |_| random_first_name(&mut rng));
        }
        if result.contains("{{$randomNameLastName}}") {
            let re = cached_re!(RE_RANDOM_NAME_LAST, r"\{\{\$randomNameLastName\}\}");
            result = self.replace_with_func(&result, &re, |_| random_last_name(&mut rng));
        }
        if result.contains("{{$randomColor}}") {
            let re = cached_re!(RE_RANDOM_COLOR, r"\{\{\$randomColor\}\}");
            result = self.replace_with_func(&result, &re, |_| random_color(&mut rng));
        }
        if result.contains("{{$randomMAC}}") {
            let re = cached_re!(RE_RANDOM_MAC, r"\{\{\$randomMAC\}\}");
            result = self.replace_with_func(&result, &re, |_| {
                let hex = random_string(&mut rng, 12, "0123456789abcdef");
                hex.chars()
                    .collect::<Vec<_>>()
                    .chunks(2)
                    .map(|c| c.iter().collect::<String>())
                    .collect::<Vec<_>>()
                    .join(":")
            });
        }

        // {{$randomPassword[:length]}}
        if result.contains("{{$randomPassword") {
            let re = cached_re!(RE_RANDOM_PASSWORD, r"\{\{\$randomPassword(?::(\d+))?\}\}");
            result = self.replace_with_func(&result, &re, |caps| {
                let len = capped_len(caps.get(1).map(|m| m.as_str()), 12);
                random_string(
                    &mut rng,
                    len,
                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*",
                )
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

const FIRST_NAMES: &[&str] = &[
    "Ava",
    "Liam",
    "Noah",
    "Emma",
    "Olivia",
    "Elijah",
    "Sophia",
    "Mia",
    "Charlotte",
    "Amelia",
    "James",
    "Benjamin",
    "Lucas",
    "Ethan",
    "Harper",
    "Evelyn",
    "Abigail",
    "William",
    "Henry",
    "Ella",
];

const LAST_NAMES: &[&str] = &[
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
];

const COMPANY_NAMES: &[&str] = &[
    "Acme Corporation",
    "Globex Corporation",
    "Initech",
    "Stark Industries",
    "Wayne Enterprises",
    "Umbrella Corporation",
    "Cyberdyne Systems",
    "Massive Dynamic",
    "Wonka Industries",
    "Blue Origin",
    "Aperture Labs",
    "Pioneer Logistics",
    "Evergreen Technologies",
    "TrueNorth Consulting",
    "Redwood Analytics",
    "Summit Systems",
    "Liberty Software",
    "Silverline Media",
    "Veridian Dynamics",
    "Northstar Financial",
];

const CITY_NAMES: &[&str] = &[
    "New York",
    "London",
    "Paris",
    "Tokyo",
    "Berlin",
    "Sydney",
    "Toronto",
    "San Francisco",
    "Chicago",
    "Barcelona",
    "Amsterdam",
    "Singapore",
    "Dubai",
    "Los Angeles",
    "Seattle",
    "Dublin",
    "Vienna",
    "Cape Town",
    "Mumbai",
    "Helsinki",
];

const COUNTRY_NAMES: &[&str] = &[
    "United States",
    "Canada",
    "United Kingdom",
    "Australia",
    "Germany",
    "France",
    "Japan",
    "Spain",
    "Italy",
    "Netherlands",
    "Sweden",
    "Norway",
    "Brazil",
    "Mexico",
    "India",
    "Singapore",
    "South Africa",
    "Switzerland",
    "Austria",
    "Ireland",
];

const STREET_NAMES: &[&str] = &[
    "Maple", "Oak", "Pine", "Cedar", "Elm", "Walnut", "Chestnut", "Birch", "Willow", "Aspen",
    "Sunset", "River", "Hill", "Grove", "Park", "Meadow", "Lake", "Forest", "Jackson", "Lincoln",
];

const STREET_SUFFIXES: &[&str] = &[
    "Street",
    "Avenue",
    "Boulevard",
    "Lane",
    "Drive",
    "Court",
    "Place",
    "Terrace",
    "Way",
    "Row",
];

const EMAIL_DOMAINS: &[&str] = &[
    "example.com",
    "example.org",
    "mail.com",
    "test.com",
    "acme.com",
    "globex.com",
    "true-north.com",
    "evergreen.io",
];

const LOREM_WORDS: &[&str] = &[
    "lorem",
    "ipsum",
    "dolor",
    "sit",
    "amet",
    "consectetur",
    "adipiscing",
    "elit",
    "sed",
    "do",
    "eiusmod",
    "tempor",
    "incididunt",
    "ut",
    "labore",
    "et",
    "dolore",
    "magna",
    "aliqua",
    "enim",
    "ad",
    "minim",
    "veniam",
    "quis",
    "nostrud",
    "exercitation",
    "ullamco",
    "laboris",
    "nisi",
    "ut",
];

fn random_choice<'a, R: RngExt>(rng: &mut R, items: &'a [&'a str]) -> &'a str {
    // Empty input must not panic (backlog P3): an empty slice would make
    // `random_range(0..0)` panic. Return an empty string instead.
    if items.is_empty() {
        return "";
    }
    items[rng.random_range(0..items.len())]
}

fn random_word<R: RngExt>(rng: &mut R) -> String {
    random_choice(rng, LOREM_WORDS).to_string()
}

fn random_words<R: RngExt>(rng: &mut R, count: usize) -> String {
    (0..count)
        .map(|_| random_choice(rng, LOREM_WORDS))
        .collect::<Vec<_>>()
        .join(" ")
}

fn random_sentence<R: RngExt>(rng: &mut R) -> String {
    let count = rng.random_range(5..12);
    let sentence = random_words(rng, count);
    capitalize_first_letter(sentence) + "."
}

fn random_lorem_paragraph<R: RngExt>(rng: &mut R) -> String {
    let sentences = rng.random_range(2..5);
    (0..sentences)
        .map(|_| random_sentence(rng))
        .collect::<Vec<_>>()
        .join(" ")
}

fn random_email<R: RngExt>(rng: &mut R) -> String {
    let first = random_first_name(rng).to_lowercase();
    let last = random_last_name(rng).to_lowercase();
    let domain = random_choice(rng, EMAIL_DOMAINS);
    match rng.random_range(0..3) {
        0 => format!("{}@{}", first, domain),
        1 => format!("{}.{}@{}", first, last, domain),
        _ => format!("{}{}@{}", first, rng.random_range(1..100), domain),
    }
}

fn random_phone_number<R: RngExt>(rng: &mut R) -> String {
    format!(
        "({}) {}-{}",
        rng.random_range(200..999),
        rng.random_range(200..999),
        rng.random_range(1000..10000)
    )
}

fn random_company_name<R: RngExt>(rng: &mut R) -> String {
    random_choice(rng, COMPANY_NAMES).to_string()
}

fn random_city<R: RngExt>(rng: &mut R) -> String {
    random_choice(rng, CITY_NAMES).to_string()
}

fn random_country<R: RngExt>(rng: &mut R) -> String {
    random_choice(rng, COUNTRY_NAMES).to_string()
}

fn random_street<R: RngExt>(rng: &mut R) -> String {
    format!(
        "{} {} {}",
        rng.random_range(100..9999),
        random_choice(rng, STREET_NAMES),
        random_choice(rng, STREET_SUFFIXES)
    )
}

fn random_postcode<R: RngExt>(rng: &mut R) -> String {
    format!("{:05}", rng.random_range(10000..100000))
}

fn random_full_name<R: RngExt>(rng: &mut R) -> String {
    format!("{} {}", random_first_name(rng), random_last_name(rng))
}

fn random_first_name<R: RngExt>(rng: &mut R) -> String {
    random_choice(rng, FIRST_NAMES).to_string()
}

fn random_last_name<R: RngExt>(rng: &mut R) -> String {
    random_choice(rng, LAST_NAMES).to_string()
}

fn random_color<R: RngExt>(rng: &mut R) -> String {
    format!("{:06x}", rng.random::<u32>() & 0xFFFFFF)
}

fn random_date<R: RngExt>(rng: &mut R) -> String {
    let start = chrono::NaiveDate::from_ymd_opt(1990, 1, 1).unwrap();
    let end = chrono::NaiveDate::from_ymd_opt(2035, 12, 31).unwrap();
    random_date_in_range(rng, start, end)
}

fn random_date_past<R: RngExt>(rng: &mut R) -> String {
    let now = chrono::Utc::now().date_naive();
    let start = now - chrono::Duration::days(3650);
    random_date_in_range(rng, start, now)
}

fn random_date_future<R: RngExt>(rng: &mut R) -> String {
    let now = chrono::Utc::now().date_naive();
    let end = now + chrono::Duration::days(3650);
    random_date_in_range(rng, now, end)
}

fn random_time<R: RngExt>(rng: &mut R) -> String {
    format!(
        "{:02}:{:02}:{:02}",
        rng.random_range(0..24),
        rng.random_range(0..60),
        rng.random_range(0..60)
    )
}

fn random_date_in_range<R: RngExt>(
    rng: &mut R,
    start: chrono::NaiveDate,
    end: chrono::NaiveDate,
) -> String {
    let days = (end - start).num_days();
    let offset = if days <= 0 {
        0
    } else {
        rng.random_range(0..=days as u32) as i64
    };
    let date = start + chrono::Duration::days(offset);
    date.format("%Y-%m-%d").to_string()
}

fn capitalize_first_letter(text: String) -> String {
    let mut chars = text.chars();
    match chars.next() {
        None => text,
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

fn random_string(rng: &mut impl RngExt, length: usize, charset: &str) -> String {
    let chars: Vec<char> = charset.chars().collect();
    // Empty charset must not panic (backlog P3): `random_range(0..0)` panics.
    if chars.is_empty() {
        return String::new();
    }
    (0..length)
        .map(|_| chars[rng.random_range(0..chars.len())])
        .collect()
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
        let guid = result
            .trim_start_matches("prefix-")
            .trim_end_matches("-suffix");
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
        assert_ne!(
            parts[0], parts[1],
            "{{$guid}}-{{$guid}} should produce two different values"
        );
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

    #[test]
    fn test_huge_length_clamped_not_abort() {
        // Backlog line 162: `{{$randomString:9999999999}}` parsed into an
        // UNBOUNDED usize and `(0..length)` attempted a ~10 GB allocation →
        // handle_alloc_error → process abort. Every :length/:count must be
        // clamped to MAX_DYNAMIC_LENGTH so resolution completes instead of
        // killing the process.
        let catalog = DynamicCatalog::new();

        let s = catalog.resolve("x={{$randomString:9999999999}}");
        assert_eq!(s.len(), "x=".len() + MAX_DYNAMIC_LENGTH);
        assert!(!s.contains("{{$"));

        let hex = catalog.resolve("h={{$randomHex:9999999999}}");
        assert_eq!(hex.len(), "h=".len() + MAX_DYNAMIC_LENGTH);

        let pwd = catalog.resolve("p={{$randomPassword:9999999999}}");
        assert_eq!(pwd.len(), "p=".len() + MAX_DYNAMIC_LENGTH);

        let alpha = catalog.resolve("a={{$randomAlphabetic:9999999999}}");
        assert_eq!(alpha.len(), "a=".len() + MAX_DYNAMIC_LENGTH);

        let alnum = catalog.resolve("n={{$randomAlphanumeric:9999999999}}");
        assert_eq!(alnum.len(), "n=".len() + MAX_DYNAMIC_LENGTH);

        let words = catalog.resolve("w={{$randomWords:9999999999}}");
        // 10k words (each >= 5 chars + spaces) — bounded, no abort; the
        // count itself is what is clamped.
        assert!(words.starts_with("w="));
        assert!(!words.contains("{{$"));
        assert!(words.len() < MAX_DYNAMIC_LENGTH * 16);
    }

    #[test]
    fn test_moderate_length_still_honored() {
        // The cap must NOT change legitimate sizes below the limit.
        let catalog = DynamicCatalog::new();
        let s = catalog.resolve("x={{$randomString:64}}");
        assert_eq!(s.len(), "x=".len() + 64);
        // "w=" + 7 words joined by spaces → 7 space-separated tokens.
        let words = catalog.resolve("w={{$randomWords:7}}");
        assert_eq!(words.split(' ').count(), 7);
    }

    #[test]
    fn test_capped_len_helper() {
        assert_eq!(capped_len(Some("5"), 10), 5);
        // Overflowing usize parse (the regex only feeds digits, so a parse
        // failure IS an overflow) clamps to the cap — the attack case.
        assert_eq!(capped_len(Some("999999999999999999999999"), 10), MAX_DYNAMIC_LENGTH);
        assert_eq!(capped_len(Some("abc"), 8), MAX_DYNAMIC_LENGTH, "any parse failure clamps to the cap");
        assert_eq!(capped_len(None, 12), 12);
    }

    #[test]
    fn test_random_phone_number() {
        let catalog = DynamicCatalog::new();
        let result = catalog.resolve("tel={{$randomPhoneNumber}}");
        assert!(!result.contains("{{$randomPhoneNumber}}"));
        assert!(result.starts_with("tel=("));
    }

    #[test]
    fn test_random_company_name() {
        let catalog = DynamicCatalog::new();
        let result = catalog.resolve("company={{$randomCompany}}");
        assert!(!result.contains("{{$randomCompany}}"));
        assert!(result.contains(' '));
    }

    #[test]
    fn test_random_lorem_paragraph() {
        let catalog = DynamicCatalog::new();
        let result = catalog.resolve("text={{$randomLorem}}");
        assert!(!result.contains("{{$randomLorem}}"));
        assert!(result.ends_with('.'));
        assert!(result.contains(' '));
    }

    #[test]
    fn test_random_date_variants() {
        let catalog = DynamicCatalog::new();
        let past = catalog.resolve("past={{$randomDatePast}}");
        let future = catalog.resolve("future={{$randomDateFuture}}");
        let date = catalog.resolve("date={{$randomDate}}");
        let time = catalog.resolve("time={{$randomTime}}");

        assert!(!past.contains("{{$randomDatePast}}"));
        assert!(!future.contains("{{$randomDateFuture}}"));
        assert!(!date.contains("{{$randomDate}}"));
        assert!(!time.contains("{{$randomTime}}"));
        assert!(past.starts_with("past="));
        assert!(future.starts_with("future="));
        assert!(date.starts_with("date="));
        assert!(time.starts_with("time="));
    }

    #[test]
    fn random_choice_empty_input_does_not_panic() {
        // Backlog P3: an empty items slice made `random_range(0..0)` panic
        // (unwinding through the variable resolver). Must return "" instead.
        let mut rng = rand::rng();
        let empty: [&str; 0] = [];
        assert_eq!(random_choice(&mut rng, &empty), "");
        // Non-empty input still works.
        assert_eq!(random_choice(&mut rng, &["a"]), "a");
    }

    #[test]
    fn random_string_empty_charset_does_not_panic() {
        // Backlog P3: an empty charset made `random_range(0..0)` panic.
        let mut rng = rand::rng();
        assert_eq!(random_string(&mut rng, 10, ""), "");
        assert_eq!(random_string(&mut rng, 0, "abc"), "");
        let s = random_string(&mut rng, 5, "ab");
        assert_eq!(s.len(), 5);
        assert!(s.chars().all(|c| c == 'a' || c == 'b'));
    }
}
