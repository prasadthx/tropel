//! k6-compatible DNS configuration: static `hosts` map, `blacklistIPs`
//! CIDRs, `dns.ttl` caching, `dns.select` address selection and
//! `dns.policy` address-family selection.
//!
//! Implemented as a custom [`reqwest::dns::Resolve`] that wraps the real
//! lookup (tokio `lookup_host`, the same getaddrinfo path reqwest's default
//! GaiResolver uses) and applies the configured options. Real lookup time is
//! still recorded into the thread-local sub-timing slot, so the `dns` phase
//! measurement is preserved (cache hits and static-host entries report zero).

use crate::client::parse_duration;
use crate::subtimings::record_dns;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tropel_core::config::HttpConfig;

/// reqwest's `Resolving` error type: boxed error that is `Send + Sync`.
/// Plain `Box::new(e)` yields `Box<dyn Error>` which does not satisfy the
/// trait bound, so every error return must go through this alias.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// How long resolved addresses are cached (k6 `dns.ttl`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DnsCacheMode {
    /// No caching — every request resolves (k6 `ttl: "0"` / unset).
    Off,
    /// Cache for a fixed duration (k6 `ttl: "1m"`, `"5m"`, …).
    Ttl(Duration),
    /// Cache forever (k6 `ttl: "inf"`).
    Forever,
}

/// Address selection policy (k6 `dns.select`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DnsSelect {
    /// Use the resolved addresses in order (first wins) — the default.
    #[default]
    First,
    /// Rotate the start of the address list on each lookup.
    RoundRobin,
    /// Pseudo-randomly rotate the address list on each lookup.
    Random,
}

/// Address-family policy (k6 `dns.policy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DnsPolicy {
    /// Keep the resolver's address order.
    #[default]
    Any,
    /// Prefer IPv4 addresses (stable sort, v4 first).
    PreferV4,
    /// Prefer IPv6 addresses (stable sort, v6 first).
    PreferV6,
    /// Only use IPv4.
    OnlyV4,
    /// Only use IPv6.
    OnlyV6,
}

/// A CIDR block or single IP used for blacklisting (k6 `blacklistIPs`).
#[derive(Debug, Clone, Copy)]
pub struct IpCidr {
    base: IpAddr,
    prefix: u8,
}

impl IpCidr {
    /// Parse `"10.0.0.0/8"`, `"192.168.1.5"`, `"::1/128"`. A bare IP gets a
    /// full-length prefix (32 for v4, 128 for v6). Returns `None` on invalid
    /// input, including overlong prefixes (`10.0.0.0/99`, `::1/200`) that
    /// would silently behave as full-length masks.
    pub fn parse(s: &str) -> Option<IpCidr> {
        let s = s.trim();
        let (ip_part, prefix) = match s.split_once('/') {
            Some((ip, p)) => (ip, p.trim().parse::<u8>().ok()?),
            None => (s, if s.contains(':') { 128 } else { 32 }),
        };
        let ip: IpAddr = ip_part.trim().parse().ok()?;
        // Validate the prefix against the address family: an overlong prefix
        // is invalid input, not a full-length mask.
        let max_prefix = match ip {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix > max_prefix {
            return None;
        }
        Some(IpCidr { base: ip, prefix })
    }

    /// Whether `ip` falls inside this CIDR.
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.base, ip) {
            (IpAddr::V4(base), IpAddr::V4(ip)) => {
                let mask = if self.prefix >= 32 {
                    u32::MAX
                } else {
                    u32::MAX << (32 - self.prefix)
                };
                (u32::from(base) & mask) == (u32::from(ip) & mask)
            }
            (IpAddr::V6(base), IpAddr::V6(ip)) => {
                let mask = if self.prefix >= 128 {
                    u128::MAX
                } else {
                    u128::MAX << (128 - self.prefix)
                };
                (u128::from(base) & mask) == (u128::from(ip) & mask)
            }
            _ => false,
        }
    }
}

#[derive(Debug)]
struct CacheEntry {
    addrs: Vec<SocketAddr>,
    expires_at: Option<Instant>,
}

/// Shared resolver state (cloned into the boxed resolve future).
#[derive(Debug)]
struct DnsShared {
    cache: DnsCacheMode,
    select: DnsSelect,
    policy: DnsPolicy,
    hosts: HashMap<String, Vec<IpAddr>>,
    blacklist: Vec<IpCidr>,
    cache_store: Mutex<HashMap<String, CacheEntry>>,
    round_robin: AtomicUsize,
}

/// reqwest DNS resolver implementing k6-compatible DNS options.
#[derive(Debug, Clone)]
pub struct DnsResolver {
    inner: Arc<DnsShared>,
}

impl DnsResolver {
    /// Build a resolver from the job's `HttpConfig`. Invalid option values
    /// fall back to sensible defaults with a warning (never a hard error, so
    /// a misconfigured `dns` block can't kill a run).
    pub fn from_config(config: &HttpConfig) -> DnsResolver {
        let cache = parse_cache_mode(config.dns_ttl.as_deref());
        let select = match config.dns_select.as_deref().map(str::to_ascii_lowercase).as_deref() {
            Some("roundrobin" | "round_robin" | "round-robin") => DnsSelect::RoundRobin,
            Some("random") => DnsSelect::Random,
            other => {
                if let Some(v) = other {
                    tracing::warn!("unknown dns.select '{v}' — using 'first'");
                }
                DnsSelect::First
            }
        };
        let policy = match config
            .dns_policy
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("preferipv4" | "prefer_ipv4") => DnsPolicy::PreferV4,
            Some("preferipv6" | "prefer_ipv6") => DnsPolicy::PreferV6,
            Some("onlyipv4" | "only_ipv4") => DnsPolicy::OnlyV4,
            Some("onlyipv6" | "only_ipv6") => DnsPolicy::OnlyV6,
            other => {
                if let Some(v) = other {
                    tracing::warn!("unknown dns.policy '{v}' — using 'any'");
                }
                DnsPolicy::Any
            }
        };
        let hosts = parse_hosts(&config.hosts);
        let blacklist: Vec<IpCidr> = config
            .blacklist_ips
            .iter()
            .filter_map(|s| {
                IpCidr::parse(s).or_else(|| {
                    tracing::warn!("invalid blacklistIPs entry '{s}' — ignored");
                    None
                })
            })
            .collect();

        DnsResolver {
            inner: Arc::new(DnsShared {
                cache,
                select,
                policy,
                hosts,
                blacklist,
                cache_store: Mutex::new(HashMap::new()),
                round_robin: AtomicUsize::new(0),
            }),
        }
    }
}

impl Resolve for DnsResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            // 1. Static hosts map (exact or wildcard) — no DNS involved. The
            //    blacklist still applies: an explicit host override must not
            //    smuggle connections to a blocked network.
            if let Some(ips) = hosts_lookup(&inner.hosts, &host) {
                record_dns(Duration::ZERO);
                let mut addrs: Vec<SocketAddr> =
                    ips.into_iter().map(|ip| SocketAddr::new(ip, 0)).collect();
                if !inner.blacklist.is_empty() {
                    let before = addrs.len();
                    addrs.retain(|a| !inner.blacklist.iter().any(|c| c.contains(a.ip())));
                    if before > 0 && addrs.is_empty() {
                        return Err(BoxError::from(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            format!("static host '{host}' resolves only to blacklisted addresses"),
                        )));
                    }
                }
                return Ok(box_addrs(addrs));
            }

            // 2. TTL cache hit? The cached list is re-selected on every hit so
            //    `dns.select` (roundRobin/random) keeps rotating across VUs
            //    and cache hits instead of every VU hammering the same first
            //    IP for the whole TTL window.
            if let Some(entry) = cache_get(&inner, &host) {
                record_dns(Duration::ZERO);
                let rotated = select_addrs(&entry, inner.select, &inner.round_robin);
                return Ok(box_addrs(rotated));
            }

            // 3. Real lookup (port 0: hyper-util applies the request's port).
            let start = Instant::now();
            let result = tokio::net::lookup_host((host.clone(), 0)).await;
            record_dns(start.elapsed());
            let mut addrs: Vec<SocketAddr> = match result {
                Ok(it) => it.collect(),
                Err(e) => return Err(BoxError::from(e)),
            };
            if addrs.is_empty() {
                return Err(BoxError::from(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("no addresses found for '{host}'"),
                )));
            }

            // 4. Address-family policy. If `onlyIPv4`/`onlyIPv6` filtered
            //    every address, fail clearly instead of handing reqwest an
            //    empty list (whose error message would be misleading).
            apply_policy(&mut addrs, inner.policy);
            if addrs.is_empty() {
                return Err(BoxError::from(std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    format!(
                        "no addresses for '{host}' match dns.policy {:?}",
                        inner.policy
                    ),
                )));
            }

            // 5. Blacklist filter. If the lookup returned addresses but every
            //    one of them is blacklisted, the request must fail loudly —
            //    this is how k6 surfaces a blocked host.
            if !inner.blacklist.is_empty() {
                let before = addrs.len();
                addrs.retain(|a| !inner.blacklist.iter().any(|c| c.contains(a.ip())));
                if before > 0 && addrs.is_empty() {
                    return Err(BoxError::from(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        format!("all resolved addresses for '{host}' are blacklisted"),
                    )));
                }
            }

            // 6. Selection policy (rotates the list; the connector still tries
            //    the first address, then falls through to the rest). Never
            //    cache an empty result — a transient resolution failure
            //    shouldn't poison the cache for the whole TTL.
            let chosen = select_addrs(&addrs, inner.select, &inner.round_robin);
            if !chosen.is_empty() {
                cache_put(&inner, &host, &chosen);
            }

            Ok(box_addrs(chosen))
        })
    }
}

fn box_addrs(addrs: Vec<SocketAddr>) -> Addrs {
    Box::new(addrs.into_iter())
}

fn parse_cache_mode(s: Option<&str>) -> DnsCacheMode {
    match s {
        None => DnsCacheMode::Off,
        Some(raw) => {
            let t = raw.trim().to_ascii_lowercase();
            match t.as_str() {
                "0" | "0s" | "off" | "none" | "disabled" => DnsCacheMode::Off,
                "inf" | "infinite" | "forever" => DnsCacheMode::Forever,
                _ => match parse_duration(&t) {
                    Ok(d) => DnsCacheMode::Ttl(d),
                    Err(_) => {
                        tracing::warn!("invalid dns.ttl '{t}' — caching disabled");
                        DnsCacheMode::Off
                    }
                },
            }
        }
    }
}

fn parse_hosts(map: &HashMap<String, String>) -> HashMap<String, Vec<IpAddr>> {
    let mut out = HashMap::new();
    for (host, value) in map {
        let ips: Vec<IpAddr> = value
            .split(',')
            .filter_map(|s| {
                let s = s.trim();
                let parsed = s.parse::<IpAddr>().ok();
                if parsed.is_none() && !s.is_empty() {
                    tracing::warn!("hosts entry '{host}' has invalid IP '{s}' — skipped");
                }
                parsed
            })
            .collect();
        if ips.is_empty() {
            if !value.trim().is_empty() {
                tracing::warn!("hosts entry '{host}' has no valid IPs — ignored");
            }
        } else {
            out.insert(host.trim().to_ascii_lowercase(), ips);
        }
    }
    out
}

/// Exact host lookup first, then `*.domain` wildcard keys.
fn hosts_lookup(hosts: &HashMap<String, Vec<IpAddr>>, host: &str) -> Option<Vec<IpAddr>> {
    let h = host.to_ascii_lowercase();
    if let Some(v) = hosts.get(&h) {
        return Some(v.clone());
    }
    for (key, v) in hosts {
        if let Some(suffix) = key.strip_prefix("*.") {
            let dot_suffix = format!(".{suffix}");
            if h.len() > dot_suffix.len() && h.ends_with(&dot_suffix) {
                return Some(v.clone());
            }
        }
    }
    None
}

fn apply_policy(addrs: &mut Vec<SocketAddr>, policy: DnsPolicy) {
    match policy {
        DnsPolicy::Any => {}
        DnsPolicy::PreferV4 => addrs.sort_by_key(|a| a.is_ipv6()),
        DnsPolicy::PreferV6 => addrs.sort_by_key(|a| a.is_ipv4()),
        DnsPolicy::OnlyV4 => addrs.retain(|a| a.is_ipv4()),
        DnsPolicy::OnlyV6 => addrs.retain(|a| a.is_ipv6()),
    }
}

fn select_addrs(addrs: &[SocketAddr], select: DnsSelect, counter: &AtomicUsize) -> Vec<SocketAddr> {
    match select {
        DnsSelect::First => addrs.to_vec(),
        DnsSelect::RoundRobin | DnsSelect::Random => {
            if addrs.len() <= 1 {
                return addrs.to_vec();
            }
            let k = match select {
                DnsSelect::RoundRobin => counter.fetch_add(1, Ordering::Relaxed) % addrs.len(),
                DnsSelect::Random => pseudo_random(counter.fetch_add(1, Ordering::Relaxed))
                    % addrs.len(),
                DnsSelect::First => unreachable!(),
            };
            let mut rotated = addrs.to_vec();
            rotated.rotate_left(k);
            rotated
        }
    }
}

/// Tiny xorshift64* PRNG — deterministic, dependency-free. Used only to pick
/// a rotation offset for `dns.select: random`; not for security.
fn pseudo_random(seed: usize) -> usize {
    let mut x = (seed as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    x as usize
}

fn cache_get(inner: &DnsShared, host: &str) -> Option<Vec<SocketAddr>> {
    match inner.cache {
        DnsCacheMode::Off => None,
        _ => {
            let store = inner.cache_store.lock().ok()?;
            let entry = store.get(host)?;
            let fresh = match inner.cache {
                DnsCacheMode::Forever => true,
                // The entry stores its precomputed expiry, so the configured
                // TTL duration itself is not needed here.
                DnsCacheMode::Ttl(_t) => entry
                    .expires_at
                    .is_some_and(|e| Instant::now() < e),
                DnsCacheMode::Off => false,
            };
            if fresh {
                Some(entry.addrs.clone())
            } else {
                None
            }
        }
    }
}

fn cache_put(inner: &DnsShared, host: &str, addrs: &[SocketAddr]) {
    let expires_at = match inner.cache {
        DnsCacheMode::Off => return,
        DnsCacheMode::Forever => None,
        DnsCacheMode::Ttl(t) => {
            if t.is_zero() {
                return;
            }
            Some(Instant::now() + t)
        }
    };
    if let Ok(mut store) = inner.cache_store.lock() {
        store.insert(
            host.to_string(),
            CacheEntry {
                addrs: addrs.to_vec(),
                expires_at,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cidr_parse_and_contains() {
        let net = IpCidr::parse("10.0.0.0/8").unwrap();
        assert!(net.contains("10.1.2.3".parse().unwrap()));
        assert!(!net.contains("11.0.0.1".parse().unwrap()));

        let single = IpCidr::parse("192.168.1.5").unwrap();
        assert!(single.contains("192.168.1.5".parse().unwrap()));
        assert!(!single.contains("192.168.1.6".parse().unwrap()));

        let v6 = IpCidr::parse("::1").unwrap();
        assert!(v6.contains("::1".parse().unwrap()));
        assert!(!v6.contains("::2".parse().unwrap()));

        let net6 = IpCidr::parse("fd00::/8").unwrap();
        assert!(net6.contains("fd12::1".parse().unwrap()));
        assert!(!net6.contains("fe80::1".parse().unwrap()));

        assert!(IpCidr::parse("not-an-ip").is_none());
        assert!(IpCidr::parse("10.0.0.0/99").is_none());
    }

    #[test]
    fn cache_mode_parsing() {
        assert_eq!(parse_cache_mode(None), DnsCacheMode::Off);
        assert_eq!(parse_cache_mode(Some("0")), DnsCacheMode::Off);
        assert_eq!(parse_cache_mode(Some("inf")), DnsCacheMode::Forever);
        assert_eq!(
            parse_cache_mode(Some("5m")),
            DnsCacheMode::Ttl(Duration::from_secs(300))
        );
        assert_eq!(parse_cache_mode(Some("garbage")), DnsCacheMode::Off);
    }

    #[test]
    fn hosts_parsing_and_lookup() {
        let mut map = HashMap::new();
        map.insert("api.example.com".to_string(), "10.0.0.1, 10.0.0.2".to_string());
        map.insert("*.wild.com".to_string(), "10.9.9.9".to_string());
        map.insert("bad.host".to_string(), "not-an-ip".to_string());

        let hosts = parse_hosts(&map);
        assert_eq!(hosts.len(), 2);
        assert_eq!(hosts.get("api.example.com").unwrap().len(), 2);

        assert_eq!(hosts_lookup(&hosts, "api.example.com").unwrap().len(), 2);
        assert_eq!(hosts_lookup(&hosts, "API.EXAMPLE.COM").unwrap().len(), 2);
        assert_eq!(hosts_lookup(&hosts, "sub.wild.com").unwrap().len(), 1);
        assert!(hosts_lookup(&hosts, "wild.com").is_none()); // wildcard ≠ bare domain
        assert!(hosts_lookup(&hosts, "other.com").is_none());
    }

    #[test]
    fn policy_filtering() {
        let v4: SocketAddr = "1.2.3.4:80".parse().unwrap();
        let v6: SocketAddr = "[::1]:80".parse().unwrap();
        let mut addrs = vec![v6, v4];

        apply_policy(&mut addrs, DnsPolicy::OnlyV4);
        assert_eq!(addrs, vec![v4]);

        apply_policy(&mut addrs, DnsPolicy::OnlyV6);
        assert_eq!(addrs, vec![]);

        let mut addrs = vec![v6, v4];
        apply_policy(&mut addrs, DnsPolicy::PreferV4);
        assert_eq!(addrs, vec![v4, v6]);
    }

    #[test]
    fn select_rotation() {
        let addrs = vec![
            "1.1.1.1:80".parse::<SocketAddr>().unwrap(),
            "2.2.2.2:80".parse().unwrap(),
            "3.3.3.3:80".parse().unwrap(),
        ];
        let counter = AtomicUsize::new(0);
        assert_eq!(select_addrs(&addrs, DnsSelect::First, &counter)[0], addrs[0]);
        assert_eq!(
            select_addrs(&addrs, DnsSelect::RoundRobin, &counter)[0],
            addrs[0]
        );
        assert_eq!(
            select_addrs(&addrs, DnsSelect::RoundRobin, &counter)[0],
            addrs[1]
        );
        assert_eq!(
            select_addrs(&addrs, DnsSelect::RoundRobin, &counter)[0],
            addrs[2]
        );
        // wraps
        assert_eq!(
            select_addrs(&addrs, DnsSelect::RoundRobin, &counter)[0],
            addrs[0]
        );
        // rotation preserves membership (no dupes / losses)
        let r = select_addrs(&addrs, DnsSelect::Random, &counter);
        let mut sorted = r.clone();
        sorted.sort();
        let mut orig = addrs.clone();
        orig.sort();
        assert_eq!(sorted, orig);
    }

    #[test]
    fn cache_store_roundtrip() {
        let shared = DnsShared {
            cache: DnsCacheMode::Ttl(Duration::from_secs(60)),
            select: DnsSelect::First,
            policy: DnsPolicy::Any,
            hosts: HashMap::new(),
            blacklist: vec![],
            cache_store: Mutex::new(HashMap::new()),
            round_robin: AtomicUsize::new(0),
        };
        let addrs = vec!["1.2.3.4:80".parse().unwrap()];
        assert!(cache_get(&shared, "x.com").is_none());
        cache_put(&shared, "x.com", &addrs);
        assert_eq!(cache_get(&shared, "x.com").unwrap(), addrs);

        // Expired entries are not returned.
        let mut expired = DnsShared {
            cache_store: Mutex::new(HashMap::new()),
            ..shared
        };
        expired.cache = DnsCacheMode::Ttl(Duration::ZERO);
        cache_put(&expired, "y.com", &addrs);
        assert!(cache_get(&expired, "y.com").is_none());
    }

    #[test]
    fn from_config_maps_options() {
        let mut cfg = HttpConfig {
            dns_ttl: Some("inf".to_string()),
            dns_select: Some("roundRobin".to_string()),
            dns_policy: Some("onlyIPv4".to_string()),
            ..Default::default()
        };
        cfg.hosts.insert("local.test".to_string(), "127.0.0.1".to_string());
        cfg.blacklist_ips.push("10.0.0.0/8".to_string());

        let r = DnsResolver::from_config(&cfg);
        assert_eq!(r.inner.cache, DnsCacheMode::Forever);
        assert_eq!(r.inner.select, DnsSelect::RoundRobin);
        assert_eq!(r.inner.policy, DnsPolicy::OnlyV4);
        assert_eq!(r.inner.hosts.get("local.test").unwrap().len(), 1);
        assert_eq!(r.inner.blacklist.len(), 1);
    }

    #[test]
    fn bad_blacklist_is_skipped() {
        let cfg = HttpConfig {
            blacklist_ips: vec!["10.0.0.0/8".to_string(), "junk".to_string()],
            ..Default::default()
        };
        let r = DnsResolver::from_config(&cfg);
        assert_eq!(r.inner.blacklist.len(), 1);
    }
}
