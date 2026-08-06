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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tropel_core::config::HttpConfig;

/// k6's default DNS cache TTL when `options.dns.ttl` is unset (k6: `"5m"`).
const K6_DEFAULT_TTL: Duration = Duration::from_secs(300);
/// Upper bound on cached host entries — the cache has no other eviction, so
/// a run that resolves many unique hostnames (randomized URLs, etc.) must not
/// grow without bound. Entries are evicted expired-first, then oldest.
const MAX_CACHE_ENTRIES: usize = 4096;

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
    ///
    /// Two correctness rules that the naive mask-shift got wrong:
    /// 1. `/0` (prefix 0) must match EVERY address. `u32::MAX << 32` is a
    ///    shift overflow (debug panic) / masked to `<< 0` in release, which
    ///    produced an all-ones mask → exact-host match → `blacklistIPs:
    ///    ["0.0.0.0/0"]` blocked only `0.0.0.0` itself. `checked_shl(...)
    ///    .unwrap_or(0)` yields mask 0 → `(base & 0) == (ip & 0)` → true.
    /// 2. IPv4-mapped IPv6 (`::ffff:10.0.0.1`) must match a v4 CIDR. Both
    ///    sides are canonicalized with [`IpAddr::to_canonical`] before the
    ///    family match, so a v4-mapped v6 host in a static `hosts` entry is
    ///    caught by `10.0.0.0/8` instead of slipping through as a v6 addr.
    pub fn contains(&self, ip: IpAddr) -> bool {
        let base = self.base.to_canonical();
        let ip = ip.to_canonical();
        match (base, ip) {
            (IpAddr::V4(base), IpAddr::V4(ip)) => {
                // A v4-mapped v6 CIDR (`::ffff:10.0.0.0/104`) canonicalizes to
                // a v4 base, but its prefix still counts v6 bits: subtract the
                // 96-bit mapped prefix so `/104` means `10.0.0.0/8`, not a
                // full 32-bit mask (which would silently shrink it to an
                // exact-host match).
                let prefix = if matches!(self.base, IpAddr::V6(_)) {
                    self.prefix.saturating_sub(96).min(32)
                } else {
                    self.prefix
                };
                let mask = if prefix >= 32 {
                    u32::MAX
                } else {
                    u32::MAX.checked_shl(32 - prefix as u32).unwrap_or(0)
                };
                (u32::from(base) & mask) == (u32::from(ip) & mask)
            }
            (IpAddr::V6(base), IpAddr::V6(ip)) => {
                let mask = if self.prefix >= 128 {
                    u128::MAX
                } else {
                    u128::MAX
                        .checked_shl(128 - self.prefix as u32)
                        .unwrap_or(0)
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
    hosts: HashMap<String, Vec<SocketAddr>>,
    blacklist: Vec<IpCidr>,
    cache_store: Mutex<HashMap<String, CacheEntry>>,
    /// Per-host rotation counters for `dns.select: roundRobin`. k6 rotates
    /// EACH HOST independently; a single global counter (the old design)
    /// couples unrelated hosts — every lookup advances one shared cursor, so
    /// a host's rotation offset depends on how many other hosts resolved in
    /// between, which can pin a host to a single IP for its whole TTL.
    rotation: Mutex<HashMap<String, usize>>,
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
        // Backlog line 151: k6's DNS DEFAULTS are ttl=5m, select=random,
        // policy=preferIPv4. An unconfigured script must behave like k6 — a
        // fresh getaddrinfo per request (the old ttl off / first / any) was a
        // silent divergence that also hammered DNS under load.
        let cache = match config.dns_ttl.as_deref() {
            Some(ttl) => parse_cache_mode(Some(ttl)),
            None => DnsCacheMode::Ttl(K6_DEFAULT_TTL),
        };
        let select = match config.dns_select.as_deref().map(str::to_ascii_lowercase).as_deref() {
            Some("roundrobin" | "round_robin" | "round-robin") => DnsSelect::RoundRobin,
            Some("random") => DnsSelect::Random,
            None => DnsSelect::Random, // k6 default
            other => {
                if let Some(v) = other {
                    tracing::warn!("unknown dns.select '{v}' — using 'random' (k6 default)");
                }
                DnsSelect::Random
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
            None => DnsPolicy::PreferV4, // k6 default
            other => {
                if let Some(v) = other {
                    tracing::warn!("unknown dns.policy '{v}' — using 'preferIPv4' (k6 default)");
                }
                DnsPolicy::PreferV4
            }
        };
        let hosts = parse_hosts(&config.hosts);
        let blacklist = parse_blacklist(&config.blacklist_ips);

        DnsResolver {
            inner: Arc::new(DnsShared {
                cache,
                select,
                policy,
                hosts,
                blacklist,
                cache_store: Mutex::new(HashMap::new()),
                rotation: Mutex::new(HashMap::new()),
            }),
        }
    }
}

/// Parse a `blacklistIPs` list into CIDRs, warning and skipping entries that
/// fail to parse. Shared by [`DnsResolver::from_config`] and the per-hop
/// IP-literal check in [`crate::client::HttpClient`] — both need the exact
/// same interpretation of the config option.
pub fn parse_blacklist(blacklist_ips: &[String]) -> Vec<IpCidr> {
    blacklist_ips
        .iter()
        .filter_map(|s| {
            IpCidr::parse(s).or_else(|| {
                tracing::warn!("invalid blacklistIPs entry '{s}' — ignored");
                None
            })
        })
        .collect()
}

impl Resolve for DnsResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            // 1. Static hosts map (exact or wildcard) — no DNS involved. The
            //    blacklist still applies: an explicit host override must not
            //    smuggle connections to a blocked network.
            if let Some(addrs) = hosts_lookup(&inner.hosts, &host) {
                record_dns(Duration::ZERO);
                let mut addrs: Vec<SocketAddr> = addrs;
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
                let rotated = select_addrs(&host, &addrs, inner.select, &inner.rotation);
                return Ok(box_addrs(rotated));
            }

            // 2. TTL cache hit? The cached list is re-selected on every hit so
            //    `dns.select` (roundRobin/random) keeps rotating across VUs
            //    and cache hits instead of every VU hammering the same first
            //    IP for the whole TTL window.
            if let Some(entry) = cache_get(&inner, &host) {
                record_dns(Duration::ZERO);
                let rotated = select_addrs(&host, &entry, inner.select, &inner.rotation);
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
            let chosen = select_addrs(&host, &addrs, inner.select, &inner.rotation);
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

fn parse_hosts(map: &HashMap<String, String>) -> HashMap<String, Vec<SocketAddr>> {
    let mut out = HashMap::new();
    for (host, value) in map {
        let addrs: Vec<SocketAddr> = value
            .split(',')
            .filter_map(|s| {
                let s = s.trim();
                // Backlog line 151: k6 accepts BOTH forms — a bare IP
                // ("1.2.3.4", port comes from the request URL) and the
                // "ip:port" override form ("1.2.3.4:8080") which pins the
                // destination port. The old parse_hosts only accepted IpAddr,
                // so the ip:port form was silently dropped.
                if let Ok(sa) = s.parse::<SocketAddr>() {
                    if sa.port() != 0 {
                        return Some(sa);
                    }
                    // "ip:0" — treat as a bare IP below.
                }
                let parsed = s.parse::<IpAddr>().ok();
                if parsed.is_none() && !s.is_empty() {
                    tracing::warn!("hosts entry '{host}' has invalid IP '{s}' — skipped");
                }
                parsed.map(|ip| SocketAddr::new(ip, 0))
            })
            .collect();
        if addrs.is_empty() {
            if !value.trim().is_empty() {
                tracing::warn!("hosts entry '{host}' has no valid IPs — ignored");
            }
        } else {
            out.insert(host.trim().to_ascii_lowercase(), addrs);
        }
    }
    out
}

/// Exact host lookup first, then `*.domain` wildcard keys.
///
/// Backlog line 151: wildcard matching was nondeterministic — it iterated the
/// HashMap and returned the FIRST matching wildcard, so when `*.example.com`
/// and `*.sub.example.com` both matched `x.sub.example.com`, which one won
/// depended on hash order. Now the LONGEST matching suffix wins (k6's rule),
/// which is deterministic and most-specific-first.
fn hosts_lookup(hosts: &HashMap<String, Vec<SocketAddr>>, host: &str) -> Option<Vec<SocketAddr>> {
    let h = host.to_ascii_lowercase();
    if let Some(v) = hosts.get(&h) {
        return Some(v.clone());
    }
    let mut best: Option<(usize, &Vec<SocketAddr>)> = None;
    for (key, v) in hosts {
        if let Some(suffix) = key.strip_prefix("*.") {
            let dot_suffix = format!(".{suffix}");
            if h.len() > dot_suffix.len() && h.ends_with(&dot_suffix) {
                let specific = suffix.len();
                if best.is_none() || specific > best.unwrap().0 {
                    best = Some((specific, v));
                }
            }
        }
    }
    best.map(|(_, v)| v.clone())
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

/// Select the address list to use, applying `dns.select`. Backlog line 151:
/// rotation counters are PER HOST (k6 semantics) — the old single global
/// `AtomicUsize` meant every host shared one cursor, so a host's rotation
/// offset depended on how many OTHER hosts had resolved, pinning hosts to a
/// single IP for their whole TTL. `host` keys the per-host counter map.
fn select_addrs(
    host: &str,
    addrs: &[SocketAddr],
    select: DnsSelect,
    rotation: &Mutex<HashMap<String, usize>>,
) -> Vec<SocketAddr> {
    match select {
        DnsSelect::First => addrs.to_vec(),
        DnsSelect::RoundRobin | DnsSelect::Random => {
            if addrs.len() <= 1 {
                return addrs.to_vec();
            }
            let mut map = rotation.lock().unwrap();
            let n = map.entry(host.to_string()).or_insert(0);
            let k = match select {
                DnsSelect::RoundRobin => *n % addrs.len(),
                DnsSelect::Random => pseudo_random(*n) % addrs.len(),
                DnsSelect::First => unreachable!(),
            };
            *n = n.wrapping_add(1);
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
        // Backlog line 151: the cache had NO eviction — expired entries were
        // skipped on read but never removed, so a run resolving many unique
        // hostnames (randomized URLs, per-iteration hosts) grew the map
        // without bound. Evict expired entries first, then the soonest-
        // expiring survivors, down to MAX_CACHE_ENTRIES.
        let now = Instant::now();
        store.retain(|_, e| match e.expires_at {
            Some(t) => t > now,
            None => true, // `inf` entries never expire
        });
        if store.len() >= MAX_CACHE_ENTRIES {
            // Evict soonest-expiring TTL'd entries FIRST; `inf` (forever)
            // entries sort LAST so they're only evicted when there is nothing
            // else — a forever entry must not be thrown away before a live
            // TTL'd one. `None` gets a ~100-year horizon instead of `now`.
            let forever = Instant::now() + Duration::from_secs(365 * 24 * 3600 * 100);
            let mut keys_by_expiry: Vec<(String, Instant)> = store
                .iter()
                .filter(|(k, _)| *k != host)
                .map(|(k, e)| (k.clone(), e.expires_at.unwrap_or(forever)))
                .collect();
            keys_by_expiry.sort_by_key(|(_, t)| *t);
            // Room for the new entry: keep the map at/below the cap AFTER the
            // insert below.
            let to_remove = store.len() + 1 - MAX_CACHE_ENTRIES;
            for (k, _) in keys_by_expiry.into_iter().take(to_remove) {
                store.remove(&k);
            }
        }
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
    fn cidr_zero_prefix_matches_everything() {
        // Regression: `u32::MAX << 32` overflowed (debug panic) / was masked
        // to `<< 0` in release, so `0.0.0.0/0` matched only 0.0.0.0 itself.
        let all = IpCidr::parse("0.0.0.0/0").unwrap();
        assert!(all.contains("1.2.3.4".parse().unwrap()));
        assert!(all.contains("255.255.255.255".parse().unwrap()));

        let all_v6 = IpCidr::parse("::/0").unwrap();
        assert!(all_v6.contains("2001:db8::1".parse().unwrap()));
    }

    #[test]
    fn cidr_v4_mapped_v6_matches_v4_net() {
        // Regression: `::ffff:10.0.0.1` is a v6 address, so it never matched
        // a v4 CIDR — a static-hosts entry with a mapped literal slipped
        // past `10.0.0.0/8`. Both sides are canonicalized now.
        let net = IpCidr::parse("10.0.0.0/8").unwrap();
        assert!(net.contains("::ffff:10.0.0.1".parse().unwrap()));
        assert!(!net.contains("::ffff:11.0.0.1".parse().unwrap()));

        // A v4-mapped v6 CIDR counts v6 bits: /104 = 96 mapped + 8 v4 bits,
        // i.e. the whole 10.0.0.0/8; /120 = 96 + 24 = 10.0.0.0/24.
        let v4_cidr_from_v6 = IpCidr::parse("::ffff:10.0.0.0/104").unwrap();
        assert!(v4_cidr_from_v6.contains("::ffff:10.5.0.1".parse().unwrap()));
        assert!(v4_cidr_from_v6.contains("10.5.0.1".parse().unwrap()));
        assert!(v4_cidr_from_v6.contains("10.255.255.255".parse().unwrap()));
        assert!(!v4_cidr_from_v6.contains("11.0.0.1".parse().unwrap()));

        let v4_24_from_v6 = IpCidr::parse("::ffff:10.0.0.0/120").unwrap();
        assert!(v4_24_from_v6.contains("::ffff:10.0.0.9".parse().unwrap()));
        assert!(v4_24_from_v6.contains("10.0.0.9".parse().unwrap()));
        assert!(!v4_24_from_v6.contains("10.0.1.9".parse().unwrap()));
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
        let rotation = Mutex::new(HashMap::new());
        assert_eq!(select_addrs("h.com", &addrs, DnsSelect::First, &rotation)[0], addrs[0]);
        assert_eq!(
            select_addrs("h.com", &addrs, DnsSelect::RoundRobin, &rotation)[0],
            addrs[0]
        );
        assert_eq!(
            select_addrs("h.com", &addrs, DnsSelect::RoundRobin, &rotation)[0],
            addrs[1]
        );
        assert_eq!(
            select_addrs("h.com", &addrs, DnsSelect::RoundRobin, &rotation)[0],
            addrs[2]
        );
        // wraps
        assert_eq!(
            select_addrs("h.com", &addrs, DnsSelect::RoundRobin, &rotation)[0],
            addrs[0]
        );
        // rotation preserves membership (no dupes / losses)
        let r = select_addrs("h.com", &addrs, DnsSelect::Random, &rotation);
        let mut sorted = r.clone();
        sorted.sort();
        let mut orig = addrs.clone();
        orig.sort();
        assert_eq!(sorted, orig);
    }

    #[test]
    fn select_rotation_is_per_host() {
        // Backlog line 151: the OLD single global counter coupled hosts — a
        // lookup of host B advanced the cursor shared with host A, so A's
        // rotation depended on how many OTHER hosts resolved (A could repeat
        // its first IP while the shared cursor advanced past its second).
        // Rotation must be independent per host. Both hosts have TWO
        // addresses so the old global counter would have skipped A's second
        // entry after B's lookup advanced the shared cursor.
        let a = vec!["1.1.1.1:80".parse::<SocketAddr>().unwrap(), "2.2.2.2:80".parse().unwrap()];
        let b = vec!["9.9.9.9:80".parse::<SocketAddr>().unwrap(), "8.8.8.8:80".parse().unwrap()];
        let rotation = Mutex::new(HashMap::new());
        assert_eq!(select_addrs("a.com", &a, DnsSelect::RoundRobin, &rotation)[0], a[0]);
        assert_eq!(select_addrs("b.com", &b, DnsSelect::RoundRobin, &rotation)[0], b[0]);
        // B's lookup advanced B's counter only: A still advances to its
        // SECOND address (under the old shared cursor, B's lookup advanced
        // the shared counter past A's second entry, so A repeated a[0]).
        assert_eq!(select_addrs("a.com", &a, DnsSelect::RoundRobin, &rotation)[0], a[1]);
    }

    #[test]
    fn cache_eviction_prefers_forever_entries() {
        // Backlog line 151: over-cap eviction must NOT evict `inf` entries
        // before live TTL'd ones (the first implementation sorted `None` as
        // `now`, so forever entries were evicted first).
        let shared = DnsShared {
            cache: DnsCacheMode::Ttl(Duration::from_secs(60)),
            select: DnsSelect::First,
            policy: DnsPolicy::Any,
            hosts: HashMap::new(),
            blacklist: vec![],
            cache_store: Mutex::new(HashMap::new()),
            rotation: Mutex::new(HashMap::new()),
        };
        let addrs = vec!["1.2.3.4:80".parse().unwrap()];
        {
            let mut store = shared.cache_store.lock().unwrap();
            // Fill to the cap with LIVE TTL'd entries, plus one forever entry
            // that must survive eviction.
            for i in 0..MAX_CACHE_ENTRIES {
                store.insert(
                    format!("live{i}.com"),
                    CacheEntry {
                        addrs: addrs.clone(),
                        expires_at: Some(Instant::now() + Duration::from_secs(60)),
                    },
                );
            }
            store.insert(
                "forever.com".to_string(),
                CacheEntry {
                    addrs: addrs.clone(),
                    expires_at: None,
                },
            );
        }
        cache_put(&shared, "new.com", &addrs);
        let store = shared.cache_store.lock().unwrap();
        assert!(
            store.contains_key("forever.com"),
            "an `inf` entry must not be evicted before live TTL'd entries"
        );
        assert!(store.contains_key("new.com"));
        assert!(store.len() <= MAX_CACHE_ENTRIES);
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
            rotation: Mutex::new(HashMap::new()),
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
    fn cache_evicts_expired_and_bounded() {
        // Backlog line 151: the cache had no eviction. Now expired entries are
        // purged on put and the map is bounded by MAX_CACHE_ENTRIES.
        let shared = DnsShared {
            cache: DnsCacheMode::Ttl(Duration::from_secs(60)),
            select: DnsSelect::First,
            policy: DnsPolicy::Any,
            hosts: HashMap::new(),
            blacklist: vec![],
            cache_store: Mutex::new(HashMap::new()),
            rotation: Mutex::new(HashMap::new()),
        };
        let addrs = vec!["1.2.3.4:80".parse().unwrap()];

        // Seed an expired entry, then a fresh put must purge it.
        {
            let mut store = shared.cache_store.lock().unwrap();
            store.insert(
                "dead.com".to_string(),
                CacheEntry {
                    addrs: addrs.clone(),
                    expires_at: Some(Instant::now() - Duration::from_secs(1)),
                },
            );
        }
        cache_put(&shared, "live.com", &addrs);
        {
            let store = shared.cache_store.lock().unwrap();
            assert!(!store.contains_key("dead.com"), "expired entry must be purged");
            assert!(store.contains_key("live.com"));
        }

        // Bounding: fill past the cap; the map must never exceed it.
        let mut store = shared.cache_store.lock().unwrap();
        for i in 0..MAX_CACHE_ENTRIES + 50 {
            store.insert(
                format!("h{i}.com"),
                CacheEntry {
                    addrs: addrs.clone(),
                    expires_at: Some(Instant::now() + Duration::from_secs(60)),
                },
            );
        }
        drop(store);
        cache_put(&shared, "final.com", &addrs);
        let len = shared.cache_store.lock().unwrap().len();
        assert!(len <= MAX_CACHE_ENTRIES, "cache must be bounded, got {len}");
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
    fn unset_dns_matches_k6_defaults() {
        // Backlog line 151: an unconfigured `dns` block must behave like k6 —
        // ttl=5m, select=random, policy=preferIPv4 — instead of the old
        // ttl off / first / any (which did a fresh getaddrinfo per request).
        let r = DnsResolver::from_config(&HttpConfig::default());
        assert_eq!(r.inner.cache, DnsCacheMode::Ttl(K6_DEFAULT_TTL));
        assert_eq!(r.inner.select, DnsSelect::Random);
        assert_eq!(r.inner.policy, DnsPolicy::PreferV4);
    }

    #[test]
    fn hosts_ip_port_override_form() {
        // Backlog line 151: k6's "ip:port" hosts value form was silently
        // dropped (parse_hosts only accepted bare IpAddr). Now "1.2.3.4:8080"
        // pins the destination port; a bare IP keeps port 0 (request URL port).
        let mut map = HashMap::new();
        map.insert("pinned.test".to_string(), "10.0.0.1:8080".to_string());
        map.insert("plain.test".to_string(), "10.0.0.2".to_string());
        let hosts = parse_hosts(&map);
        let pinned = &hosts["pinned.test"];
        assert_eq!(pinned.len(), 1);
        assert_eq!(pinned[0], "10.0.0.1:8080".parse::<SocketAddr>().unwrap());
        let plain = &hosts["plain.test"];
        assert_eq!(plain[0].port(), 0, "bare IP keeps port 0 (request port applies)");
    }

    #[test]
    fn wildcard_longest_suffix_wins() {
        // Backlog line 151: wildcard matching was nondeterministic (HashMap
        // order). The longest matching suffix must win deterministically.
        let mut map = HashMap::new();
        map.insert("*.example.com".to_string(), "10.1.1.1".to_string());
        map.insert("*.sub.example.com".to_string(), "10.2.2.2".to_string());
        let hosts = parse_hosts(&map);
        let got = hosts_lookup(&hosts, "x.sub.example.com").unwrap();
        assert_eq!(got[0].ip(), "10.2.2.2".parse::<IpAddr>().unwrap());
        let got = hosts_lookup(&hosts, "x.example.com").unwrap();
        assert_eq!(got[0].ip(), "10.1.1.1".parse::<IpAddr>().unwrap());
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
