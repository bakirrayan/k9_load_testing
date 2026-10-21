//! Name resolution and the network options around it: k6's `hosts`, `dns`,
//! `blockHostnames`, `blacklistIPs` and `localIPs`.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;

use hickory_resolver::TokioResolver;
use hickory_resolver::config::LookupIpStrategy;
use tokio::net::{TcpSocket, TcpStream};

use crate::{ClientConfig, HttpError};

/// Which of a name's addresses to use (k6's `dns.select`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DnsSelect {
    First,
    #[default]
    Random,
    RoundRobin,
}

impl FromStr for DnsSelect {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "first" => Ok(DnsSelect::First),
            "random" => Ok(DnsSelect::Random),
            "roundRobin" => Ok(DnsSelect::RoundRobin),
            _ => Err(format!(
                "unknown DNS select `{s}` (expected first, random or roundRobin)"
            )),
        }
    }
}

/// Which IP versions to use (k6's `dns.policy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DnsPolicy {
    #[default]
    PreferIpv4,
    PreferIpv6,
    OnlyIpv4,
    OnlyIpv6,
    Any,
}

impl FromStr for DnsPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "preferIPv4" => Ok(DnsPolicy::PreferIpv4),
            "preferIPv6" => Ok(DnsPolicy::PreferIpv6),
            "onlyIPv4" => Ok(DnsPolicy::OnlyIpv4),
            "onlyIPv6" => Ok(DnsPolicy::OnlyIpv6),
            "any" => Ok(DnsPolicy::Any),
            _ => Err(format!(
                "unknown DNS policy `{s}` (expected preferIPv4, preferIPv6, onlyIPv4, onlyIPv6 \
                 or any)"
            )),
        }
    }
}

/// k6's `dns` option.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DnsConfig {
    /// How long a lookup is cached at most; `None`: forever, zero: not at all.
    pub ttl: Option<Duration>,
    pub select: DnsSelect,
    pub policy: DnsPolicy,
}

impl Default for DnsConfig {
    fn default() -> Self {
        DnsConfig {
            ttl: Some(Duration::from_secs(5 * 60)),
            select: DnsSelect::default(),
            policy: DnsPolicy::default(),
        }
    }
}

/// A hostname, or `*.domain` for every name under it (`blockHostnames`,
/// `hosts`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPattern {
    /// The name, or `.domain` for a wildcard.
    name: String,
    wildcard: bool,
}

impl HostPattern {
    pub fn matches(&self, host: &str) -> bool {
        if self.wildcard {
            host.len() > self.name.len() && host.ends_with(&self.name)
        } else {
            host == self.name
        }
    }
}

impl FromStr for HostPattern {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let s = s.trim().to_ascii_lowercase();
        let (name, wildcard) = match s.strip_prefix('*') {
            Some(rest) => (rest.to_owned(), true),
            None => (s.clone(), false),
        };
        let bare = name.strip_prefix('.').unwrap_or(&name);
        if bare.is_empty() || name.contains('*') || (wildcard && !name.starts_with('.')) {
            return Err(format!(
                "`{s}` must be a hostname, or `*.` and a hostname for the names under it"
            ));
        }
        Ok(HostPattern { name, wildcard })
    }
}

impl fmt::Display for HostPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.wildcard {
            f.write_str("*")?;
        }
        f.write_str(&self.name)
    }
}

/// One `hosts` entry: a name (maybe with a port, maybe `*.domain`) sent to
/// an IP address, or to an IP address and port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostOverride {
    host: HostPattern,
    port: Option<u16>,
    ip: IpAddr,
    to_port: Option<u16>,
}

impl HostOverride {
    pub fn new(from: &str, to: &str) -> Result<HostOverride, String> {
        let (host, port) = match from.rsplit_once(':') {
            Some((host, port)) => {
                let port = port
                    .parse()
                    .map_err(|_| format!("`{from}`: the port must be a number"))?;
                (host, Some(port))
            }
            None => (from, None),
        };
        let host = host.parse().map_err(|e| format!("`{from}`: {e}"))?;
        let to = to.trim();
        let (ip, to_port) = match to.parse::<SocketAddr>() {
            Ok(addr) => (addr.ip(), Some(addr.port())),
            Err(_) => (
                to.parse()
                    .map_err(|_| format!("`{to}` must be an IP address, or IP:port"))?,
                None,
            ),
        };
        Ok(HostOverride {
            host,
            port,
            ip,
            to_port,
        })
    }

    /// How well it matches `host:port`, if at all: a port beats none, a name
    /// beats a wildcard, a longer wildcard beats a shorter one.
    fn rank(&self, host: &str, port: u16) -> Option<(bool, bool, usize)> {
        if self.port.is_some_and(|p| p != port) || !self.host.matches(host) {
            return None;
        }
        Some((
            self.port.is_some(),
            !self.host.wildcard,
            self.host.name.len(),
        ))
    }
}

/// An IP address, a CIDR block (`10.0.0.0/24`) or a span
/// (`10.0.0.1-10.0.0.9`): for `blacklistIPs` and `localIPs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpRange {
    first: IpAddr,
    last: IpAddr,
    text: String,
}

impl IpRange {
    pub fn contains(&self, ip: IpAddr) -> bool {
        ip.is_ipv4() == self.first.is_ipv4()
            && bits(ip) >= bits(self.first)
            && bits(ip) <= bits(self.last)
    }

    /// Addresses in the range.
    pub fn len(&self) -> u128 {
        (bits(self.last) - bits(self.first)).saturating_add(1)
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    /// The `i`-th address (`i` below [`IpRange::len`]).
    fn nth(&self, i: u128) -> IpAddr {
        from_bits(bits(self.first) + i, self.first.is_ipv4())
    }
}

impl FromStr for IpRange {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let text = s.trim().to_owned();
        let bad = |why: &str| format!("`{text}`: {why}");
        let ip = |s: &str| {
            s.trim()
                .parse::<IpAddr>()
                .map_err(|_| bad("expected an IP address, a CIDR block or first-last"))
        };
        let (first, last) = if let Some((addr, prefix)) = text.split_once('/') {
            let addr = ip(addr)?;
            let width = if addr.is_ipv4() { 32 } else { 128 };
            let prefix: u32 = prefix
                .parse()
                .ok()
                .filter(|p| *p <= width)
                .ok_or_else(|| bad(&format!("the prefix must be 0 to {width}")))?;
            let host_bits = width - prefix;
            let mask = if host_bits == 128 {
                u128::MAX
            } else {
                (1u128 << host_bits) - 1
            };
            let v4 = addr.is_ipv4();
            (
                from_bits(bits(addr) & !mask, v4),
                from_bits(bits(addr) | mask, v4),
            )
        } else if let Some((first, last)) = text.split_once('-') {
            let (first, last) = (ip(first)?, ip(last)?);
            if first.is_ipv4() != last.is_ipv4() {
                return Err(bad("both ends must be the same IP version"));
            }
            if bits(first) > bits(last) {
                return Err(bad("the first address is after the last"));
            }
            (first, last)
        } else {
            let one = ip(&text)?;
            (one, one)
        };
        Ok(IpRange { first, last, text })
    }
}

impl fmt::Display for IpRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

fn bits(ip: IpAddr) -> u128 {
    match ip {
        IpAddr::V4(ip) => u32::from(ip).into(),
        IpAddr::V6(ip) => ip.into(),
    }
}

fn from_bits(bits: u128, v4: bool) -> IpAddr {
    if v4 {
        IpAddr::V4(Ipv4Addr::from(bits as u32))
    } else {
        IpAddr::V6(Ipv6Addr::from(bits))
    }
}

/// k6's `localIPs`: addresses to send from, handed out to VUs in turn.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IpPool {
    ranges: Vec<IpRange>,
}

impl IpPool {
    pub fn new(ranges: Vec<IpRange>) -> IpPool {
        IpPool { ranges }
    }

    pub fn len(&self) -> u128 {
        self.ranges.iter().map(IpRange::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// The `i`-th address, wrapping around at the end.
    ///
    /// # Panics
    /// If the pool is empty.
    pub fn nth(&self, i: u64) -> IpAddr {
        let mut i = u128::from(i) % self.len();
        for range in &self.ranges {
            if i < range.len() {
                return range.nth(i);
            }
            i -= range.len();
        }
        unreachable!("i is below the pool's length")
    }
}

/// Resolves hosts to the addresses to try, applying the network options.
pub(crate) struct Resolver {
    dns: TokioResolver,
    hosts: Vec<HostOverride>,
    blocked_hosts: Vec<HostPattern>,
    blocked_ips: Vec<IpRange>,
    select: DnsSelect,
    policy: DnsPolicy,
    /// `roundRobin`: the next address to use, by host.
    turns: Mutex<HashMap<String, usize>>,
}

impl Resolver {
    pub fn new(config: &ClientConfig) -> Result<Resolver, HttpError> {
        let mut builder = TokioResolver::builder_tokio()
            .map_err(|e| HttpError::Config(format!("system DNS configuration: {e}")))?;
        let options = builder.options_mut();
        let dns = config.dns;
        options.ip_strategy = match dns.policy {
            DnsPolicy::OnlyIpv4 => LookupIpStrategy::Ipv4Only,
            DnsPolicy::OnlyIpv6 => LookupIpStrategy::Ipv6Only,
            _ => LookupIpStrategy::Ipv4AndIpv6,
        };
        match dns.ttl {
            Some(ttl) => options.positive_max_ttl = Some(ttl),
            None => {
                let forever = Duration::from_secs(hickory_resolver::MAX_TTL.into());
                options.positive_min_ttl = Some(forever);
                options.positive_max_ttl = Some(forever);
            }
        }
        let dns_resolver = builder
            .build()
            .map_err(|e| HttpError::Config(format!("DNS resolver: {e}")))?;
        Ok(Resolver {
            dns: dns_resolver,
            hosts: config.hosts.clone(),
            blocked_hosts: config.block_hostnames.clone(),
            blocked_ips: config.blocked_ips.clone(),
            select: dns.select,
            policy: dns.policy,
            turns: Mutex::default(),
        })
    }

    /// The addresses to try for `host:port`, best first (never empty). With
    /// `local`, only those of its IP version.
    pub async fn resolve(
        &self,
        host: &str,
        port: u16,
        local: Option<IpAddr>,
    ) -> Result<Vec<SocketAddr>, HttpError> {
        let blocked = |reason: String| HttpError::Blocked {
            host: host.to_owned(),
            reason,
        };
        let dns_err = |reason: String| HttpError::Dns {
            host: host.to_owned(),
            reason,
        };
        let literal = host.parse::<IpAddr>().ok();
        if literal.is_none()
            && let Some(pattern) = self.blocked_hosts.iter().find(|p| p.matches(host))
        {
            return Err(blocked(format!(
                "the hostname matches blockHostnames entry `{pattern}`"
            )));
        }
        let overridden = self
            .hosts
            .iter()
            .filter_map(|o| Some((o.rank(host, port)?, o)))
            .max_by_key(|(rank, _)| *rank)
            .map(|(_, o)| SocketAddr::new(o.ip, o.to_port.unwrap_or(port)));
        let mut addrs = match (overridden, literal) {
            (Some(addr), _) => vec![addr],
            (None, Some(ip)) => vec![SocketAddr::new(ip, port)],
            (None, None) => {
                let lookup = self
                    .dns
                    .lookup_ip(host)
                    .await
                    .map_err(|e| dns_err(e.to_string()))?;
                let ips = arrange(lookup.iter().collect(), self.policy, |n| self.pick(host, n));
                ips.into_iter()
                    .map(|ip| SocketAddr::new(ip, port))
                    .collect()
            }
        };
        if addrs.is_empty() {
            return Err(dns_err("no addresses found".to_owned()));
        }
        if let Some(local) = local {
            addrs.retain(|a| a.is_ipv4() == local.is_ipv4());
            if addrs.is_empty() {
                return Err(dns_err(format!(
                    "no address of the same IP version as the local IP {local}"
                )));
            }
        }
        let mut first_blocked = None;
        addrs.retain(
            |a| match self.blocked_ips.iter().find(|r| r.contains(a.ip())) {
                Some(range) => {
                    first_blocked.get_or_insert((a.ip(), range));
                    false
                }
                None => true,
            },
        );
        match first_blocked {
            Some((ip, range)) if addrs.is_empty() => {
                Err(blocked(format!("IP {ip} is in blacklistIPs range {range}")))
            }
            _ => Ok(addrs),
        }
    }

    /// Which of `n` addresses to try first.
    fn pick(&self, host: &str, n: usize) -> usize {
        match self.select {
            DnsSelect::First => 0,
            DnsSelect::Random => fastrand::usize(..n),
            DnsSelect::RoundRobin => {
                let mut turns = self.turns.lock().unwrap_or_else(|e| e.into_inner());
                let turn = turns.entry(host.to_owned()).or_default();
                let i = *turn % n;
                *turn = turn.wrapping_add(1);
                i
            }
        }
    }
}

/// Keeps the addresses `policy` allows, the one `pick` chooses first (the
/// others follow, in case it doesn't answer).
fn arrange(ips: Vec<IpAddr>, policy: DnsPolicy, pick: impl FnOnce(usize) -> usize) -> Vec<IpAddr> {
    let (v4, v6): (Vec<IpAddr>, Vec<IpAddr>) = ips.iter().partition(|ip| ip.is_ipv4());
    let mut ips = match policy {
        DnsPolicy::PreferIpv4 if !v4.is_empty() => v4,
        DnsPolicy::PreferIpv6 if !v6.is_empty() => v6,
        DnsPolicy::PreferIpv4 | DnsPolicy::OnlyIpv6 => v6,
        DnsPolicy::PreferIpv6 | DnsPolicy::OnlyIpv4 => v4,
        DnsPolicy::Any => ips,
    };
    if !ips.is_empty() {
        let first = pick(ips.len());
        ips.rotate_left(first);
    }
    ips
}

/// Connects to `addr`, from `local` if given.
pub(crate) async fn tcp_connect(addr: SocketAddr, local: Option<IpAddr>) -> io::Result<TcpStream> {
    let Some(local) = local else {
        return TcpStream::connect(addr).await;
    };
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.bind(SocketAddr::new(local, 0))?;
    socket.connect(addr).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ips(list: &[&str]) -> Vec<IpAddr> {
        list.iter().map(|s| s.parse().unwrap()).collect()
    }

    #[test]
    fn policies_pick_ip_versions() {
        let both = ips(&["::1", "10.0.0.1", "::2", "10.0.0.2"]);
        let only4 = ips(&["10.0.0.1"]);
        let first = |_| 0;
        for (policy, list, expected) in [
            (DnsPolicy::PreferIpv4, &both, vec!["10.0.0.1", "10.0.0.2"]),
            (DnsPolicy::PreferIpv6, &both, vec!["::1", "::2"]),
            (DnsPolicy::PreferIpv6, &only4, vec!["10.0.0.1"]),
            (DnsPolicy::OnlyIpv6, &only4, vec![]),
            (DnsPolicy::OnlyIpv4, &both, vec!["10.0.0.1", "10.0.0.2"]),
            (
                DnsPolicy::Any,
                &both,
                vec!["::1", "10.0.0.1", "::2", "10.0.0.2"],
            ),
        ] {
            assert_eq!(
                arrange(list.clone(), policy, first),
                ips(&expected),
                "{policy:?}"
            );
        }
        assert_eq!(
            arrange(both, DnsPolicy::PreferIpv4, |n| n - 1),
            ips(&["10.0.0.2", "10.0.0.1"]),
            "the picked address first, the rest as fallbacks"
        );
    }

    #[test]
    fn round_robin_takes_turns_per_host() {
        let resolver = Resolver::new(&ClientConfig {
            dns: DnsConfig {
                select: DnsSelect::RoundRobin,
                ..DnsConfig::default()
            },
            ..ClientConfig::default()
        })
        .unwrap();
        let turns: Vec<usize> = (0..4).map(|_| resolver.pick("a", 3)).collect();
        assert_eq!(turns, [0, 1, 2, 0]);
        assert_eq!(resolver.pick("b", 3), 0);
        let first = Resolver::new(&ClientConfig {
            dns: DnsConfig {
                select: DnsSelect::First,
                ..DnsConfig::default()
            },
            ..ClientConfig::default()
        })
        .unwrap();
        assert_eq!(first.pick("a", 3) + first.pick("a", 3), 0);
    }

    #[test]
    fn option_names_are_k6s() {
        assert_eq!("roundRobin".parse(), Ok(DnsSelect::RoundRobin));
        assert_eq!("onlyIPv6".parse(), Ok(DnsPolicy::OnlyIpv6));
        assert!("round-robin".parse::<DnsSelect>().is_err());
        assert!("ipv4".parse::<DnsPolicy>().is_err());
    }
}
