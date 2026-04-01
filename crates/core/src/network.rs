use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;

use serde::Deserialize;
use url::Url;

use crate::SandboxError;

const DEFAULT_PATH_PREFIX: &str = "/";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 65_536;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkPolicy {
    pub allowed_origins: Vec<NetworkOriginPolicy>,
    pub allowed_methods: BTreeSet<String>,
    pub allowed_schemes: BTreeSet<String>,
    pub request_timeout_ms: u64,
    pub max_response_bytes: usize,
    pub block_private_ranges: bool,
    pub dns_rebinding_protection: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkOriginPolicy {
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub path_prefix: String,
    pub injected_headers: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct RawNetworkPolicy {
    allowed_origins: Vec<RawNetworkOriginPolicy>,
    #[serde(default = "default_allowed_methods")]
    allowed_methods: BTreeSet<String>,
    #[serde(default = "default_allowed_schemes")]
    allowed_schemes: BTreeSet<String>,
    #[serde(default = "default_request_timeout_ms")]
    request_timeout_ms: u64,
    #[serde(default = "default_max_response_bytes")]
    max_response_bytes: usize,
    #[serde(default = "default_true")]
    block_private_ranges: bool,
    #[serde(default = "default_true")]
    dns_rebinding_protection: bool,
}

#[derive(Debug, Deserialize)]
struct RawNetworkOriginPolicy {
    origin: String,
    #[serde(default = "default_path_prefix")]
    path_prefix: String,
    #[serde(default)]
    injected_headers: BTreeMap<String, String>,
}

pub fn parse_network_policy_json(json: &str) -> Result<NetworkPolicy, SandboxError> {
    let raw = serde_json::from_str::<RawNetworkPolicy>(json).map_err(|error| {
        SandboxError::InvalidRequest(format!("network policy must be valid JSON: {error}"))
    })?;
    normalize_network_policy(raw)
}

pub fn normalize_http_method(value: &str) -> Result<String, SandboxError> {
    let method = value.trim().to_ascii_uppercase();
    if method.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "HTTP method must not be empty".to_string(),
        ));
    }
    if method
        .bytes()
        .all(|byte| matches!(byte, b'A'..=b'Z' | b'-'))
    {
        Ok(method)
    } else {
        Err(SandboxError::InvalidRequest(format!(
            "unsupported HTTP method token: {value}"
        )))
    }
}

impl NetworkPolicy {
    pub fn allows_method(&self, method: &str) -> Result<(), SandboxError> {
        let normalized = normalize_http_method(method)?;
        if self.allowed_methods.contains(&normalized) {
            Ok(())
        } else {
            Err(SandboxError::PolicyDenied(format!(
                "HTTP method is not allowed by network policy: {normalized}"
            )))
        }
    }

    pub fn match_url<'a>(&'a self, url: &Url) -> Result<&'a NetworkOriginPolicy, SandboxError> {
        if !url.username().is_empty() || url.password().is_some() {
            return Err(SandboxError::InvalidRequest(
                "URLs with embedded credentials are not allowed".to_string(),
            ));
        }

        let scheme = url.scheme().to_ascii_lowercase();
        if !self.allowed_schemes.contains(&scheme) {
            return Err(SandboxError::PolicyDenied(format!(
                "URL scheme is not allowed by network policy: {scheme}"
            )));
        }

        let host = url
            .host_str()
            .ok_or_else(|| SandboxError::InvalidRequest("URL must include a host".to_string()))?
            .to_ascii_lowercase();
        let port = effective_port(url)?;
        let path = normalized_url_path(url);

        self.allowed_origins
            .iter()
            .find(|origin| {
                origin.scheme == scheme
                    && origin.host == host
                    && origin.port == port
                    && path_is_within_prefix(&path, &origin.path_prefix)
            })
            .ok_or_else(|| {
                SandboxError::PolicyDenied(format!(
                    "URL is not allowed by network policy: {}",
                    url.as_str()
                ))
            })
    }

    pub fn ensure_remote_addrs(&self, addrs: &[IpAddr]) -> Result<(), SandboxError> {
        if !self.block_private_ranges {
            return Ok(());
        }
        if addrs.iter().copied().any(is_private_or_local_ip) {
            return Err(SandboxError::PolicyDenied(
                "network policy blocks private or loopback destinations".to_string(),
            ));
        }
        Ok(())
    }
}

fn normalize_network_policy(raw: RawNetworkPolicy) -> Result<NetworkPolicy, SandboxError> {
    if raw.allowed_origins.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "network policy must configure at least one allowed origin".to_string(),
        ));
    }
    if raw.request_timeout_ms == 0 {
        return Err(SandboxError::InvalidRequest(
            "network policy request_timeout_ms must be greater than zero".to_string(),
        ));
    }
    if raw.max_response_bytes == 0 {
        return Err(SandboxError::InvalidRequest(
            "network policy max_response_bytes must be greater than zero".to_string(),
        ));
    }

    let allowed_methods = raw
        .allowed_methods
        .iter()
        .map(|method| normalize_http_method(method))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if allowed_methods.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "network policy must configure at least one allowed HTTP method".to_string(),
        ));
    }

    let allowed_schemes = raw
        .allowed_schemes
        .iter()
        .map(|scheme| normalize_scheme(scheme))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if allowed_schemes.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "network policy must configure at least one allowed URL scheme".to_string(),
        ));
    }

    let allowed_origins = raw
        .allowed_origins
        .into_iter()
        .map(normalize_origin_policy)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(NetworkPolicy {
        allowed_origins,
        allowed_methods,
        allowed_schemes,
        request_timeout_ms: raw.request_timeout_ms,
        max_response_bytes: raw.max_response_bytes,
        block_private_ranges: raw.block_private_ranges,
        dns_rebinding_protection: raw.dns_rebinding_protection,
    })
}

fn normalize_origin_policy(
    raw: RawNetworkOriginPolicy,
) -> Result<NetworkOriginPolicy, SandboxError> {
    let parsed = Url::parse(&raw.origin).map_err(|error| {
        SandboxError::InvalidRequest(format!("invalid allowed origin: {error}"))
    })?;
    if parsed.cannot_be_a_base() {
        return Err(SandboxError::InvalidRequest(
            "allowed origin must be an absolute origin URL".to_string(),
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(SandboxError::InvalidRequest(
            "allowed origin must not contain credentials".to_string(),
        ));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(SandboxError::InvalidRequest(
            "allowed origin must not contain query or fragment components".to_string(),
        ));
    }
    let scheme = normalize_scheme(parsed.scheme())?;
    let host = parsed
        .host_str()
        .ok_or_else(|| {
            SandboxError::InvalidRequest("allowed origin must include a host".to_string())
        })?
        .to_ascii_lowercase();
    let port = effective_port(&parsed)?;
    let path = parsed.path();
    if path != "/" {
        return Err(SandboxError::InvalidRequest(
            "allowed origin must not include a path; use path_prefix instead".to_string(),
        ));
    }

    Ok(NetworkOriginPolicy {
        scheme,
        host,
        port,
        path_prefix: normalize_path_prefix(&raw.path_prefix)?,
        injected_headers: raw.injected_headers,
    })
}

fn normalize_scheme(value: &str) -> Result<String, SandboxError> {
    let scheme = value.trim().to_ascii_lowercase();
    if scheme.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "URL scheme must not be empty".to_string(),
        ));
    }
    if scheme.bytes().all(|byte| matches!(byte, b'a'..=b'z')) {
        Ok(scheme)
    } else {
        Err(SandboxError::InvalidRequest(format!(
            "unsupported URL scheme token: {value}"
        )))
    }
}

fn normalize_path_prefix(value: &str) -> Result<String, SandboxError> {
    let prefix = if value.trim().is_empty() {
        DEFAULT_PATH_PREFIX.to_string()
    } else {
        value.trim().to_string()
    };
    if !prefix.starts_with('/') {
        return Err(SandboxError::InvalidRequest(
            "network policy path_prefix must start with '/'".to_string(),
        ));
    }
    if prefix.contains("/../") || prefix.ends_with("/..") || prefix.contains('\0') {
        return Err(SandboxError::InvalidRequest(
            "network policy path_prefix must not contain traversal".to_string(),
        ));
    }
    Ok(prefix)
}

fn normalized_url_path(url: &Url) -> String {
    match url.path() {
        "" => DEFAULT_PATH_PREFIX.to_string(),
        path => path.to_string(),
    }
}

fn path_is_within_prefix(path: &str, prefix: &str) -> bool {
    if prefix == DEFAULT_PATH_PREFIX {
        return true;
    }
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn effective_port(url: &Url) -> Result<u16, SandboxError> {
    url.port_or_known_default().ok_or_else(|| {
        SandboxError::InvalidRequest(format!(
            "URL must include a known port for scheme '{}'",
            url.scheme()
        ))
    })
}

fn default_allowed_methods() -> BTreeSet<String> {
    BTreeSet::from(["GET".to_string()])
}

fn default_allowed_schemes() -> BTreeSet<String> {
    BTreeSet::from(["https".to_string()])
}

fn default_request_timeout_ms() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_MS
}

fn default_max_response_bytes() -> usize {
    DEFAULT_MAX_RESPONSE_BYTES
}

fn default_true() -> bool {
    true
}

fn default_path_prefix() -> String {
    DEFAULT_PATH_PREFIX.to_string()
}

fn is_private_or_local_ip(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(v4) => {
            v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_unspecified()
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_unique_local()
                || v6.is_unicast_link_local()
                || v6.segments()[0] == 0x2001 && v6.segments()[1] == 0x0db8
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    fn sample_policy() -> NetworkPolicy {
        parse_network_policy_json(
            r#"{
              "allowed_origins": [
                {
                  "origin": "https://api.example.test",
                  "path_prefix": "/v1",
                  "injected_headers": {"authorization": "Bearer secret"}
                }
              ],
              "allowed_methods": ["GET", "POST"]
            }"#,
        )
        .unwrap()
    }

    #[test]
    fn allowed_origins_use_effective_ports() {
        let policy = sample_policy();
        let url = Url::parse("https://api.example.test:443/v1/demo").unwrap();
        assert!(policy.match_url(&url).is_ok());
    }

    #[test]
    fn path_prefix_blocks_sibling_paths() {
        let policy = sample_policy();
        let error = policy
            .match_url(&Url::parse("https://api.example.test/v12/demo").unwrap())
            .unwrap_err();
        assert_eq!(error.to_string(), "policy denied: URL is not allowed by network policy: https://api.example.test/v12/demo");
    }

    #[test]
    fn schemes_are_restricted() {
        let policy = sample_policy();
        let error = policy
            .match_url(&Url::parse("http://api.example.test/v1/demo").unwrap())
            .unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::PolicyDenied);
    }

    #[test]
    fn methods_are_restricted() {
        let policy = sample_policy();
        let error = policy.allows_method("DELETE").unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::PolicyDenied);
    }

    #[test]
    fn private_literal_ips_are_blocked() {
        let policy = sample_policy();
        let error = policy
            .ensure_remote_addrs(&[IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))])
            .unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::PolicyDenied);
    }

    #[test]
    fn public_ips_are_allowed() {
        let policy = sample_policy();
        assert!(policy
            .ensure_remote_addrs(&[IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))])
            .is_ok());
    }
}
