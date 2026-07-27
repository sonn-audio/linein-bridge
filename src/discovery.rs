use anyhow::{Context, Result};
use mdns_sd::{ServiceDaemon, ServiceEvent};
use std::collections::HashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct DiscoveredServer {
    pub base_url: String,
    pub register_path: String,
    pub status_path: String,
    pub txt: HashMap<String, String>,
}

pub fn discover_server(
    preferred_name: Option<&str>,
    preferred_mac: Option<&str>,
) -> Result<DiscoveredServer> {
    const SERVICE_TYPE: &str = "_sonncore._tcp.local.";
    let mdns = ServiceDaemon::new().context("start mDNS daemon")?;
    let receiver = mdns.browse(SERVICE_TYPE).context("browse mDNS services")?;
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut candidates = Vec::new();

    while Instant::now() < deadline {
        let timeout = deadline.saturating_duration_since(Instant::now());
        match receiver.recv_timeout(timeout) {
            Ok(event) => {
                if let ServiceEvent::ServiceResolved(info) = event {
                    let txt = info
                        .get_properties()
                        .iter()
                        .map(|prop| (prop.key().to_string(), prop.val_str().to_string()))
                        .collect::<HashMap<_, _>>();
                    let host = resolve_host(info.get_addresses(), info.get_hostname());
                    let port = info.get_port();
                    let base_url = format!("http://{}:{}", host, port);
                    let api_prefix = txt
                        .get("api")
                        .cloned()
                        .unwrap_or_else(|| "/api".to_string());
                    let register_path = normalize_path(
                        txt.get("linein_register")
                            .cloned()
                            .unwrap_or_else(|| format!("{}/linein/bridges/register", api_prefix)),
                    );
                    let status_path =
                        normalize_path(txt.get("linein_status").cloned().unwrap_or_else(|| {
                            format!("{}/linein/bridges/{{bridge_id}}/status", api_prefix)
                        }));
                    candidates.push(DiscoveredServer {
                        base_url,
                        register_path,
                        status_path,
                        txt,
                    });
                }
            }
            Err(_) => break,
        }
    }

    if candidates.is_empty() {
        shutdown_mdns(&mdns, SERVICE_TYPE);
        anyhow::bail!("no _sonncore._tcp services found");
    }

    if candidates.len() == 1 {
        shutdown_mdns(&mdns, SERVICE_TYPE);
        return Ok(candidates.remove(0));
    }

    if let Some(mac) = preferred_mac {
        if let Some(server) = candidates
            .iter()
            .find(|server| server.txt.get("mac").map(|v| v == mac).unwrap_or(false))
        {
            shutdown_mdns(&mdns, SERVICE_TYPE);
            return Ok(server.clone());
        }
    }

    if let Some(name) = preferred_name {
        if let Some(server) = candidates
            .iter()
            .find(|server| server.txt.get("name").map(|v| v == name).unwrap_or(false))
        {
            shutdown_mdns(&mdns, SERVICE_TYPE);
            return Ok(server.clone());
        }
    }

    shutdown_mdns(&mdns, SERVICE_TYPE);
    Ok(candidates.remove(0))
}

fn resolve_host(addresses: &std::collections::HashSet<IpAddr>, hostname: &str) -> String {
    if let Some(addr) = addresses.iter().find(|addr| addr.is_ipv4()) {
        return addr.to_string();
    }
    let trimmed = hostname.trim_end_matches('.');
    trimmed.to_string()
}

fn normalize_path(path: String) -> String {
    if path.starts_with('/') {
        path
    } else {
        format!("/{}", path)
    }
}

fn shutdown_mdns(mdns: &ServiceDaemon, service_type: &str) {
    let _ = service_type;
    if let Ok(receiver) = mdns.shutdown() {
        let _ = receiver.recv_timeout(Duration::from_millis(200));
    }
}
