#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

// Viewer owns only this small preferences file. It never initializes Curator's
// database or server and every request is pinned to a Tailnet peer IP.
slint::include_modules!();

use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
    net::IpAddr,
    path::PathBuf,
    time::Duration,
};

const DEFAULT_PORT: u16 = 42168;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SavedHost {
    name: String,
    endpoint: String,
    instance_id: String,
}
#[derive(Debug, Default, Serialize, Deserialize)]
struct HostStore {
    #[serde(default)]
    hosts: Vec<SavedHost>,
}
#[derive(Deserialize)]
struct SystemInfo {
    edition: String,
    api_protocol: String,
    instance_id: String,
    tailnet_only: bool,
}
#[derive(Deserialize)]
struct TailscaleStatus {
    #[serde(rename = "BackendState")]
    backend_state: Option<String>,
    #[serde(rename = "Peer", default)]
    peers: HashMap<String, TailscaleNode>,
    #[serde(rename = "Self")]
    self_node: Option<TailscaleNode>,
}
#[derive(Deserialize)]
struct TailscaleNode {
    #[serde(rename = "DNSName")]
    dns_name: Option<String>,
    #[serde(rename = "HostName")]
    host_name: Option<String>,
    #[serde(rename = "TailscaleIPs", default)]
    tailscale_ips: Vec<String>,
}
struct TailnetPeer {
    names: BTreeSet<String>,
    ips: BTreeSet<IpAddr>,
}

fn preferences_path() -> Result<PathBuf, String> {
    let directory = dirs::config_dir()
        .ok_or("No user configuration directory is available.")?
        .join("Curator Viewer");
    std::fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
    Ok(directory.join("hosts.json"))
}
fn load_hosts() -> Result<HostStore, String> {
    match std::fs::read_to_string(preferences_path()?) {
        Ok(text) => serde_json::from_str(&text).map_err(|error| error.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(HostStore::default()),
        Err(error) => Err(error.to_string()),
    }
}
fn save_host(host: SavedHost) -> Result<(), String> {
    let mut store = load_hosts()?;
    if let Some(previous) = store
        .hosts
        .iter_mut()
        .find(|item| item.instance_id == host.instance_id)
    {
        *previous = host;
    } else {
        store.hosts.push(host);
    }
    let text = serde_json::to_string_pretty(&store).map_err(|error| error.to_string())?;
    std::fs::write(preferences_path()?, text).map_err(|error| error.to_string())
}
fn normalized_endpoint(raw: &str) -> Result<reqwest::Url, String> {
    let mut url = reqwest::Url::parse(raw.trim())
        .map_err(|_| "Enter an http:// Tailnet host URL.".to_string())?;
    if url.scheme() != "http"
        || url.host_str().is_none()
        || url.username() != ""
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "Enter only an http:// Tailnet host origin without credentials or a path.".into(),
        );
    }
    if url.port().is_none() {
        url.set_port(Some(DEFAULT_PORT))
            .map_err(|_| "Could not set the Curator port.")?;
    }
    Ok(url)
}
async fn tailnet_peers() -> Result<Vec<TailnetPeer>, String> {
    let output = tokio::time::timeout(
        Duration::from_secs(3),
        curator::process::command("tailscale")
            .args(["status", "--json"])
            .output(),
    )
    .await
    .map_err(|_| "Timed out waiting for Tailscale.")?
    .map_err(|_| "Tailscale is not available on this device.")?;
    if !output.status.success() {
        return Err("Tailscale did not return a connected peer inventory.".into());
    }
    let status: TailscaleStatus = serde_json::from_slice(&output.stdout)
        .map_err(|_| "Tailscale returned unreadable peer data.")?;
    if !status
        .backend_state
        .as_deref()
        .is_some_and(|state| state.eq_ignore_ascii_case("running"))
    {
        return Err("Tailscale is not connected.".into());
    }
    let mut nodes: Vec<_> = status.peers.into_values().collect();
    if let Some(node) = status.self_node {
        nodes.push(node);
    }
    Ok(nodes
        .into_iter()
        .filter_map(|node| {
            let ips = node
                .tailscale_ips
                .iter()
                .filter_map(|value| value.parse().ok())
                .collect::<BTreeSet<IpAddr>>();
            if ips.is_empty() {
                return None;
            }
            let mut names = BTreeSet::new();
            if let Some(name) = node.dns_name {
                names.insert(name.trim_end_matches('.').to_ascii_lowercase());
            }
            if let Some(name) = node.host_name {
                names.insert(name.to_ascii_lowercase());
            }
            Some(TailnetPeer { names, ips })
        })
        .collect())
}
fn origin(ip: IpAddr, port: u16) -> String {
    match ip {
        IpAddr::V4(ip) => format!("http://{ip}:{port}"),
        IpAddr::V6(ip) => format!("http://[{ip}]:{port}"),
    }
}
async fn connect(endpoint: &str) -> Result<(String, String), String> {
    let url = normalized_endpoint(endpoint)?;
    let name = url
        .host_str()
        .unwrap()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let peers = tailnet_peers().await?;
    let peer = peers
        .iter()
        .find(|peer| {
            name.parse::<IpAddr>()
                .map(|ip| peer.ips.contains(&ip))
                .unwrap_or_else(|_| peer.names.contains(&name))
        })
        .ok_or("That host is not in this device's Tailnet peer inventory.")?;
    let mut addresses = if let Ok(ip) = name.parse() {
        vec![ip]
    } else {
        tokio::net::lookup_host((
            name.as_str(),
            url.port_or_known_default().unwrap_or(DEFAULT_PORT),
        ))
        .await
        .map_err(|_| "Could not resolve that Tailnet hostname.")?
        .map(|address| address.ip())
        .collect()
    };
    addresses.sort();
    addresses.dedup();
    if addresses.is_empty() || addresses.iter().any(|ip| !peer.ips.contains(ip)) {
        return Err(
            "The host did not resolve solely to the Tailnet peer reported by Tailscale.".into(),
        );
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(|error| error.to_string())?;
    let pinned = origin(
        addresses[0],
        url.port_or_known_default().unwrap_or(DEFAULT_PORT),
    );
    let info: SystemInfo = client
        .get(format!("{pinned}/api/system/info"))
        .send()
        .await
        .map_err(|error| error.to_string())?
        .error_for_status()
        .map_err(|error| error.to_string())?
        .json()
        .await
        .map_err(|_| "The peer did not return a Curator system-info response.")?;
    if info.api_protocol != curator::API_PROTOCOL
        || !matches!(info.edition.as_str(), "host" | "server")
        || !info.tailnet_only
    {
        return Err("The peer is not a compatible Tailnet-only Curator Host or Server.".into());
    }
    let summary: serde_json::Value = match client
        .get(format!("{pinned}/api/library/summary"))
        .send()
        .await
    {
        Ok(response) => response.json().await.unwrap_or_default(),
        Err(_) => serde_json::Value::Null,
    };
    let message = if summary.is_null() {
        format!("Connected to {}.", info.edition)
    } else {
        format!("Connected to {}: {}", info.edition, summary)
    };
    Ok((info.instance_id, message))
}

fn main() -> Result<(), slint::PlatformError> {
    let runtime = tokio::runtime::Runtime::new().expect("Could not start Viewer runtime");
    let window = CuratorViewerWindow::new()?;
    let weak = window.as_weak();
    window.on_connect(move |name, endpoint| {
        let result = runtime.block_on(connect(&endpoint));
        if let Some(window) = weak.upgrade() {
            match result {
                Ok((instance_id, message)) => {
                    let _ = save_host(SavedHost {
                        name: name.to_string(),
                        endpoint: endpoint.to_string(),
                        instance_id,
                    });
                    window.set_status(message.into());
                }
                Err(error) => window.set_status(error.into()),
            }
        }
    });
    let stored = load_hosts().unwrap_or_default();
    if let Some(host) = stored.hosts.first() {
        window.set_host_name(host.name.clone().into());
        window.set_endpoint(host.endpoint.clone().into());
    }
    window.set_status(
        format!(
            "{} saved host(s). Viewer stores no library database.",
            stored.hosts.len()
        )
        .into(),
    );
    window.run()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn endpoint_requires_a_plain_http_origin() {
        assert!(normalized_endpoint("https://example.com").is_err());
        assert!(normalized_endpoint("http://example.com/a").is_err());
        assert_eq!(
            normalized_endpoint("http://host.tailnet.ts.net")
                .unwrap()
                .port(),
            Some(DEFAULT_PORT)
        );
    }
}
