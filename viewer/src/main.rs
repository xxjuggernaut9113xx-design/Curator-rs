#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

// Viewer owns only this small preferences file. It never initializes Curator's
// database or server and every request is pinned to a Tailnet peer IP.
slint::include_modules!();

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
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
    /// Keep preferences introduced by a newer Viewer build intact when an
    /// older build merely changes a host name or endpoint.
    #[serde(default, flatten)]
    extra: BTreeMap<String, Value>,
}
#[derive(Debug, Default, Serialize, Deserialize)]
struct HostStore {
    #[serde(default)]
    hosts: Vec<SavedHost>,
    #[serde(default, flatten)]
    extra: BTreeMap<String, Value>,
}
#[derive(Deserialize)]
struct SystemInfo {
    edition: String,
    api_protocol: String,
    instance_id: String,
    tailnet_only: bool,
    #[serde(default)]
    viewer_permissions: curator::native::ViewerPermissions,
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
        .join("tech.webmaster19083.curator.viewer");
    std::fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
    Ok(directory.join("hosts.json"))
}
fn legacy_preferences_path() -> Result<PathBuf, String> {
    Ok(dirs::config_dir()
        .ok_or("No user configuration directory is available.")?
        .join("Curator Viewer")
        .join("hosts.json"))
}
fn load_hosts() -> Result<HostStore, String> {
    let primary = preferences_path()?;
    match std::fs::read_to_string(&primary) {
        Ok(text) => serde_json::from_str(&text).map_err(|error| error.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let legacy = legacy_preferences_path()?;
            match std::fs::read_to_string(legacy) {
                Ok(text) => serde_json::from_str(&text).map_err(|error| error.to_string()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    Ok(HostStore::default())
                }
                Err(error) => Err(error.to_string()),
            }
        }
        Err(error) => Err(error.to_string()),
    }
}
fn write_hosts(store: &HostStore) -> Result<(), String> {
    use std::io::Write;
    let path = preferences_path()?;
    let parent = path.parent().ok_or("Invalid Viewer preferences path")?;
    let mut file = tempfile::NamedTempFile::new_in(parent).map_err(|error| error.to_string())?;
    serde_json::to_writer_pretty(&mut file, store).map_err(|error| error.to_string())?;
    file.flush().map_err(|error| error.to_string())?;
    file.as_file()
        .sync_all()
        .map_err(|error| error.to_string())?;
    file.persist(path).map_err(|error| error.to_string())?;
    Ok(())
}
fn save_host(mut host: SavedHost) -> Result<(), String> {
    let mut store = load_hosts()?;
    if let Some(previous) = store
        .hosts
        .iter_mut()
        .find(|item| item.instance_id == host.instance_id)
    {
        host.extra = std::mem::take(&mut previous.extra);
        *previous = host;
    } else {
        store.hosts.push(host);
    }
    write_hosts(&store)
}
fn remove_host(index: usize) -> Result<(), String> {
    let mut store = load_hosts()?;
    if index >= store.hosts.len() {
        return Err("The saved Host no longer exists.".into());
    }
    store.hosts.remove(index);
    write_hosts(&store)
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
async fn connect(
    endpoint: &str,
) -> Result<(String, String, curator::native::ViewerPermissions), String> {
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
    let port = url.port_or_known_default().unwrap_or(DEFAULT_PORT);
    let mut failures = Vec::new();
    for address in addresses {
        let pinned = origin(address, port);
        let info = match client
            .get(format!("{pinned}/api/system/info"))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
        {
            Ok(response) => match response.json::<SystemInfo>().await {
                Ok(info) => info,
                Err(_) => {
                    failures.push(format!("{address}: invalid system information"));
                    continue;
                }
            },
            Err(error) => {
                failures.push(format!("{address}: {error}"));
                continue;
            }
        };
        if info.api_protocol == curator::API_PROTOCOL
            && matches!(info.edition.as_str(), "host" | "server")
            && info.tailnet_only
        {
            return Ok((info.instance_id, pinned, info.viewer_permissions));
        }
        failures.push(format!("{address}: incompatible Curator host"));
    }
    Err(format!(
        "Could not connect to a compatible Tailnet Curator host ({})",
        failures.join("; ")
    ))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Runtime::new()?;
    while let Some(client) = choose_host(&runtime)? {
        if curator_desktop::run_ui_with_exit(&runtime, client, false)?
            != curator_desktop::NativeExit::SwitchHost
        {
            break;
        }
    }
    Ok(())
}

fn choose_host(
    runtime: &tokio::runtime::Runtime,
) -> Result<Option<curator::native::Client>, Box<dyn std::error::Error>> {
    use slint::ComponentHandle;
    use std::{cell::RefCell, rc::Rc, sync::mpsc};
    let window = CuratorViewerWindow::new()?;
    let selected = Rc::new(RefCell::new(None));
    let (tx, rx) = mpsc::channel();
    let handle = runtime.handle().clone();
    let weak = window.as_weak();
    window.on_connect(move |name, endpoint| {
        if let Some(w) = weak.upgrade() {
            w.set_status("Validating Tailnet host…".into());
            w.set_busy(true);
        }
        let tx = tx.clone();
        handle.spawn(async move {
            let result = connect(&endpoint).await;
            let _ = tx.send((name.to_string(), endpoint.to_string(), result));
        });
    });
    let weak = window.as_weak();
    window.on_select_saved(move |index| {
        let Ok(store) = load_hosts() else {
            return;
        };
        let Some(host) = store.hosts.get(index as usize) else {
            return;
        };
        if let Some(w) = weak.upgrade() {
            w.set_host_name(host.name.clone().into());
            w.set_endpoint(host.endpoint.clone().into());
            w.set_status(format!("Selected saved Host: {}", host.name).into());
        }
    });
    let weak = window.as_weak();
    window.on_remove_saved(move |index| match remove_host(index as usize) {
        Ok(()) => {
            if let Some(w) = weak.upgrade() {
                match load_hosts() {
                    Ok(store) => {
                        w.set_saved_hosts(slint::ModelRc::new(slint::VecModel::from(
                            store
                                .hosts
                                .iter()
                                .map(|host| format!("{} — {}", host.name, host.endpoint).into())
                                .collect::<Vec<slint::SharedString>>(),
                        )));
                        w.set_status("Saved Host removed.".into());
                    }
                    Err(error) => w.set_status(error.into()),
                }
            }
        }
        Err(error) => {
            if let Some(w) = weak.upgrade() {
                w.set_status(error.into());
            }
        }
    });
    let timer = slint::Timer::default();
    let weak = window.as_weak();
    let target = selected.clone();
    timer.start(
        slint::TimerMode::Repeated,
        Duration::from_millis(50),
        move || {
            let Some(w) = weak.upgrade() else {
                return;
            };
            while let Ok((name, endpoint, result)) = rx.try_recv() {
                w.set_busy(false);
                let outcome = (|| -> Result<(), String> {
                    let (instance_id, pinned, permissions) = result?;
                    let store = load_hosts()?;
                    if store
                        .hosts
                        .iter()
                        .any(|host| host.endpoint == endpoint && host.instance_id != instance_id)
                    {
                        return Err("This saved address now identifies a different library.".into());
                    }
                    let client = curator::native::RemoteClient::from_validated_peer_with_identity(
                        &pinned,
                        permissions,
                        instance_id.clone(),
                    )?;
                    if name.trim().is_empty() {
                        return Err("Give the host a name.".into());
                    }
                    save_host(SavedHost {
                        name,
                        endpoint,
                        instance_id,
                        extra: BTreeMap::new(),
                    })?;
                    *target.borrow_mut() = Some(client);
                    w.hide().map_err(|e| e.to_string())?;
                    slint::quit_event_loop().map_err(|e| e.to_string())?;
                    Ok(())
                })();
                if let Err(error) = outcome {
                    w.set_status(error.into());
                }
            }
        },
    );
    match load_hosts() {
        Ok(store) => {
            window.set_saved_hosts(slint::ModelRc::new(slint::VecModel::from(
                store
                    .hosts
                    .iter()
                    .map(|host| format!("{} — {}", host.name, host.endpoint).into())
                    .collect::<Vec<slint::SharedString>>(),
            )));
            if let Some(host) = store.hosts.first() {
                window.set_host_name(host.name.clone().into());
                window.set_endpoint(host.endpoint.clone().into());
            }
        }
        Err(error) => window.set_status(error.into()),
    }
    window.run()?;
    timer.stop();
    drop(timer);
    drop(window);
    let selected_client = selected
        .borrow_mut()
        .take()
        .map(curator::native::Client::Remote);
    Ok(selected_client)
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

    #[test]
    fn saved_hosts_preserve_unknown_fields_during_migration() {
        let store: HostStore = serde_json::from_value(serde_json::json!({
            "hosts": [{
                "name": "Desk",
                "endpoint": "http://100.64.1.2:42168",
                "instance_id": "library-1",
                "future_host_preference": {"reconnect": true}
            }],
            "future_store_preference": "kept"
        }))
        .unwrap();
        let value = serde_json::to_value(store).unwrap();
        assert_eq!(value["future_store_preference"], "kept");
        assert_eq!(
            value["hosts"][0]["future_host_preference"]["reconnect"],
            true
        );
    }
}
