//! The one Curator HTTP listener set and its remote-access diagnostics.
//!
//! The desktop shell, headless fallback, and any remote browser all use the
//! same Axum router. It is always bound to loopback and to addresses reported
//! by the local Tailscale daemon. An ordinary-LAN listener is strictly
//! opt-in (`lan_access_enabled`): enabling it binds the configured port on
//! all local interfaces, so an accidentally shared Wi-Fi or Ethernet network
//! never silently becomes an unauthenticated Curator admin surface.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::AppState;

/// Kept stable for local bookmarks. Loopback always binds at this port; the
/// opt-in LAN wildcard and Tailnet listeners reuse it.
pub const DEFAULT_SERVER_PORT: u16 = 42168;
const LISTENER_REFRESH_INTERVAL: Duration = Duration::from_secs(15);

/// Bookkeeping key for the non-loopback listeners Curator owns.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum ExtraListenerKey {
    Tailscale(IpAddr),
}

/// Process-lifetime state for every socket owned by Curator. The desktop
/// window deliberately has no ownership relationship with this state: hiding
/// it to the tray must not disconnect a browser fallback client.
pub struct ServerStatus {
    port: AtomicU16,
    running: AtomicBool,
    bound_addresses: RwLock<BTreeSet<SocketAddr>>,
    extra_listeners: Mutex<HashMap<ExtraListenerKey, CancellationToken>>,
    start_lock: tokio::sync::Mutex<()>,
    /// Cancels the primary listener set. The loopback socket and the LAN
    /// wildcard share one port, so enabling/disabling LAN swaps the primary
    /// instead of binding the port twice (the second bind fails on Windows
    /// and races on every other OS).
    primary: Mutex<Option<CancellationToken>>,
    /// Whether the primary socket is currently the LAN wildcard.
    primary_lan: AtomicBool,
    /// The periodic listener refresher is process-lifetime: a restart
    /// through `start_http_server_on` must not stack a second loop.
    refresher_spawned: AtomicBool,
}

impl ServerStatus {
    pub fn new(port: u16) -> Self {
        Self {
            port: AtomicU16::new(port),
            running: AtomicBool::new(false),
            bound_addresses: RwLock::new(BTreeSet::new()),
            extra_listeners: Mutex::new(HashMap::new()),
            start_lock: tokio::sync::Mutex::new(()),
            primary: Mutex::new(None),
            primary_lan: AtomicBool::new(false),
            refresher_spawned: AtomicBool::new(false),
        }
    }

    pub fn port(&self) -> u16 {
        self.port.load(Ordering::Acquire)
    }

    pub fn running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub fn bound_addresses(&self) -> Vec<SocketAddr> {
        self.bound_addresses
            .read()
            .map(|addresses| addresses.iter().copied().collect())
            .unwrap_or_default()
    }

    fn set_port(&self, port: u16) {
        self.port.store(port, Ordering::Release);
    }

    fn register(&self, address: SocketAddr) {
        if let Ok(mut addresses) = self.bound_addresses.write() {
            addresses.insert(address);
            self.running.store(!addresses.is_empty(), Ordering::Release);
        }
    }

    fn unregister(&self, address: SocketAddr) {
        if let Ok(mut addresses) = self.bound_addresses.write() {
            addresses.remove(&address);
            self.running.store(!addresses.is_empty(), Ordering::Release);
        }
    }

    fn reserve_extra(&self, key: ExtraListenerKey) -> Option<CancellationToken> {
        let mut listeners = self.extra_listeners.lock().ok()?;
        if listeners.contains_key(&key) {
            return None;
        }
        let cancellation = CancellationToken::new();
        listeners.insert(key, cancellation.clone());
        Some(cancellation)
    }

    fn release_extra(&self, key: ExtraListenerKey) {
        if let Ok(mut listeners) = self.extra_listeners.lock() {
            listeners.remove(&key);
        }
    }

    fn active_extra(&self) -> Vec<(ExtraListenerKey, CancellationToken)> {
        self.extra_listeners
            .lock()
            .map(|listeners| {
                listeners
                    .iter()
                    .map(|(key, cancellation)| (*key, cancellation.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn primary_is_lan(&self) -> bool {
        self.primary_lan.load(Ordering::Acquire)
    }

    fn set_primary(&self, cancellation: CancellationToken, lan: bool) {
        if let Ok(mut primary) = self.primary.lock() {
            *primary = Some(cancellation);
        }
        self.primary_lan.store(lan, Ordering::Release);
    }

    fn take_primary(&self) -> Option<CancellationToken> {
        self.primary
            .lock()
            .ok()
            .and_then(|mut primary| primary.take())
    }
}

/// Bind the shared server at Curator's normal port.
pub async fn start_http_server(state: &AppState) -> Result<u16> {
    start_http_server_on(state, DEFAULT_SERVER_PORT).await
}

/// Startup body shared by `start_http_server_on` and the refresh-cycle
/// restart path. The caller must hold `start_lock`; on success the primary
/// bookkeeping matches the current LAN setting.
async fn start_primary(state: &AppState, requested_port: u16) -> Result<u16> {
    // Loopback is mandatory. It is the common desktop/browser fallback and
    // remains available if Tailscale is absent or disconnected. When the
    // opt-in LAN mode is set, the wildcard primary covers loopback too, so
    // only one of the two ever owns the port.
    let lan = state.settings.read().await.lan_access_enabled;
    let token = CancellationToken::new();
    let port = spawn_primary_set(state, requested_port, lan, &token).await?;
    state.remote_server.set_port(port);
    state.remote_server.set_primary(token, lan);

    refresh_tailscale_listeners(state).await;
    spawn_listener_refresher(state);
    info!("Curator HTTP server listening on loopback port {port}; Tailnet listeners are added only when Tailscale reports local addresses");
    Ok(port)
}

/// A port of zero is useful for tests. Production calls start_http_server.
pub async fn start_http_server_on(state: &AppState, requested_port: u16) -> Result<u16> {
    {
        let _start_guard = state.remote_server.start_lock.lock().await;
        if !state.remote_server.running() {
            return start_primary(state, requested_port).await;
        }
    }

    // Already running: reconcile the LAN mode without holding the startup
    // guard. refresh_lan_listener -> swap_primary takes the same lock, so
    // holding it across this call would self-deadlock.
    refresh_lan_listener(state).await;
    Ok(state.remote_server.port())
}

/// Bind the primary socket(s) for `lan` mode and spawn their accept loops,
/// returning the bound port. LAN mode binds the IPv4 wildcard (which also
/// serves loopback); otherwise the IPv4 loopback plus a best-effort IPv6
/// loopback are bound. The two modes never bind at once: the second bind of
/// the same port fails outright on Windows.
async fn spawn_primary_set(
    state: &AppState,
    requested_port: u16,
    lan: bool,
    token: &CancellationToken,
) -> Result<u16> {
    if lan {
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, requested_port)))
            .await
            .with_context(|| format!("binding Curator LAN wildcard on 0.0.0.0:{requested_port}"))?;
        let port = listener
            .local_addr()
            .context("reading Curator LAN listener address")?
            .port();
        spawn_primary_listener(state, listener, token.clone());
        info!("Curator LAN listener enabled on 0.0.0.0:{port}");
        return Ok(port);
    }
    let ipv4_listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, requested_port)))
        .await
        .with_context(|| format!("binding Curator on 127.0.0.1:{requested_port}"))?;
    let port = ipv4_listener
        .local_addr()
        .context("reading Curator loopback listener address")?
        .port();
    spawn_primary_listener(state, ipv4_listener, token.clone());
    // IPv6 loopback is an additional local endpoint, never a wildcard bind.
    match TcpListener::bind(SocketAddr::from((Ipv6Addr::LOCALHOST, port))).await {
        Ok(listener) => spawn_primary_listener(state, listener, token.clone()),
        Err(error) => info!("Curator IPv6 loopback unavailable on [::1]:{port}: {error}"),
    }
    Ok(port)
}

/// Serve one primary socket until the primary set is swapped or the process
/// shuts down. Unregisters its address on exit so swaps can wait for the OS
/// to release the port before rebinding.
fn spawn_primary_listener(state: &AppState, listener: TcpListener, token: CancellationToken) {
    let address = match listener.local_addr() {
        Ok(address) => address,
        Err(error) => {
            warn!("Could not read Curator listener address: {error}");
            return;
        }
    };
    state.remote_server.register(address);
    let app = crate::router(state.clone());
    let global_shutdown = state.shutdown.clone();
    let status = Arc::clone(&state.remote_server);
    state.server_tasks.spawn(async move {
        let result = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            tokio::select! {
                _ = global_shutdown.cancelled() => {},
                _ = token.cancelled() => {},
            }
        })
        .await;
        if let Err(error) = result {
            warn!("Curator HTTP listener {address} stopped unexpectedly: {error}");
        }
        status.unregister(address);
    });
}

fn spawn_listener(
    state: &AppState,
    listener: TcpListener,
    extra: Option<(ExtraListenerKey, CancellationToken)>,
) {
    let address = match listener.local_addr() {
        Ok(address) => address,
        Err(error) => {
            warn!("Could not read Curator listener address: {error}");
            return;
        }
    };
    state.remote_server.register(address);
    let app = crate::router(state.clone());
    let global_shutdown = state.shutdown.clone();
    let status = Arc::clone(&state.remote_server);
    state.server_tasks.spawn(async move {
        let local_shutdown = extra.as_ref().map(|(_, token)| token.clone());
        let result = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            if let Some(local_shutdown) = local_shutdown {
                tokio::select! {
                    _ = global_shutdown.cancelled() => {},
                    _ = local_shutdown.cancelled() => {},
                }
            } else {
                global_shutdown.cancelled().await;
            }
        })
        .await;
        if let Err(error) = result {
            warn!("Curator HTTP listener {address} stopped unexpectedly: {error}");
        }
        status.unregister(address);
        if let Some((key, _)) = extra {
            status.release_extra(key);
        }
    });
}

fn spawn_listener_refresher(state: &AppState) {
    // The refresher loop runs until process shutdown; restarts must not
    // stack additional loops.
    if state
        .remote_server
        .refresher_spawned
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    let state = state.clone();
    let server_tasks = state.server_tasks.clone();
    server_tasks.spawn(async move {
        loop {
            tokio::select! {
                _ = state.shutdown.cancelled() => break,
                _ = tokio::time::sleep(LISTENER_REFRESH_INTERVAL) => {
                    refresh_tailscale_listeners(&state).await;
                    refresh_lan_listener(&state).await;
                }
            }
        }
    });
}

/// Reconcile the Tailscale-only sockets with the daemon's live status. A
/// removed Tailscale address is cancelled immediately; a new one is bound
/// without restarting the desktop app or moving to a wildcard socket. When
/// the LAN wildcard is the primary, Tailnet addresses are already served and
/// specific-IP binds cannot share the port, so extras are released instead.
async fn refresh_tailscale_listeners(state: &AppState) {
    if state.remote_server.primary_is_lan() {
        for (key, cancellation) in state.remote_server.active_extra() {
            if matches!(key, ExtraListenerKey::Tailscale(_)) {
                cancellation.cancel();
            }
        }
        return;
    }
    let snapshot = detect_tailscale().await;
    let desired: HashSet<IpAddr> = snapshot.addresses.into_iter().collect();
    for (key, cancellation) in state.remote_server.active_extra() {
        let ExtraListenerKey::Tailscale(address) = key;
        if !desired.contains(&address) {
            cancellation.cancel();
        }
    }
    for address in desired {
        let key = ExtraListenerKey::Tailscale(address);
        let Some(cancellation) = state.remote_server.reserve_extra(key) else {
            continue;
        };
        let socket = SocketAddr::new(address, state.remote_server.port());
        match TcpListener::bind(socket).await {
            Ok(listener) => {
                info!("Curator Tailnet listener enabled on {socket}");
                spawn_listener(state, listener, Some((key, cancellation)));
            }
            Err(error) => {
                // A stale status entry, a race while reconnecting, or an IPv6
                // capability gap must leave Curator local-only rather than
                // falling back to a LAN wildcard.
                state.remote_server.release_extra(key);
                info!("Curator Tailnet listener unavailable on {socket}: {error}");
            }
        }
    }
}

/// Reconcile the opt-in LAN mode with the current setting. The LAN wildcard
/// and the loopback socket share one port, so a mode change swaps the
/// primary listener set instead of binding the port twice.
pub(crate) async fn refresh_lan_listener(state: &AppState) {
    let enabled = state.settings.read().await.lan_access_enabled;
    if enabled == state.remote_server.primary_is_lan() {
        return;
    }
    if !state.remote_server.running() {
        // A failed swap can leave no primary bound at all; `swap_primary`
        // early-returns when nothing is running, so rebuild through the
        // startup body instead. This also makes the "next refresh cycle
        // retries" recovery claim true after a double bind failure.
        let _start_guard = state.remote_server.start_lock.lock().await;
        if !state.remote_server.running() {
            if let Err(error) = start_primary(state, state.remote_server.port()).await {
                warn!("Curator listener refresh could not restart the HTTP server: {error:#}");
            }
        }
        return;
    }
    swap_primary(state, enabled).await;
}

/// Cancel the current primary listener set, wait for the OS to release the
/// port, then bind the other mode's primary. Serialized with startup so two
/// swaps can never race. If the rebind fails the next refresh cycle retries.
async fn swap_primary(state: &AppState, lan: bool) {
    let _start_guard = state.remote_server.start_lock.lock().await;
    if lan == state.remote_server.primary_is_lan() || !state.remote_server.running() {
        return;
    }
    // Tailnet extras cannot share the port with the wildcard; they are
    // redundant while it is bound and are restored by their own refresher
    // when loopback returns.
    if let Some(token) = state.remote_server.take_primary() {
        token.cancel();
    }
    for (_, cancellation) in state.remote_server.active_extra() {
        cancellation.cancel();
    }
    let port = state.remote_server.port();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while state.remote_server.running() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let token = CancellationToken::new();
    match spawn_primary_set(state, port, lan, &token).await {
        Ok(bound_port) => {
            state.remote_server.set_port(bound_port);
            state.remote_server.set_primary(token, lan);
            if lan {
                info!("Curator LAN mode enabled; loopback is served by the wildcard");
            } else {
                info!("Curator LAN mode disabled; loopback listener restored");
                refresh_tailscale_listeners(state).await;
            }
        }
        Err(error) => {
            warn!("Curator primary listener unavailable after LAN mode change: {error:#}");
            // The old listener set is already cancelled. Restore the previous
            // mode so the host is never left with no primary bound; the next
            // refresh cycle retries the requested mode because the setting
            // still differs from the restored bookkeeping.
            let restore = CancellationToken::new();
            match spawn_primary_set(state, port, !lan, &restore).await {
                Ok(bound_port) => {
                    state.remote_server.set_port(bound_port);
                    state.remote_server.set_primary(restore, !lan);
                    if !lan {
                        refresh_tailscale_listeners(state).await;
                    }
                    info!(
                        "Curator primary listener restored to previous mode after failed LAN change"
                    );
                }
                Err(restore_error) => {
                    warn!(
                        "Curator could not restore the previous primary listener either: {restore_error:#}; the next refresh cycle will retry"
                    );
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RemoteAccessInfo {
    pub running: bool,
    pub server: &'static str,
    pub port: u16,
    pub bound_addresses: Vec<String>,
    pub local_urls: Vec<String>,
    /// Populated only when the opt-in LAN wildcard (`lan_access_enabled`) is
    /// bound. Retained as a named field for additive API compatibility.
    pub lan_urls: Vec<String>,
    pub tailscale_urls: Vec<String>,
    pub magicdns_hostname: Option<String>,
    pub access_scope: &'static str,
}

#[derive(Default)]
struct TailscaleSnapshot {
    addresses: Vec<IpAddr>,
    magicdns_hostname: Option<String>,
}

/// Read a fresh daemon snapshot so status updates as Wi-Fi/Tailscale changes.
pub async fn remote_access_info(state: &AppState) -> RemoteAccessInfo {
    let port = state.remote_server.port();
    let bound = state.remote_server.bound_addresses();
    let running = !bound.is_empty();
    let snapshot = detect_tailscale().await;
    let reported_tailscale: HashSet<IpAddr> = snapshot.addresses.into_iter().collect();

    // Under the LAN wildcard no Tailnet-specific socket is bound, but the
    // Tailnet addresses are still reachable through it.
    let wildcard_active = bound.iter().any(|address| address.ip().is_unspecified());
    let mut local_urls = bound
        .iter()
        .filter(|address| address.ip().is_loopback())
        .map(|address| url_for(address.ip(), port))
        .collect::<Vec<_>>();
    // The wildcard serves loopback as well; advertise it explicitly so the
    // status stays accurate when no loopback-specific socket is bound.
    if wildcard_active && !local_urls.iter().any(|url| url.contains("127.0.0.1")) {
        local_urls.push(url_for(IpAddr::V4(Ipv4Addr::LOCALHOST), port));
    }
    let tailscale_urls = if wildcard_active {
        let mut urls: Vec<String> = reported_tailscale
            .iter()
            .map(|address| url_for(*address, port))
            .collect();
        urls.sort();
        urls
    } else {
        bound
            .iter()
            .map(SocketAddr::ip)
            .filter(|address| reported_tailscale.contains(address))
            .map(|address| url_for(address, port))
            .collect::<Vec<_>>()
    };
    let lan_urls = bound
        .iter()
        .filter(|address| {
            let ip = address.ip();
            (ip.is_unspecified() || is_lan_address(ip)) && !reported_tailscale.contains(&ip)
        })
        .map(|address| url_for(address.ip(), port))
        .collect::<Vec<_>>();
    let magicdns_hostname = snapshot
        .magicdns_hostname
        .filter(|_| !tailscale_urls.is_empty());

    RemoteAccessInfo {
        running,
        server: if running { "Running" } else { "Stopped" },
        port,
        bound_addresses: bound
            .into_iter()
            .map(|address| address.to_string())
            .collect(),
        local_urls,
        lan_urls: lan_urls.clone(),
        tailscale_urls,
        magicdns_hostname,
        access_scope: if lan_urls.is_empty() {
            "loopback_and_tailscale_only"
        } else {
            "loopback_tailscale_and_lan"
        },
    }
}

/// Ordinary LAN addresses: private-use IPv4, IPv6 unique-local, and the
/// wildcard sockets that serve them. Link-local is included — a directly
/// connected peer is still "the local network".
fn is_lan_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            address.is_private()
                || address.is_link_local()
                || (address.octets()[0] == 100
                    && (address.octets()[1] & 0b1100_0000) == 0b0100_0000)
        }
        IpAddr::V6(address) => {
            let segments = address.segments();
            (segments[0] & 0xfe00) == 0xfc00 || address.is_unicast_link_local()
        }
    }
}

fn url_for(address: IpAddr, port: u16) -> String {
    match address {
        IpAddr::V4(address) => format!("http://{address}:{port}"),
        IpAddr::V6(address) => format!("http://[{address}]:{port}"),
    }
}

async fn command_output(command: &str, args: &[&str]) -> Option<String> {
    let output = timeout(
        Duration::from_secs(2),
        crate::process::command(command).args(args).output(),
    )
    .await
    .ok()?
    .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).into_owned())
}

async fn detect_tailscale() -> TailscaleSnapshot {
    let Some(output) = command_output("tailscale", &["status", "--json"]).await else {
        return TailscaleSnapshot::default();
    };
    parse_tailscale_status(&output)
}

fn parse_tailscale_status(text: &str) -> TailscaleSnapshot {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return TailscaleSnapshot::default();
    };
    let running = value
        .get("BackendState")
        .and_then(Value::as_str)
        .is_some_and(|state| state.eq_ignore_ascii_case("running"));
    let Some(self_node) = value.get("Self") else {
        return TailscaleSnapshot::default();
    };
    let online = self_node
        .get("Online")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !running || !online {
        return TailscaleSnapshot::default();
    }

    let addresses = self_node
        .get("TailscaleIPs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter_map(|value| value.parse::<IpAddr>().ok())
        .collect();
    let magicdns_hostname = self_node
        .get("DNSName")
        .and_then(Value::as_str)
        .map(|name| name.trim_end_matches('.'))
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            let hostname = self_node.get("HostName").and_then(Value::as_str)?;
            let suffix = value.get("MagicDNSSuffix").and_then(Value::as_str)?;
            (!hostname.is_empty() && !suffix.is_empty()).then(|| format!("{hostname}.{suffix}"))
        });
    TailscaleSnapshot {
        addresses,
        magicdns_hostname,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn parses_connected_tailscale_status_and_magicdns() {
        let fixture = r#"{
          "BackendState": "Running",
          "MagicDNSSuffix": "tailnet-name.ts.net",
          "Self": {
            "Online": true,
            "HostName": "curator-pc",
            "TailscaleIPs": ["100.83.44.1", "fd7a:115c:a1e0::123"]
          }
        }"#;
        let parsed = parse_tailscale_status(fixture);
        assert_eq!(parsed.addresses.len(), 2);
        assert_eq!(
            parsed.magicdns_hostname.as_deref(),
            Some("curator-pc.tailnet-name.ts.net")
        );
    }

    #[test]
    fn ignores_disconnected_tailscale_status() {
        let fixture =
            r#"{"BackendState":"Stopped","Self":{"Online":false,"TailscaleIPs":["100.64.0.1"]}}"#;
        assert!(parse_tailscale_status(fixture).addresses.is_empty());
    }

    #[test]
    fn formats_ipv6_urls_correctly() {
        assert_eq!(
            url_for("fd7a:115c:a1e0::123".parse().unwrap(), 42168),
            "http://[fd7a:115c:a1e0::123]:42168"
        );
    }

    #[tokio::test]
    async fn server_binds_no_lan_address_by_default() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let port = start_http_server_on(&state, 0).await.unwrap();
        let bound = state.remote_server.bound_addresses();
        let tailscale = detect_tailscale()
            .await
            .addresses
            .into_iter()
            .collect::<HashSet<_>>();
        assert!(bound.iter().any(|address| address.ip().is_loopback()));
        assert!(bound
            .iter()
            .all(|address| address.ip().is_loopback() || tailscale.contains(&address.ip())));
        let info = remote_access_info(&state).await;
        assert!(info.lan_urls.is_empty());
        assert_eq!(info.access_scope, "loopback_and_tailscale_only");

        let mut stream = tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port))
            .await
            .unwrap();
        stream
            .write_all(
                b"GET /api/remote-access HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200"));

        state.shutdown.cancel();
        state.server_tasks.close();
        state.server_tasks.wait().await;
        assert!(!state.remote_server.running());
    }

    #[tokio::test]
    async fn lan_listener_is_opt_in_and_advertised() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        state.settings.write().await.lan_access_enabled = true;
        let port = start_http_server_on(&state, 0).await.unwrap();
        let bound = state.remote_server.bound_addresses();
        assert!(bound.iter().any(|address| address.ip().is_unspecified()));
        let info = remote_access_info(&state).await;
        assert!(!info.lan_urls.is_empty());
        assert_eq!(info.access_scope, "loopback_tailscale_and_lan");

        // The wildcard serves ordinary HTTP on the loopback route too.
        let mut stream = tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port))
            .await
            .unwrap();
        stream
            .write_all(
                b"GET /api/remote-access HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200"));

        // Disabling the setting releases the wildcard without stopping the
        // loopback listener.
        state.settings.write().await.lan_access_enabled = false;
        refresh_lan_listener(&state).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let bound = state.remote_server.bound_addresses();
        assert!(bound.iter().all(|address| !address.ip().is_unspecified()));
        assert!(bound.iter().any(|address| address.ip().is_loopback()));

        state.shutdown.cancel();
        state.server_tasks.close();
        state.server_tasks.wait().await;
        assert!(!state.remote_server.running());
    }

    /// Regression test: calling `start_http_server_on` on an already-running
    /// server after flipping the LAN setting must swap the listener instead
    /// of self-deadlocking on the startup guard.
    #[tokio::test]
    async fn restart_call_on_running_server_swaps_lan_mode() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let port = start_http_server_on(&state, 0).await.unwrap();
        assert!(!state.remote_server.primary_is_lan());

        state.settings.write().await.lan_access_enabled = true;
        let second = tokio::time::timeout(Duration::from_secs(10), start_http_server_on(&state, 0))
            .await
            .expect("restart call on a running server must not deadlock")
            .unwrap();
        assert_eq!(second, port, "the swap reuses the same port");
        // Give the accept loops a moment to settle on the new socket set.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(state.remote_server.primary_is_lan());
        let bound = state.remote_server.bound_addresses();
        assert!(bound.iter().any(|address| address.ip().is_unspecified()));

        // And back to loopback through the same entry point.
        state.settings.write().await.lan_access_enabled = false;
        tokio::time::timeout(Duration::from_secs(10), start_http_server_on(&state, 0))
            .await
            .expect("second restart call must not deadlock")
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!state.remote_server.primary_is_lan());
        let bound = state.remote_server.bound_addresses();
        assert!(bound.iter().any(|address| address.ip().is_loopback()));
        assert!(bound.iter().all(|address| !address.ip().is_unspecified()));

        state.shutdown.cancel();
        state.server_tasks.close();
        state.server_tasks.wait().await;
        assert!(!state.remote_server.running());
    }

    /// Regression test: after a swap failure that leaves no primary bound
    /// (the double-failure path in `swap_primary`), the periodic refresh
    /// must rebuild through full startup rather than early-returning on
    /// `!running()`. Simulated here as a never-started server whose
    /// bookkeeping disagrees with the LAN setting.
    #[tokio::test]
    async fn lan_refresh_restarts_a_server_with_no_primary_bound() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        assert!(!state.remote_server.running());

        state.settings.write().await.lan_access_enabled = true;
        refresh_lan_listener(&state).await;
        // Give the accept loops a moment to settle on the new socket set.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(state.remote_server.running());
        assert!(state.remote_server.primary_is_lan());
        let bound = state.remote_server.bound_addresses();
        assert!(bound.iter().any(|address| address.ip().is_unspecified()));

        // A consistent state stays a no-op: no duplicate refresher, no rebind.
        refresh_lan_listener(&state).await;
        assert!(state.remote_server.running());

        state.shutdown.cancel();
        state.server_tasks.close();
        state.server_tasks.wait().await;
        assert!(!state.remote_server.running());
    }

    #[test]
    fn classifies_lan_addresses() {
        for address in [
            "192.168.1.10",
            "10.0.0.2",
            "172.16.9.9",
            "100.64.5.6",
            "169.254.10.20",
            "fd7a:115c:a1e0::123",
            "fe80::1",
        ] {
            assert!(
                is_lan_address(address.parse().unwrap()),
                "{address} should count as LAN"
            );
        }
        for address in ["8.8.8.8", "1.1.1.1", "127.0.0.1", "::1", "2001:db8::1"] {
            assert!(
                !is_lan_address(address.parse().unwrap()),
                "{address} should not count as LAN"
            );
        }
    }
}
