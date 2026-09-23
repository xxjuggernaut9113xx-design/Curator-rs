# Curator 0.3.3

Curator is a self-hosted gallery-dl library: download media you are entitled to
access, organize it with groups/tags/ratings, and browse it locally in a
browser or native desktop app. The backend serves only loopback and explicitly
detected Tailscale addresses—never ordinary LAN or wildcard interfaces.

Curator has three editions built from one Rust core:

- **Curator Server** (`curator`) is the headless backend, browser UI, download
  manager, media server, and background service.
- **Curator Host** (`Curator`) is the native Slint application with local
  library and session controls. It owns a library just like Server.
- **Curator Viewer** (`curator-viewer`) is a lightweight native Slint client. It
  starts no database or server and connects only to saved Tailnet hosts.

Host and Server can never open the same resolved data directory at once. An OS
lock is held for the complete backend lifetime, so an unsafe shared SQLite/WAL
setup fails before workers begin.

## Installation scope

Every product has a **Current user** choice (the default) and an **All users**
choice. App binaries follow the selected scope; Host and Viewer preferences
stay per-user in either case.

Current-user Server data lives in `%LocalAppData%\Curator` on Windows,
`~/.local/share/Curator` on typical Linux desktops. It runs as a Windows
scheduled task or `systemd --user` unit.

All-users Server data lives in `%ProgramData%\Curator` or `/var/lib/curator`.
It requires elevation and runs as a Windows service or systemd service. The package service
templates are in [packaging](packaging/README.md).

Windows Server releases contain separate current-user and all-users NSIS
installers. The Linux portable archive includes its non-elevated service
installer script; Linux `.deb` is the all-users Server package. Linux Host and Viewer releases also include
portable AppImages alongside their `.deb` packages for current-user use.

To bring a stopped Host library into a new all-users Server location, run the
elevated import command. It locks both locations, snapshots the source SQLite
database, copies library artifacts, and refuses to overwrite a non-empty
destination:

```text
curator import-host --from "C:\path\to\host-data" --install-scope all-users
```

## Viewer and Tailnet access

Start Server or Host on the machine that owns the library, install Tailscale on
both devices, and configure an owner-only Tailnet grant. In Viewer, add the
host URL, test it, then connect. Viewer verifies the host against the local
Tailscale peer inventory and resolved Tailnet IP before it accepts the
connection; it also checks `/api/system/info` for the Curator API protocol and
edition.

Viewer has ordinary library management access—browsing, playback, downloads,
sources, groups, tags, ratings, and routine settings—but never exposes local
Admin, OOBE, executable-path, service, or P-HAR installation controls.

## Local Admin and recovery

Host and local Server browser sessions expose **Local Admin**. Tailnet peers
receive a 403 for this surface. Admin serializes maintenance jobs, reports
progress, creates SQLite online backups, validates/downloads/restores backups,
rebuilds derived caches, reconciles the library, and provides explicitly
confirmed reset/cleanup tools.

Every destructive job requires its displayed typed phrase, pauses/quiesces
workers, creates a database/configuration backup, performs transactional data
changes, invalidates caches, and restores prior state on failure. Restore and
factory reset are staged for restart. Backups cover database/configuration,
not downloaded media; factory reset preserves media, archives, managed P-HAR
files, and backups unless their separate delete control is chosen.

## Diagnostics

Host shows the current diagnostic log path under Manage → Settings. Server
prints its diagnostic log directory at startup. Logs rotate daily and retain
eight files. Secret shaped values are redacted before file output; existing
logs are redacted again when read through the native view or `/api/log`.
Normal runs log at `info` level. Set `RUST_LOG=debug` before starting Host or
Server to opt into more detail. Viewer cannot open the Host diagnostic log
through its native interface.

## Appearance and layout

The app uses one primary scroller per view, an independent sidebar scroller,
and modal-body scrollers so long lists remain usable on short displays.
`100dvh`, bounded flex/grid sizing, sticky actions, keyboard focus, touch
scrolling, and horizontal-overflow checks are part of the shell contract.

Alongside Curator palettes, known GTK mappings are available for GTK System,
Adwaita, Yaru, Arc, and Breeze in light/dark variants. Host and Viewer can
inject their local GTK name, light/dark preference, accent, and font. Curator
maps those known families to accessible palettes; it does not parse arbitrary
GTK stylesheet files. Browser clients fall back to `prefers-color-scheme`.

## Optional classification and P-HAR

NudeNet is optional and can only suggest SFW, Slow, or Medium from anatomical
evidence. P-HAR is separately opt-in and can suggest Fast only when a
qualifying upstream action class appears in two consecutive temporal windows.
Kissing/fondling are insufficient; climax labels never assign Cum
automatically.

The managed P-HAR environment pins the upstream source archive beneath the
data directory. Native CUDA is preferred, with supported AMD ROCm used
when CUDA is unavailable or explicitly selected. Curator does not redistribute
or download model checkpoints until each checkpoint has a verified upstream
right, size, and SHA-256.
If setup is unavailable or fails, NudeNet/manual review remains operational
and P-HAR is not reported ready.

The Server NSIS installer and local OOBE both offer an unchecked opt-in choice;
Local Admin can enable, cancel, repair, self-test, or remove the managed
environment later. Setup reports persistent native-install stages and fails
closed if model publisher metadata or hardware compatibility cannot be
verified; NudeNet and manual review remain available.

## Building from source

```bash
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features -- -D warnings
node --test tests/*.test.js
```

Run `curator --docs` for the full operational reference. Desktop releases
target Windows and Linux.

Only add sources you have the right to access, and respect each source site's
terms and rate limits.
