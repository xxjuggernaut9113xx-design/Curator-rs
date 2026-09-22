# Native Host and Viewer workflow inventory

## Reference and status rules

- **Reference:** `webmaster19083-tech/Curator-rs` at `3fc9aaca46ca0246e7a30f393f2dd799e82656f0` (pinned 2026-09-22). Browser behaviour is from `static/`; the API contract is `src/routes/mod.rs`.
- **Status:** `implemented` requires an appropriate native screen and typed service. `verified` also requires service/HTTP tests where applicable and explicit Windows/Linux manual evidence. `backend only`, `partial`, and `absent` are not complete.
- **Roles:** Host (H) has local library and Local Admin rights; Viewer (V) has only negotiated remote operations; Server (S) keeps the browser client. V must be denied Host-only work at UI and service boundaries.
- **Baseline:** routes are broad, but the Host is a list prototype, Viewer is a connection form, and `native::LocalClient` still adapts route handlers. No workflow is verified merely because an HTTP handler exists.

## Architecture gates

- [ ] **A-01 Typed service boundary** — Extract `library`, `settings`, `discovery`, `storage`, `downloads`, `jobs`, `backup`, `export`, `media`, and `playback` services; adapters retain every path, method and format. H/S full; V scoped. Status: partial. Session admission, media byte-range planning, typed download activity, and global/per-source download controls now have transport-independent service operations with direct/HTTP tests. Download control results still use compatibility JSON; the broader extraction and Viewer role negotiation remain open.
- [ ] **A-02 Operation control** — Separate interaction, jobs, thumbnails and playback; use bounded queues, cancellation, deduplication, progress and actionable errors. Status: partial. Native Host/Viewer now use bounded regular (128), control (16), and image (1) work queues; a full regular queue reports rejected actions and a stale preview result cannot replace the current preview. The control-lane unit test covers pause admission under a full regular queue. Seek, cancellation, navigation latency, quit latency, Windows manual, and Linux manual results remain unavailable.
- [ ] **A-03 Typed durable models** — Versioned settings, discovery capability, storage, activity, job, backup, export, media and playback models. Preserve unknown Viewer preferences. Status: partial (`db::Settings` coexists with ad-hoc JSON).
- [ ] **A-04 Lifecycle authority** — Service boundary owns permissions, maintenance lease, library lock and shutdown; listener failure remains visible while offline H works. Status: partial. Session start/control now share direct-service admission for maintenance and shutdown; direct and HTTP parity, shutdown denial, and direct maintenance denial have automated tests. Other operations, role permissions, occupied port, disconnect, and Windows/Linux manual results remain open.

## Shell and appearance

- [ ] **U-01 Native shell** — File/Tools menus; Library, Player, Manage; source/group tree/counts/Add source; location title, compact navigation and contextual actions. H/V as permitted. Status: partial. The shell has Library, Player, Manage and nested source/group navigation; media counts come from the shared library summary, with native/HTTP parity and nested ordering tested. Menus, contextual actions, location title, compact navigation, role negotiation, and Windows/Linux manual results remain open.
- [ ] **U-02 Manage destinations** — direct Discover, Organization, Activity and Settings navigation with retained state. H/V as permitted. Status: absent.
- [ ] **U-03 Settings dialog** — General, Media & Storage, Appearance, Playback & GOON, Automation, Local Admin; vertical navigation, independent scroll and fixed Save/Cancel. H full; V device preferences. Status: absent.
- [ ] **U-04 Theme/accessibility** — supported palettes as Slint tokens that update every component. Status: partial (value persists but components do not theme). Verify contrast, focus, font/window/display scaling on Windows/Linux.

## Library and editing

- [ ] **L-01 Browse/hierarchy** — non-overlapping grid and table/list, bounded thumbnail cache, stable headers, source/group navigation and reassignment. H/V read-only as negotiated. Status: partial. The native Library now renders separate adaptive grid and table layouts using the saved layout value; the table header remains outside its scroller. Thumbnail cards/cache, role negotiation, and Windows/Linux visual results remain open.
- [ ] **L-02 Query/continuity** — search, sort, media-type, tag, rating, review, size, creator, source and group filters; previous/next; preserve query, scroll and selection. Status: partial. Native Library keeps one Slint media model across selection updates and binds scroll position to window state across workspace switches. Missing filters, per-query scroll behavior, and Windows/Linux manual results remain open.
- [ ] **L-03 Inspect/review** — metadata, provenance, rating, approve, undo and review actions. H/V per permission. Status: partial.
- [ ] **L-04 Tags/organization** — media tag add/remove, inherited group tags, groups, source assignment, tag administration and source-tag review/rules. H full; V restricted. Status: partial.
- [ ] **L-05 Bulk/file actions** — accurate selection count and limits; refresh, local import, open/reveal, deletion confirmation and non-destructive state refresh. H full; V cannot open/reveal/import/local-delete. Status: partial. Viewer now hides file deletion and local metadata refresh, and its client denies both commands before any network request (automated denial test). Open/reveal, complete bulk UX, server-side role authority, and Windows/Linux manual results remain open.
- [ ] **L-06 Queue actions** — replace/append Player queue and atomically retain it. H/V playback-capable only. Status: partial.

## Player, sessions and modes

- [ ] **P-01 Renderer** — bundle libmpv, Slint OpenGL texture plus software fallback, efficient stills and safe teardown. H/V permitted stream only. Status: absent.
- [ ] **P-02 Controls** — video/audio/animated/still; pause, seek, duration, volume, speed, loop, fullscreen, loading/errors and workspace persistence. Status: partial (still preview/fullscreen only).
- [ ] **P-03 Queue/clips** — reorder/remove/advance/shuffle/repeat, slideshow and boundaries; clip edit/progress/cancel/error/result. Status: partial.
- [ ] **P-04 Remote streaming** — validated range seeking, reject redirects/nested playlists/unvalidated mpv URLs. Status: partial. Server now has an additive ID-based `/api/media/:id/stream` endpoint with single-range validation, 200/206/416 and HEAD behavior; Viewer still-image loads use its pinned Tailnet origin and ID. Parser and HTTP byte/header tests use a disposable library. Native libmpv use, playlist/redirect validation in the renderer, clip boundaries, role negotiation, and Windows/Linux manual results remain unavailable.
- [ ] **P-05 Session authority** — consume deterministic Rust session effects and persist monotonic session state. Status: partial; `session.rs`/`native.rs` tests exist, and Host now calls `services::session` directly while Server retains the same session routes and payloads through adapters. Direct/HTTP parity and shutdown/maintenance admission are tested. Native timing presentation and Windows/Linux manual results remain unavailable.
- [ ] **P-06 Modes/cues** — feed, portrait wall, review, slideshow, GOON, Cock Hero; native metronome/audio/speech, Linux speech engine, beats, soundtrack/connectors, effects, stale-cue prevention and multi-video decoder/focus policy. Status: absent.

## Manage

- [ ] **M-01 Discover** — provider capability/status/auth, query/result-type/sort/page/cancel/errors, selection/select-all/add/download. H/V permitted remote subset. Status: partial.
- [ ] **M-02 Organization/sources** — nested group CRUD, source edit/inspect/resync/pause/resume/remove/log and clear remove-vs-files distinction. H full; V restricted. Status: partial.
- [ ] **M-03 Activity** — active/queued/retrying totals, global/per-source control, current file/retry/errors and hidden/tray updates. Status: partial. Native Host reads typed `services::downloads::status` directly and exposes per-source pause/resume controls backed by the shared service; Viewer is denied those Host-only commands. Server adapters retain all download paths and payloads. Direct/native/HTTP status parity and global/per-source control parity, shutdown denial, maintenance denial, and global-pause precedence are tested with disposable libraries. Retry/error presentation, hidden/tray updates, and Windows/Linux manual results remain open.
- [ ] **M-04 Import/export** — source-list, metadata and package workflows with native dialogs, validation/conflicts/progress/cancel. H full; V explicit remote exports only. Status: backend only.

## Settings, admin and lifecycle

- [ ] **S-01 General** — concurrency, startup/tray, remote config, listening addresses/failure. H full; V preferences. Status: partial.
- [ ] **S-02 Media/Storage** — tool/download/media settings, summaries, cache and cleanups. H full; V read-only. Status: backend only.
- [ ] **S-03 Appearance/Playback/Automation** — all durable reference controls, previews and save/cancel semantics; opt-in classification, backup reminders and export. H/V appropriate fields. Status: partial.
- [ ] **S-04 Local Admin** — status/maintenance cards, selectable backup details, jobs, setup rerun, confirmation and restart signal. H/S only. Status: partial (manual backup ID is a defect).
- [ ] **S-05 Maintenance** — override clear; rating/evidence reset/requeue; group flatten/delete; tag/rule clear; session/CH history; thumbnail/cache rebuild; reconcile/backfill; staged reset; P-HAR removal; archive cleanup. Require backup, quiescence, exclusion, progress and recovery. Status: backend partial/native absent.
- [ ] **S-06 Classifier** — opt-in install/evaluate/configure/repair/cancel/self-test/status with platform/dependency/model/checksum/rights validation, honest blocked reasons and human/automatic provenance. H/S only. Status: backend partial/native absent.
- [ ] **D-01 Host lifecycle** — first run/reopen, dialogs, safe data-dir move, single instance, tray/startup/shortcuts/window state and graceful quit. Status: partial.
- [ ] **D-02 Viewer lifecycle** — saved-host list/edit/remove, unknown-field-preserving migration, stable identity, negotiation, validated retries/reconnect/switching. Status: partial.

## Compatibility and verification

- [ ] **C-01 Diagnostics** — rotating redacted logs, opt-in verbosity and visible locations. Status: partial.
- [ ] **C-02 Bundles** — libmpv for both editions; ffmpeg/ffprobe/isolated gallery-dl for H; install-relative resolution/overrides, pinned checksums/notices and packaging failure on missing tools. Status: absent.
- [ ] **C-03 Installers** — collision-free Windows NSIS/MSI and Linux Debian/AppImage with identity/scope/upgrade/uninstall-data guarantees. Status: partial (binaries only).
- [ ] **V-01 Evidence ledger** — each record requires direct-service/HTTP tests, H/V permission test and Windows/Linux manual result with disposable libraries. Existing tests cover backend/session/pagination/browser/package contracts; no native UI/install evidence exists.
- [ ] **V-02 Final gates** — fmt, locked check/test, strict Clippy, Server browser tests, native UI/package smokes, fresh/upgrade installers, offline/occupied-port/tray tests and browser-engine package/process audit.

## API, commands and history

The reference API covers system/OOBE; media, thumbnails, tags and source-tag rules; sources/groups/downloads; search; settings/remote/storage; import/export/chpack; stats/log; admin jobs/backups/P-HAR; session/GOON/beat maps; and Cock Hero playlist/session routes. Exact methods and paths remain in `src/routes/mod.rs`; migrations must retain them. Server CLI commands are `import-host` and `phar-intent`; former desktop bridge commands were path choosing, local import, media action, library summary and generic API request. Relevant history: `b6e98d71`, `e49cdfdf`, `e5301006`, `d4d1ca43`, `ff4ae654`, `3fc9aaca`, `45de18ef`, `1602d216`, `f74b14d9`.
