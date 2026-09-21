# Native Host and Viewer feature checklist

This checklist tracks replacement of the removed desktop web shell. Server keeps
its browser client; every checked desktop item must use Rust and Slint only.

## Library

- [x] Search, filters, sort, cursor paging, sources, groups, selection, ratings, tags, approval, grouping
- [x] Metadata refresh, deletion, local import, queue replacement and append
- [ ] Thumbnail grid/list switch, prior-page navigation, tag removal, rating undo, source/group editing
- [ ] Native reveal/open commands and keyboard navigation

## Player and sessions

- [x] Persistent queue, still-image preview, fullscreen, deterministic session controls, clip jobs
- [ ] libmpv video/audio/animated-media renderer; seek, speed, volume, loop, completed playback and remote stream bridge
- [ ] Slideshow, shuffle, feed, portrait wall, review and the full interactive/session effect set
- [ ] Native metronome, speech, beat maps, soundtrack connectors, session configuration and summaries

## Manage

- [x] Add/import sources, pause/resume/resync downloads, groups, native discovery and compatible-result queuing
- [x] Settings theme update, storage/statistics/remote diagnostics, backups and recovery jobs
- [ ] Source edit/delete/logs, tag administration, provider selection, exports/imports, full settings and storage cleanup
- [ ] Optional NudeNet/P-HAR setup, diagnostics log viewer, first-run setup and remote-listener controls

## Viewer and platform

- [x] Pinned Tailnet client, browser-free shared native screens, remote administration restrictions
- [ ] Saved-host management/migration, reconnect/address retries, remote playback protection
- [ ] Tray, single instance, startup integration, window position, desktop shortcuts and file associations

## Distribution and verification

- [x] Host does not serve browser assets; Server does
- [ ] Bundle libmpv, FFmpeg/ffprobe and gallery-dl; checksums and notices
- [ ] Native NSIS/MSI, Debian and AppImage release jobs; fresh-install and upgrade tests
- [ ] Full native UI, packaging, media and cross-platform smoke suite
