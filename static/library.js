'use strict';

// The original app remains Curator's media and playback engine. This adapter
// replaces its surrounding surface with a single Explorer-style library so
// existing downloads, media URLs, lightbox, feed, and Tauri protocol paths
// remain intact.

const explorerLegacy = {
  buildTile,
  loadView,
  switchView,
  renderSidebar,
  populateTagFilterOptions,
  exitSlideshow,
};

const explorer = {
  installed: false,
  active: 'media',
  nav: 'all',
  selected: new Set(),
  selectionAnchor: null,
  searchQuery: '',
  searchTimer: null,
  panelRequest: 0,
  searchResults: [],
  searchSelected: new Set(),
  playMode: localStorage.getItem('curator-last-play-mode') || 'slideshow',
  layout: localStorage.getItem('curator-library-layout') || 'grid',
  sourceCollapsed: new Set(),
  activityTimer: null,
  goon: null,
  sizeBackfillRequest: false,
};

let settingsAdminTimer = null;

try {
  explorer.sourceCollapsed = new Set(JSON.parse(localStorage.getItem('curator-source-hierarchy-collapsed') || '[]'));
} catch (_) { explorer.sourceCollapsed = new Set(); }

function normalizePlayMode(mode) {
  return ({ 'mobile-feed': 'feed', 'portrait-wall': 'portrait' })[mode] || mode;
}

function supportsPlayMode(mode) {
  // Host is a native desktop app. Its panorama fallback is available without
  // an attached headset, so VR must remain a visible Play choice there.
  if (mode === 'vr' && curatorRuntime === 'host') return true;
  const narrowOrCoarse = window.matchMedia('(max-width: 900px), (pointer: coarse)').matches;
  const wideFine = window.matchMedia('(min-width: 901px) and (pointer: fine)').matches;
  if (mode === 'feed') return narrowOrCoarse;
  if (mode === 'portrait' || mode === 'vr') return wideFine;
  return true;
}
function setExplorerCommandContext(context) {
  document.body.dataset.commandContext = context;
}
function explorerCommandContext() {
  return ['search', 'sources', 'creators'].includes(explorer.nav) ? 'discover'
    : ['groups', 'tags', 'ratings', 'review'].includes(explorer.nav) ? 'organization'
      : ['downloads', 'recent'].includes(explorer.nav) ? 'activity' : 'library';
}
window.refreshExplorerCommandContext = () => setExplorerCommandContext(explorerCommandContext());
function hasNativeHostBridge() {
  return curatorRuntime === 'host' && typeof window.curatorNative?.importLocalFolder === 'function';
}
async function importNativeFolder() {
  if (!hasNativeHostBridge()) return;
  try {
    const sourceId = await window.curatorNative.importLocalFolder(null);
    if (sourceId == null) return;
    toast('Local folder added to Curator.');
    await refreshSources();
    await explorerLoadView();
  } catch (error) {
    toast(`Could not import that folder: ${error.message || error}`, true);
  }
}
function updatePlayCapabilities() {
  explorerAll('[data-play-mode="feed"]').forEach((button) => { button.hidden = !supportsPlayMode('feed'); });
  explorerAll('[data-play-mode="portrait"]').forEach((button) => { button.hidden = !supportsPlayMode('portrait'); });
  explorerAll('[data-play-mode="vr"]').forEach((button) => { button.hidden = !supportsPlayMode('vr'); });
  const vr = explorerEl('#vr-btn'); if (vr && !supportsPlayMode('vr')) vr.hidden = true;
  if (!supportsPlayMode(explorer.playMode)) setPlayMode('slideshow', false);
}

state.selectedMediaIds = explorer.selected;
state.explorerSection = 'media';

function explorerEl(selector, root = document) { return root.querySelector(selector); }
function explorerAll(selector, root = document) { return [...root.querySelectorAll(selector)]; }

function effectiveRating(item) {
  const value = item?.effective_rating ?? item?.human_rating ?? item?.rating ?? item?.auto_rating ?? 0;
  return Number.isFinite(Number(value)) ? Number(value) : 0;
}

function setExplorerLayout(layout, persist = true) {
  layout = layout === 'table' ? 'table' : 'grid';
  explorer.layout = layout;
  localStorage.setItem('curator-library-layout', layout);
  const grid = explorerEl('#grid'); const columns = explorerEl('#explorer-columns');
  grid?.classList.toggle('explorer-grid-layout', layout === 'grid');
  grid?.classList.toggle('explorer-table-layout', layout === 'table');
  columns?.classList.toggle('layout-hidden', layout === 'grid');
  explorerAll('[data-library-layout]').forEach((button) => {
    const active = button.dataset.libraryLayout === layout; button.classList.toggle('active', active); button.setAttribute('aria-pressed', String(active));
  });
  if (persist) api('/api/settings', { method: 'PATCH', body: JSON.stringify({ library_layout: layout }) }).catch(() => {});
}

function formatBytes(value) {
  if (value == null || value === '') return '—';
  const number = Number(value);
  if (!Number.isFinite(number) || number < 0) return '—';
  if (number < 1024) return `${number} B`;
  const unit = Math.min(4, Math.floor(Math.log(number) / Math.log(1024)));
  return `${(number / 1024 ** unit).toLocaleString(undefined, { maximumFractionDigits: 1 })} ${['B', 'KB', 'MB', 'GB', 'TB'][unit]}`;
}

function formatDuration(value) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds <= 0) return '—';
  const total = Math.round(seconds);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const remaining = total % 60;
  return hours ? `${hours}:${String(minutes).padStart(2, '0')}:${String(remaining).padStart(2, '0')}` : `${minutes}:${String(remaining).padStart(2, '0')}`;
}

function formatDate(value) {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value).slice(0, 10);
  return new Intl.DateTimeFormat(undefined, { year: 'numeric', month: 'short', day: 'numeric' }).format(date);
}

function sourceFor(item) { return state.sourcesById?.[item.source_id] || null; }
function sourceLabel(item) { return sourceFor(item)?.name || item.creator || item.source || '—'; }
function sourceHost(item) {
  const raw = item.origin_url || sourceFor(item)?.url || item.source_url || '';
  try { return new URL(raw).hostname.replace(/^www\./, ''); } catch (_) { return raw || '—'; }
}

function setExplorerVisible(mediaVisible) {
  const panel = explorerEl('#explorer-panel');
  const columns = explorerEl('#explorer-columns');
  const grid = explorerEl('#grid');
  const sentinel = explorerEl('#grid-sentinel');
  if (panel) panel.hidden = mediaVisible;
  if (columns) columns.hidden = !mediaVisible;
  if (grid) grid.hidden = !mediaVisible;
  if (sentinel) sentinel.hidden = !mediaVisible;
  if (!mediaVisible) explorerEl('#empty-state')?.setAttribute('hidden', '');
}

function updateExplorerLocation(label) {
  const node = explorerEl('#explorer-location');
  if (node) node.textContent = label;
  const category = ['search', 'sources', 'creators'].includes(explorer.nav) ? 'Discover'
    : ['groups', 'tags', 'ratings', 'review'].includes(explorer.nav) ? 'Organization'
      : ['downloads', 'recent'].includes(explorer.nav) ? 'Activity' : 'Library';
  const route = explorerEl('#explorer-route-label'); if (route) route.textContent = `CURATOR / ${category}`;
  const kicker = explorerEl('.explorer-kicker'); if (kicker) kicker.textContent = category;
}

function updateNavigation() {
  if (!explorer.installed) return;
  renderExplorerSourceHierarchy();
  explorerAll('[data-nav]').forEach((button) => button.classList.toggle('active', button.dataset.nav === explorer.nav));
  const category = explorerCommandContext();
  setExplorerCommandContext(category);
  explorerAll('[data-top-nav]').forEach((button) => button.classList.toggle('active', button.dataset.topNav === category));
  const activeDownloads = state.sources.filter((source) => source.status === 'pending' || source.status === 'downloading').length;
  const badge = explorerEl('#topnav-download-count');
  if (badge) {
    badge.hidden = activeDownloads === 0;
    badge.textContent = activeDownloads ? String(activeDownloads) : '';
  }
  const stats = explorerEl('#explorer-stats');
  if (stats) {
    const items = state.sources.reduce((sum, source) => sum + Number(source.item_count || 0), 0);
    stats.textContent = `${state.sources.length} sources · ${items.toLocaleString()} items`;
  }
}

async function refreshExplorerSizeBackfill() {
  const label = explorerEl('#explorer-size-backfill');
  if (!label || explorer.sizeBackfillRequest) return;
  explorer.sizeBackfillRequest = true;
  try {
    const summary = await api('/api/library/summary');
    const progress = summary.size_backfill;
    if (!progress) return;
    if (progress.running) {
      label.hidden = false;
      label.textContent = 'Calculating file sizes… ' + Number(progress.completed || 0).toLocaleString() + ' / ' + Number(progress.total || 0).toLocaleString();
    } else if (progress.error) {
      label.hidden = false;
      label.textContent = 'File-size backfill paused: ' + progress.error;
    } else {
      label.hidden = true;
    }
  } catch (_) {
    // Size totals are supplementary; media browsing must stay usable during a
    // transient diagnostics failure.
  } finally {
    explorer.sizeBackfillRequest = false;
  }
}

function saveSourceHierarchyCollapse() {
  localStorage.setItem('curator-source-hierarchy-collapsed', JSON.stringify([...explorer.sourceCollapsed]));
}
function sourceStatusText(source) {
  const status = String(source.status || 'idle').toLowerCase();
  return ({ downloading: 'active', pending: 'queued', retrying: 'retry', paused: 'paused', complete: 'done', completed: 'done', error: 'failed' })[status] || status;
}
function renderExplorerSourceHierarchy() {
  const host = explorerEl('#explorer-source-hierarchy'); if (!host) return;
  host.replaceChildren();
  const groupsByParent = new Map();
  state.groups.forEach((group) => {
    const key = group.parent_id == null ? 'root' : String(group.parent_id);
    if (!groupsByParent.has(key)) groupsByParent.set(key, []);
    groupsByParent.get(key).push(group);
  });
  const sourcesFor = (groupId) => state.sources.filter((source) => (source.group_id == null ? null : Number(source.group_id)) === groupId);
  const descendantSources = (group) => {
    const children = groupsByParent.get(String(group.id)) || [];
    return sourcesFor(group.id).length + children.reduce((count, child) => count + descendantSources(child), 0);
  };
  const appendSource = (source, depth) => {
    const row = document.createElement('div'); row.className = 'source-tree-source'; row.style.setProperty('--depth', depth);
    const browse = document.createElement('button'); browse.type = 'button'; browse.className = 'source-tree-browse'; browse.textContent = source.name;
    browse.title = source.url; browse.addEventListener('click', () => switchView({ type: 'creator', id: source.id }));
    const badge = document.createElement('span'); badge.className = `source-status source-status-${sourceStatusText(source)}`; badge.textContent = sourceStatusText(source);
    const actions = document.createElement('button'); actions.type = 'button'; actions.className = 'source-tree-actions'; actions.textContent = '⋯'; actions.setAttribute('aria-label', `Actions for ${source.name}`);
    actions.addEventListener('click', (event) => { event.stopPropagation(); openSourceMenu(source.id, actions); });
    row.append(browse, badge, actions); host.append(row);
  };
  const appendGroup = (group, depth) => {
    const key = `group:${group.id}`; const collapsed = explorer.sourceCollapsed.has(key);
    const children = groupsByParent.get(String(group.id)) || []; const direct = sourcesFor(group.id);
    const row = document.createElement('div'); row.className = 'source-tree-group'; row.style.setProperty('--depth', depth);
    const toggle = document.createElement('button'); toggle.type = 'button'; toggle.className = 'source-tree-toggle'; toggle.textContent = collapsed ? '›' : '⌄';
    toggle.setAttribute('aria-label', `${collapsed ? 'Expand' : 'Collapse'} ${group.name}`);
    toggle.addEventListener('click', () => { if (collapsed) explorer.sourceCollapsed.delete(key); else explorer.sourceCollapsed.add(key); saveSourceHierarchyCollapse(); renderExplorerSourceHierarchy(); });
    const browse = document.createElement('button'); browse.type = 'button'; browse.className = 'source-tree-browse'; browse.textContent = group.name;
    browse.addEventListener('click', () => switchView({ type: 'group', id: group.id, name: group.name }));
    const count = document.createElement('span'); count.className = 'source-tree-count'; count.textContent = `${descendantSources(group)} · ${children.length}`;
    row.append(toggle, browse, count); host.append(row);
    if (!collapsed) { children.forEach((child) => appendGroup(child, depth + 1)); direct.forEach((source) => appendSource(source, depth + 1)); }
  };
  const ungrouped = state.sources.filter((source) => source.group_id == null);
  const ungroupedKey = 'ungrouped'; const ungroupedCollapsed = explorer.sourceCollapsed.has(ungroupedKey);
  const ungroupedRow = document.createElement('div'); ungroupedRow.className = 'source-tree-group';
  const toggle = document.createElement('button'); toggle.type = 'button'; toggle.className = 'source-tree-toggle'; toggle.textContent = ungroupedCollapsed ? '›' : '⌄';
  toggle.addEventListener('click', () => { if (ungroupedCollapsed) explorer.sourceCollapsed.delete(ungroupedKey); else explorer.sourceCollapsed.add(ungroupedKey); saveSourceHierarchyCollapse(); renderExplorerSourceHierarchy(); });
  const browse = document.createElement('button'); browse.type = 'button'; browse.className = 'source-tree-browse'; browse.textContent = 'Ungrouped Sources'; browse.addEventListener('click', () => switchView({ type: 'group', id: 0, name: 'Ungrouped Sources' }));
  const count = document.createElement('span'); count.className = 'source-tree-count'; count.textContent = String(ungrouped.length);
  ungroupedRow.append(toggle, browse, count); host.append(ungroupedRow);
  if (!ungroupedCollapsed) ungrouped.forEach((source) => appendSource(source, 1));
  (groupsByParent.get('root') || []).forEach((group) => appendGroup(group, 0));
}

function updateBulkUI() {
  if (!explorer.installed) return;
  const count = explorer.selected.size;
  const bar = explorerEl('#explorer-bulk-bar');
  const label = explorerEl('#explorer-selection-count');
  if (bar) bar.hidden = count === 0;
  if (label) label.textContent = `${count} selected`;
  const selectAll = explorerEl('#explorer-select-all');
  if (selectAll) {
    const visibleIds = state.currentItems.map((item) => item.id);
    selectAll.checked = visibleIds.length > 0 && visibleIds.every((id) => explorer.selected.has(id));
    selectAll.indeterminate = !selectAll.checked && visibleIds.some((id) => explorer.selected.has(id));
  }
  explorerAll('.explorer-row').forEach((row) => {
    const selected = explorer.selected.has(Number(row.dataset.mediaId));
    row.classList.toggle('selected', selected);
    const checkbox = explorerEl('input[type="checkbox"]', row);
    if (checkbox) checkbox.checked = selected;
  });
}

function clearExplorerSelection() {
  explorer.selected.clear();
  explorer.selectionAnchor = null;
  updateBulkUI();
}

function selectExplorerItem(id, index, event) {
  const range = event?.shiftKey && explorer.selectionAnchor != null;
  const toggle = event?.ctrlKey || event?.metaKey;
  if (range) {
    const from = Math.min(explorer.selectionAnchor, index);
    const to = Math.max(explorer.selectionAnchor, index);
    if (!toggle) explorer.selected.clear();
    state.currentItems.slice(from, to + 1).forEach((item) => explorer.selected.add(item.id));
  } else if (toggle) {
    if (explorer.selected.has(id)) explorer.selected.delete(id);
    else explorer.selected.add(id);
    explorer.selectionAnchor = index;
  } else {
    explorer.selected.clear();
    explorer.selected.add(id);
    explorer.selectionAnchor = index;
  }
  updateBulkUI();
}

function selectAllVisible() {
  const items = state.currentItems;
  const allSelected = items.length > 0 && items.every((item) => explorer.selected.has(item.id));
  if (allSelected) explorer.selected.clear();
  else items.forEach((item) => explorer.selected.add(item.id));
  updateBulkUI();
}

function explorerBuildTile(item, index) {
  const row = document.createElement('article');
  const unavailable = mediaUnavailable(item);
  row.className = `explorer-row${item.type === 'video' ? ' explorer-row-video' : ''}${unavailable ? ' explorer-row-unavailable' : ''}`;
  row.dataset.mediaId = item.id;
  row.dataset.index = index;
  row.tabIndex = 0;
  row.setAttribute('role', 'row');
  row.setAttribute('aria-label', item.filename || `Media ${item.id}`);

  const select = document.createElement('input');
  select.type = 'checkbox';
  select.className = 'explorer-row-select';
  select.checked = explorer.selected.has(item.id);
  select.setAttribute('aria-label', `Select ${item.filename}`);
  select.addEventListener('click', (event) => event.stopPropagation());
  select.addEventListener('change', (event) => {
    if (event.target.checked) explorer.selected.add(item.id);
    else explorer.selected.delete(item.id);
    explorer.selectionAnchor = index;
    updateBulkUI();
  });
  row.append(select);

  const name = document.createElement('div');
  name.className = 'explorer-name';
  const preview = document.createElement(item.type === 'video' && !unavailable ? 'video' : 'img');
  preview.className = 'explorer-thumb';
  if (item.type === 'video' && !unavailable) {
    preview.muted = true;
    preview.preload = 'metadata';
    preview.dataset.src = mediaFullSrc(item);
    preview.setAttribute('aria-hidden', 'true');
    videoLazyObserver.observe(preview);
  } else {
    preview.src = mediaThumbSrc(item);
    preview.loading = 'lazy';
    preview.alt = '';
  }
  const nameText = document.createElement('span');
  nameText.className = 'explorer-name-text';
  nameText.textContent = item.filename || 'Untitled media';
  if (unavailable) nameText.title = item.skip_reason || 'Original unavailable; metadata remains.';
  name.append(preview, nameText);
  row.append(name);

  const creator = document.createElement('span');
  creator.className = 'explorer-creator'; creator.textContent = sourceLabel(item); creator.title = creator.textContent; row.append(creator);
  const source = document.createElement('span');
  source.className = 'explorer-source'; source.textContent = sourceHost(item); source.title = source.textContent; row.append(source);
  const duration = document.createElement('span'); duration.className = 'explorer-duration mono'; duration.textContent = formatDuration(item.duration_secs); row.append(duration);
  const size = document.createElement('span'); size.className = 'explorer-size mono'; size.textContent = formatBytes(item.file_size_bytes); row.append(size);
  const rating = document.createElement('span'); rating.className = 'explorer-rating mono';
  const value = effectiveRating(item); const paceLabels = { 1: 'SFW', 2: 'Slow', 3: 'Medium', 4: 'Fast', 5: 'Cum' };
  rating.textContent = value ? `★ ${value} · ${paceLabels[value] || 'Unrated'}` : '—';
  rating.title = item.human_rating != null ? `Human ${item.human_rating}; automatic ${item.auto_rating || 0}` : item.auto_rating ? `Automatic ${item.auto_rating}` : 'Unrated';
  if (item.human_rating != null || item.rating_reviewed) rating.classList.add('human'); row.append(rating);
  const date = document.createElement('span'); date.className = 'explorer-date mono'; date.textContent = formatDate(item.added_at); row.append(date);

  row.addEventListener('click', (event) => selectExplorerItem(item.id, index, event));
  row.addEventListener('dblclick', () => openLightbox(index));
  row.addEventListener('keydown', (event) => {
    if (event.key === ' ' || event.key === 'Spacebar') { event.preventDefault(); selectExplorerItem(item.id, index, event); }
    if (event.key === 'Enter') { event.preventDefault(); openLightbox(index); }
  });
  return row;
}

buildTile = explorerBuildTile;

function localMediaMatch(item, query) {
  const needle = query.trim().toLowerCase();
  if (!needle) return true;
  return [item.filename, sourceLabel(item), sourceHost(item), ...(item.tags || []), ...(item.inherited_tags || [])]
    .filter(Boolean)
    .some((value) => String(value).toLowerCase().includes(needle));
}

async function explorerLoadView() {
  if (explorer.active !== 'media') return renderExplorerPanel(explorer.active);
  setExplorerVisible(true);
  const result = await explorerLegacy.loadView();
  void refreshExplorerSizeBackfill();
  if (explorer.searchQuery.trim()) {
    state.currentItems = state.currentItems.filter((item) => localMediaMatch(item, explorer.searchQuery));
    state.renderedCount = 0;
    explorerEl('#grid').replaceChildren();
    toggleEmptyState(state.currentItems.length === 0);
    await renderNextPage(true);
  }
  updateBulkUI();
  return result;
}

loadView = explorerLoadView;

switchView = function explorerSwitchView(view) {
  explorer.active = 'media';
  state.explorerSection = 'media';
  if (view.type === 'creator') explorer.nav = 'creators';
  else if (view.type === 'group') explorer.nav = 'groups';
  updateNavigation();
  return explorerLegacy.switchView(view);
};

renderSidebar = function explorerRenderSidebar() {
  if (!explorer.installed) return;
  updateNavigation();
};

populateTagFilterOptions = function explorerPopulateTagOptions(tags) {
  explorerLegacy.populateTagFilterOptions(tags);
  const select = explorerEl('#explorer-tag-filter');
  if (!select) return;
  const previous = select.value;
  select.replaceChildren(new Option('All tags', ''));
  tags.forEach((tag) => select.add(new Option(`${tag.name} (${tag.media_count || 0})`, tag.name)));
  select.value = [...select.options].some((option) => option.value === previous) ? previous : state.tagFilter || '';
};

function mediaNavigation(section) {
  explorer.active = 'media'; state.explorerSection = 'media'; explorer.nav = section;
  state.view = { type: 'all' }; state.ratingStatus = ''; state.tagFilter = ''; state.maxRatingFilter = '';
  state.typeFilter = ({ all: 'all', images: 'image', clips: 'clip', videos: 'video' })[section] || 'all';
  if (section === 'recent') state.sortOrder = 'date_desc';
  explorer.searchQuery = '';
  const search = explorerEl('#explorer-library-search'); if (search) search.value = '';
  explorerAll('.explorer-type-filter').forEach((button) => button.classList.toggle('active', button.dataset.type === state.typeFilter));
  const sort = explorerEl('#explorer-sort'); if (sort) sort.value = state.sortOrder || 'default';
  const tag = explorerEl('#explorer-tag-filter'); if (tag) tag.value = '';
  const max = explorerEl('#explorer-max-rating'); if (max) max.value = '';
  const rating = explorerEl('#explorer-rating-status'); if (rating) rating.value = '';
  updateExplorerLocation(({ all: 'All Media', images: 'Images', clips: 'Clips', videos: 'Videos', recent: 'Recent' })[section] || 'Library');
  clearExplorerSelection(); updateNavigation(); return explorerLoadView();
}

function navigateTo(section) {
  if (['all', 'images', 'clips', 'videos', 'recent'].includes(section)) return mediaNavigation(section);
  if (section === 'review') {
    explorer.active = 'media'; state.explorerSection = 'media'; explorer.nav = 'review';
    state.view = { type: 'all' }; state.typeFilter = 'all'; state.ratingStatus = 'needs_review';
    updateExplorerLocation('Review Queue'); const rating = explorerEl('#explorer-rating-status'); if (rating) rating.value = 'needs_review';
    updateNavigation(); return explorerLoadView();
  }
  explorer.active = section; state.explorerSection = section; explorer.nav = section;
  clearExplorerSelection(); updateNavigation();
  updateExplorerLocation(({ search: 'Search', sources: 'Sources', creators: 'Creators', groups: 'Groups', tags: 'Tags', ratings: 'Ratings', downloads: 'Downloads' })[section] || 'Library');
  return renderExplorerPanel(section);
}

function makePanelHeading(title, description) {
  const heading = document.createElement('header'); heading.className = 'explorer-panel-heading';
  const h = document.createElement('h2'); h.textContent = title; heading.append(h);
  if (description) { const p = document.createElement('p'); p.textContent = description; heading.append(p); }
  return heading;
}

function panelButton(label, className = '') {
  const button = document.createElement('button');
  button.type = 'button'; button.className = `btn btn-ghost ${className}`.trim(); button.textContent = label;
  return button;
}

function showExplorerDialog(title, build) {
  const dialog = document.createElement('dialog'); dialog.className = 'explorer-dialog';
  const header = document.createElement('header'); const h = document.createElement('h2'); h.textContent = title;
  const close = panelButton('Close'); close.classList.add('explorer-dialog-close'); close.addEventListener('click', () => dialog.close());
  header.append(h, close); dialog.append(header); build(dialog); document.body.append(dialog);
  dialog.addEventListener('close', () => dialog.remove(), { once: true }); dialog.showModal(); return dialog;
}

async function chooseExplorerGroup(title = 'Choose a group') {
  return new Promise((resolve) => {
    const dialog = showExplorerDialog(title, (node) => {
      const list = document.createElement('div'); list.className = 'explorer-picker-list';
      const none = panelButton('No group'); none.addEventListener('click', () => { dialog.close(); resolve(null); }); list.append(none);
      state.groups.forEach((group) => {
        const button = panelButton(group.name); button.addEventListener('click', () => { dialog.close(); resolve(group.id); }); list.append(button);
      });
      const create = panelButton('+ New group'); create.addEventListener('click', async () => {
        const name = prompt('New group name:'); if (!name?.trim()) return;
        const created = await createGroup(name.trim()); if (created) { dialog.close(); resolve(created.id); }
      });
      node.append(list);
    });
    dialog.addEventListener('cancel', () => resolve(undefined), { once: true });
  });
}

async function doBulk(action, extra = {}) {
  const ids = [...explorer.selected]; if (!ids.length) return;
  try {
    await api('/api/media/bulk', { method: 'POST', body: JSON.stringify({ ids, action, ...extra }) });
    toast(`${ids.length} item${ids.length === 1 ? '' : 's'} updated`);
    await refreshGroups(); await refreshSources(); await refreshTagIndex(); await explorerLoadView(); clearExplorerSelection();
  } catch (error) {
    const selectedItems = state.currentItems.filter((item) => ids.includes(item.id));
    try {
      if (action === 'add_tag' && extra.tag) for (const item of selectedItems) await addTagToMedia(item, extra.tag);
      else if (action === 'set_rating' && Number.isInteger(extra.rating)) for (const item of selectedItems) await rateMedia(item, extra.rating);
      else throw error;
      toast(`${ids.length} item${ids.length === 1 ? '' : 's'} updated`); clearExplorerSelection();
    } catch (_) { toast(`Bulk action failed: ${error.message}`, true); }
  }
}

function selectedItems() { return state.currentItems.filter((item) => explorer.selected.has(item.id)); }

async function handleBulkAction(action) {
  if (action === 'clear') return clearExplorerSelection();
  if (action === 'add-group' || action === 'move') {
    const groupId = await chooseExplorerGroup(action === 'move' ? 'Move selected media' : 'Add selected media to a group');
    if (groupId !== undefined) await doBulk(action === 'move' ? 'move' : 'add_group', { group_id: groupId });
  } else if (action === 'add-tag') {
    const tag = prompt('Add tag to selected media:')?.trim(); if (tag) await doBulk('add_tag', { tag });
  } else if (action === 'set-rating') {
    const rating = Number(prompt('Set human rating (1–5):', '3'));
    if (Number.isInteger(rating) && rating >= 1 && rating <= 5) await doBulk('set_rating', { rating });
  } else if (action === 'delete') {
    if (confirm(`Delete ${explorer.selected.size} selected media item(s)? This can remove local files.`)) await doBulk('delete');
  } else if (action === 'review') launchPlayMode('review', selectedItems());
  else if (action === 'play') launchPlayMode(explorer.playMode, selectedItems());
  else if (action === 'refresh') await doBulk('refresh_metadata');
  else if (action === 'open-source') {
    const urls = [...new Set(selectedItems().map((item) => item.origin_url || sourceFor(item)?.url).filter(Boolean))];
    urls.slice(0, 8).forEach((url) => window.open(url, '_blank', 'noopener,noreferrer'));
  }
}

function launchSlideshowItems(items) {
  const playable = excludeSfwFromPlayback(items, 'Slideshow');
  if (!playable.length) { toast('Nothing eligible to show yet.', true); return; }
  closeLightbox(); ss.items = preparePlaybackItems(playable, false); ss.index = 0; ss.playing = true;
  ss.speed = Number(el('#ss-speed')?.value || appSettings.default_slideshow_speed || 3000);
  ss.loop = !!el('#ss-loop')?.checked; ss.shuffleMode = !!el('#ss-shuffle')?.checked;
  if (ss.shuffleMode && ss.items.length > 1) ss.items = preparePlaybackItems(ss.items, true);
  ss.active = true; el('#slideshow').hidden = false; renderSlide();
}

function setPlayMode(mode, persist = true) {
  const names = { feed: 'Mobile Feed', slideshow: 'Slideshow', portrait: 'Portrait Wall', review: 'Review', goon: 'GOON', vr: 'VR' };
  mode = normalizePlayMode(mode);
  explorer.playMode = names[mode] ? mode : 'slideshow'; localStorage.setItem('curator-last-play-mode', explorer.playMode);
  const primary = explorerEl('#explorer-play-primary'); if (primary) primary.textContent = `Play · ${names[explorer.playMode]}`;
  explorerAll('#explorer-play-menu [data-play-mode]').forEach((button) => {
    const current = button.dataset.playMode === explorer.playMode; button.classList.toggle('current', current); button.setAttribute('aria-checked', String(current));
  });
  if (persist) api('/api/settings', { method: 'PATCH', body: JSON.stringify({ last_play_mode: explorer.playMode }) }).catch(() => {});
}

async function restorePlayMode() {
  try {
    const settings = await api('/api/settings');
    const mode = normalizePlayMode(settings.last_play_mode);
    if (mode) setPlayMode(mode, false);
    if (settings.library_layout) setExplorerLayout(settings.library_layout, false);
  } catch (_) {
    // Local persistence is still useful when a remote server is reconnecting.
  }
}

function launchPlayMode(mode = explorer.playMode, items = null) {
  if (!supportsPlayMode(mode)) { toast(`${mode === 'feed' ? 'Mobile Feed' : mode === 'vr' ? 'VR' : 'Portrait Wall'} is unavailable on this device.`); return; }
  setPlayMode(mode); const list = items?.length ? items : state.currentItems;
  if (mode === 'feed') startFeed(false, items?.length ? items : null);
  else if (mode === 'slideshow') launchSlideshowItems(list);
  else if (mode === 'portrait') startPortraitWall(items?.length ? items : null);
  else if (mode === 'review') startFeed(true, items?.length ? items : null);
  else if (mode === 'goon') startGoonSession(list);
  else if (mode === 'vr') startVRMode();
}

// GOON is intentionally driven by one Web Audio timebase.  UI animation,
// stage transitions, visual swaps, metronome clicks and local speech derive
// from `AudioContext.currentTime`; no stage owns an independent timeout.
function goonCurrentBeat(session) {
  if (!session.running || !session.audio) return session.anchorBeat;
  const raw = session.anchorBeat + (session.audio.currentTime - session.anchorAt) * session.bpm / 60;
  return Math.max(0, Math.min(session.totalBeats, raw));
}
function goonRebase(session, beat = goonCurrentBeat(session)) {
  session.anchorBeat = Math.max(0, Math.min(session.totalBeats, beat));
  // The offset shifts the timeline origin in audio seconds. Keeping it in
  // the anchor (rather than layering another timer) makes seek, pause/resume,
  // and BPM edits use the same master clock.
  if (session.audio) session.anchorAt = session.audio.currentTime + session.beatOffsetSecs;
  session.nextScheduledBeat = Math.ceil(session.anchorBeat);
}
function goonRecordTimingCorrection(session, kind) {
  const corrections = session.timingCorrections || (session.timingCorrections = []);
  corrections.push({
    kind,
    beat: Math.round(goonCurrentBeat(session) * 1000) / 1000,
    bpm: session.bpm,
    beat_offset_secs: session.beatOffsetSecs,
    active_duration_s: Math.round(goonActiveDurationMs(session) / 10) / 100,
  });
  if (corrections.length > 128) corrections.splice(0, corrections.length - 128);
}
function goonActiveDurationMs(session) {
  return (session.activeDurationMs || 0) + (session.activeStartedAt == null ? 0 : Math.max(0, performance.now() - session.activeStartedAt));
}
function goonStopActiveDuration(session) {
  const duration = goonActiveDurationMs(session);
  session.activeDurationMs = duration;
  session.activeStartedAt = null;
  return duration;
}
function goonStageIndex(session, beat) {
  return session.stages.findIndex((stage) => beat >= Number(stage.start_beat) && beat < Number(stage.end_beat));
}
function goonClick(session, time, accented) {
  if (!session.metronome.enabled || !session.audio) return;
  const oscillator = session.audio.createOscillator();
  const gain = session.audio.createGain();
  oscillator.frequency.value = accented ? 1320 : 880;
  gain.gain.setValueAtTime(Math.max(0, Math.min(1, session.metronome.volume)) * (accented ? 0.20 : 0.12), time);
  gain.gain.exponentialRampToValueAtTime(0.001, time + 0.045);
  oscillator.connect(gain).connect(session.audio.destination);
  oscillator.start(time); oscillator.stop(time + 0.05);
}
function goonScheduleMetronome(session) {
  if (!session.running || !session.audio || session.audio.state !== 'running') return;
  const secondsPerBeat = 60 / session.bpm;
  const now = session.audio.currentTime;
  const horizon = now + 0.14;
  while (session.nextScheduledBeat <= session.totalBeats) {
    const time = session.anchorAt + (session.nextScheduledBeat - session.anchorBeat) * secondsPerBeat;
    if (time > horizon) break;
    if (time >= now - 0.03) goonClick(session, time, session.nextScheduledBeat % session.meter === 0);
    session.nextScheduledBeat += 1;
  }
  session.scheduler = window.setTimeout(() => goonScheduleMetronome(session), 25);
}
function goonSpeak(session, text) {
  if (!text || !session.running) return;
  if (!('speechSynthesis' in window)) { session.textOnly = true; return; }
  window.speechSynthesis.cancel();
  const utterance = new SpeechSynthesisUtterance(text);
  const voices = window.speechSynthesis.getVoices();
  const voiceName = appSettings.tts_voice || '';
  const voice = voices.find((candidate) => candidate.name === voiceName);
  if (voice) utterance.voice = voice;
  utterance.rate = Number(appSettings.tts_rate || 1);
  utterance.pitch = Number(appSettings.tts_pitch || 1);
  utterance.volume = Math.max(0, Math.min(1, Number(appSettings.tts_volume ?? 1)));
  window.speechSynthesis.speak(utterance);
}
function goonRenderBeatMap(session, beat) {
  const map = explorerEl('#goon-beat-map'); if (!map) return;
  const current = Math.floor(beat);
  explorerAll('.goon-beat', map).forEach((node) => {
    const value = Number(node.dataset.beat);
    node.classList.toggle('current', value === current);
    node.classList.toggle('past', value < current);
    node.classList.toggle('upcoming', value > current && value <= current + session.meter * 2);
  });
  const seek = explorerEl('#goon-seek'); if (seek && document.activeElement !== seek) seek.value = String(Math.round(beat));
}
function goonShowStageMedia(session, stage, force = false) {
  const mediaStage = explorerEl('#slideshow-stage');
  if (!stage?.media_rating) {
    clearAdvanceTimer(); detachVideoListeners(); ss.items = []; ss.playing = false;
    if (mediaStage) { mediaStage.replaceChildren(); mediaStage.classList.add('goon-no-media'); }
    return;
  }
  const media = Array.isArray(stage.media) ? stage.media : [];
  if (!media.length) {
    clearAdvanceTimer(); detachVideoListeners(); ss.items = []; ss.playing = false;
    if (mediaStage) { mediaStage.replaceChildren(); mediaStage.classList.add('goon-no-media'); }
    return;
  }
  mediaStage?.classList.remove('goon-no-media');
  const marker = `${stage.id}:${session.visualIndex}`;
  if (!force && session.visualMarker === marker) return;
  session.visualMarker = marker;
  const item = media[session.visualIndex % media.length];
  session.visualIndex += 1;
  ss.items = [item]; ss.index = 0; ss.active = true; ss.playing = false;
  renderSlide();
}
function goonApplyStage(session, index) {
  session.stageIndex = index;
  const stage = index >= 0 ? session.stages[index] : null;
  const title = explorerEl('#goon-stage-title');
  const prompt = explorerEl('#goon-stage-prompt');
  const intensity = explorerEl('#goon-intensity');
  if (!stage) {
    if (title) title.textContent = 'Count in';
    if (prompt) prompt.textContent = 'Follow the count before the first stage.';
    if (intensity) intensity.textContent = `Beat ${Math.floor(goonCurrentBeat(session)) + 1} / ${session.totalBeats}`;
    return;
  }
  if (title) title.textContent = stage.title || stage.pace || 'GOON';
  if (prompt) prompt.textContent = stage.prompt || '';
  if (intensity) intensity.textContent = stage.media_rating ? `${String(stage.pace || '').toUpperCase()} · ${stage.media_rating}★` : 'SUCCUBUS · beat map only';
  session.visualIndex = 0; session.visualMarker = null;
  goonShowStageMedia(session, stage, true);
  goonSpeak(session, stage.prompt || '');
  session.lastBeat = Math.floor(goonCurrentBeat(session));
}
function goonTick(session) {
  if (!session.running || explorer.goon !== session) return;
  const beat = goonCurrentBeat(session);
  if (beat >= session.totalBeats) { endGoonSession('completed'); return; }
  const whole = Math.floor(beat);
  const index = goonStageIndex(session, whole);
  if (index !== session.stageIndex) goonApplyStage(session, index);
  const stage = index >= 0 ? session.stages[index] : null;
  if (stage && whole !== session.lastBeat) {
    const markers = stage.visual_change_beats || [];
    if (markers.includes(whole)) goonShowStageMedia(session, stage);
  }
  session.lastBeat = whole;
  const elapsed = Math.max(0, beat * 60 / session.bpm);
  const time = explorerEl('#goon-stage-time'); if (time) time.textContent = `${Math.floor(elapsed / 60)}:${String(Math.floor(elapsed % 60)).padStart(2, '0')}`;
  goonRenderBeatMap(session, beat);
  session.raf = requestAnimationFrame(() => goonTick(session));
}
function goonBuildBeatMap(session) {
  const map = explorerEl('#goon-beat-map'); if (!map) return;
  map.replaceChildren();
  for (let beat = 0; beat <= session.totalBeats; beat++) {
    const node = document.createElement('span'); node.className = 'goon-beat'; node.dataset.beat = String(beat);
    if (beat % session.meter === 0) node.classList.add('accent');
    const stage = session.stages.find((entry) => Number(entry.start_beat) === beat || Number(entry.end_beat) === beat);
    if (stage) { node.classList.add('boundary'); node.title = stage.title || stage.pace; }
    if (session.stages.some((entry) => (entry.visual_change_beats || []).includes(beat))) node.classList.add('visual');
    map.append(node);
  }
}
function goonStartClock(session) {
  if (session.running) return;
  const AudioCtor = window.AudioContext || window.webkitAudioContext;
  if (!AudioCtor) { toast('This browser has no Web Audio clock; the beat map remains text-only.', true); return; }
  session.audio ||= new AudioCtor();
  session.audio.resume().then(() => {
    if (explorer.goon !== session || session.ended) return;
    session.running = true;
    session.startedAt ||= performance.now();
    session.activeStartedAt = performance.now();
    goonRebase(session, session.anchorBeat);
    explorerEl('#goon-start')?.setAttribute('hidden', '');
    const pause = explorerEl('#goon-pause'); if (pause) pause.textContent = 'Pause';
    goonScheduleMetronome(session); goonTick(session);
  }).catch(() => toast('Start was blocked by the browser. Tap Start again.', true));
}
function goonPauseClock(session) {
  if (!session?.audio) return;
  if (session.running) {
    goonRebase(session); goonStopActiveDuration(session); session.running = false; cancelAnimationFrame(session.raf); clearTimeout(session.scheduler);
    session.audio.suspend(); window.speechSynthesis?.cancel();
    const pause = explorerEl('#goon-pause'); if (pause) pause.textContent = 'Resume';
  } else goonStartClock(session);
}
function goonSetBpm(session, value, correctionKind = 'bpm') {
  const bpm = Number(value); if (!Number.isFinite(bpm) || bpm < 40 || bpm > 300) return;
  const beat = goonCurrentBeat(session); session.bpm = bpm; goonRebase(session, beat);
  goonRecordTimingCorrection(session, correctionKind);
  const readout = explorerEl('#goon-bpm'); if (readout) readout.value = String(Math.round(bpm * 10) / 10);
}
function goonTapTempo(session) {
  const now = performance.now(); session.taps = [...(session.taps || []), now].slice(-6);
  if (session.taps.length < 2) return;
  const intervals = session.taps.slice(1).map((time, index) => time - session.taps[index]).filter((value) => value >= 200 && value <= 1500);
  if (intervals.length) goonSetBpm(session, 60000 / (intervals.reduce((sum, value) => sum + value, 0) / intervals.length), 'tap_tempo');
}
async function startGoonSession(items) {
  const original = Array.isArray(items) ? items : [];
  const selected = excludeSfwFromPlayback(original, 'GOON');
  let payload;
  try { payload = await api('/api/goon/session', { method: 'POST', body: JSON.stringify({ media_ids: original.map((item) => item.id), persona: appSettings.goon_persona, metronome: { enabled: !!appSettings.metronome_enabled, volume: Number(appSettings.metronome_volume ?? .55) } }) }); }
  catch (error) { toast(`Could not plan GOON: ${error.message}`, true); return; }
  const stages = Array.isArray(payload?.stages) ? payload.stages : [];
  if (!stages.length) { toast('GOON needs a valid pace plan.', true); return; }
  if (!selected.length && Number(payload?.selection?.skipped_sfw_count || 0)) toast('All selected media are SFW; GOON will run its beat map without media.');
  closeLightbox(); ss.active = true; ss.playing = false; clearAdvanceTimer();
  explorerEl('#slideshow').hidden = false;
  const timeline = payload.timeline || {};
  const hud = explorerEl('#goon-hud'); hud.hidden = false;
  const session = explorer.goon = {
    payload, stages, bpm: Number(timeline.bpm || 120), meter: Number(timeline.meter || 4),
    totalBeats: Number(timeline.total_beats || stages.at(-1)?.end_beat || 4), anchorBeat: 0, anchorAt: 0,
    stageIndex: -2, lastBeat: -1, visualIndex: 0, visualMarker: null, running: false, ended: false,
    metronome: { enabled: !!payload.metronome?.enabled, volume: Number(payload.metronome?.volume ?? .55) },
    soundtrack: payload.soundtrack || {}, beatOffsetSecs: Number(timeline.beat_offset_secs || 0), taps: [], startedAt: null,
    activeDurationMs: 0, activeStartedAt: null, timingCorrections: [],
  };
  goonBuildBeatMap(session); goonApplyStage(session, -1); goonRenderBeatMap(session, 0);
  const bpm = explorerEl('#goon-bpm'); if (bpm) bpm.value = String(session.bpm);
  const offset = explorerEl('#goon-offset'); if (offset) offset.value = String(session.beatOffsetSecs);
  const metro = explorerEl('#goon-metronome'); if (metro) metro.checked = session.metronome.enabled;
  const metroVolume = explorerEl('#goon-metronome-volume'); if (metroVolume) metroVolume.value = String(session.metronome.volume);
  explorerEl('#goon-start')?.removeAttribute('hidden');
  const pause = explorerEl('#goon-pause'); if (pause) pause.textContent = 'Pause';
  const seek = explorerEl('#goon-seek'); if (seek) { seek.max = String(session.totalBeats); seek.value = '0'; }
  const meta = explorerEl('#goon-soundtrack'); if (meta) meta.textContent = `${session.soundtrack.provider || 'local'} · ${session.bpm} BPM${session.soundtrack.spotify_synchronization_disabled ? ' · visual sync disabled' : ''}`;
}
function endGoonSession(endedState = 'completed') {
  const session = explorer.goon; if (!session) return;
  const duration = Math.round(goonStopActiveDuration(session) / 1000);
  session.ended = true; session.running = false; cancelAnimationFrame(session.raf); clearTimeout(session.scheduler);
  session.audio?.suspend().catch(() => {}); window.speechSynthesis?.cancel();
  explorerEl('#goon-hud').hidden = true; explorer.goon = null;
  api('/api/goon/session/complete', { method: 'POST', body: JSON.stringify({ duration_s: duration, stages_completed: Math.max(0, session.stageIndex + 1), ended_state: endedState, soundtrack_provider: session.soundtrack.provider || 'local', bpm: session.bpm, beat_offset_secs: session.beatOffsetSecs, timing_corrections: session.timingCorrections, rating_phases: session.stages.map((stage) => stage.pace) }) }).catch(() => {});
  explorerLegacy.exitSlideshow();
}

exitSlideshow = function explorerExitSlideshow() {
  if (explorer.goon) return endGoonSession('cancelled');
  return explorerLegacy.exitSlideshow();
};

function normalizeSearchResult(result) {
  return {
    title: result.title || result.name || result.source_url || 'Untitled result', creator: result.creator || result.author || 'Unknown creator',
    thumbnail: result.thumbnail || result.thumbnail_url || '', source: result.source || result.provider || '', source_url: result.source_url || result.url || '',
    provider: result.provider || result.source || 'gallery-dl', result_type: result.result_type || result.type || 'Collection',
    item_count: result.item_count ?? result.count ?? null, date: result.date || result.published_at || '', gallery_dl_compatible: result.gallery_dl_compatible !== false,
    gallery_dl_validated: result.gallery_dl_validated === true,
  };
}

async function runUnifiedSearch() {
  const panel = explorerEl('#explorer-panel'); const query = explorerEl('#discover-query')?.value.trim() || ''; if (!query) return;
  const providers = explorerAll('#discover-providers input[type="checkbox"]:checked').map((input) => input.value);
  const resultType = explorerEl('#discover-result-type')?.value || ''; const sort = explorerEl('#discover-sort')?.value || 'relevance';
  const status = explorerEl('#discover-status'); if (status) status.textContent = 'Searching providers…';
  try {
    const params = new URLSearchParams({ query, sort }); if (providers.length) params.set('providers', providers.join(',')); if (resultType) params.set('result_type', resultType);
    const data = await api(`/api/search?${params}`); explorer.searchResults = (data.results || data.items || []).map(normalizeSearchResult); explorer.searchSelected.clear();
    if (status) {
      const unavailable = (data.provider_errors || []).map((entry) => entry.provider).filter(Boolean);
      status.textContent = `${explorer.searchResults.length} results${unavailable.length ? ` · ${unavailable.join(', ')} unavailable` : ''}`;
    }
    renderSearchResults(panel);
  } catch (error) { if (status) status.textContent = `Search unavailable: ${error.message}`; }
}

function renderSearchResults(panel) {
  explorerEl('#discover-results', panel)?.remove();
  const results = document.createElement('div'); results.id = 'discover-results'; results.className = 'discover-results';
  const tableHead = document.createElement('div'); tableHead.className = 'discover-result head'; tableHead.innerHTML = '<span></span><span>Result</span><span>Creator</span><span>Provider</span><span>Type</span><span>Date</span><span></span>'; results.append(tableHead);
  explorer.searchResults.forEach((result, index) => {
    const row = document.createElement('article'); row.className = 'discover-result';
    const check = document.createElement('input'); check.type = 'checkbox'; check.checked = explorer.searchSelected.has(index);
    check.addEventListener('change', () => { if (check.checked) explorer.searchSelected.add(index); else explorer.searchSelected.delete(index); updateSearchSelection(); });
    const resultCell = document.createElement('div'); resultCell.className = 'discover-result-title';
    if (result.thumbnail) { const image = document.createElement('img'); image.src = result.thumbnail; image.alt = ''; image.loading = 'lazy'; resultCell.append(image); }
    const text = document.createElement('span'); text.textContent = result.title; resultCell.append(text);
    const creator = document.createElement('span'); creator.textContent = result.creator;
    const provider = document.createElement('span'); provider.textContent = result.provider;
    const type = document.createElement('span'); type.textContent = result.result_type;
    const date = document.createElement('span'); date.textContent = formatDate(result.date);
    const actions = document.createElement('div'); actions.className = 'discover-row-actions';
    const preview = panelButton('Preview'); preview.addEventListener('click', () => previewSearchResult(result));
    const source = panelButton('Open'); source.addEventListener('click', () => result.source_url && window.open(result.source_url, '_blank', 'noopener,noreferrer'));
    actions.append(preview, source); row.append(check, resultCell, creator, provider, type, date, actions); results.append(row);
  });
  panel.append(results); updateSearchSelection();
}

function updateSearchSelection() {
  const count = explorerEl('#discover-selection-count'); if (count) count.textContent = `${explorer.searchSelected.size} selected`;
  const all = explorerEl('#discover-select-all');
  if (all) { all.checked = explorer.searchResults.length > 0 && explorer.searchSelected.size === explorer.searchResults.length; all.indeterminate = explorer.searchSelected.size > 0 && !all.checked; }
}

function previewSearchResult(result) {
  showExplorerDialog(result.title, (dialog) => {
    if (result.thumbnail) { const image = document.createElement('img'); image.src = result.thumbnail; image.alt = ''; image.className = 'search-preview-image'; dialog.append(image); }
    const info = document.createElement('dl'); info.className = 'search-preview';
    [['Creator', result.creator], ['Provider', result.provider], ['Type', result.result_type], ['Items', result.item_count ?? '—'], ['Date', formatDate(result.date)], ['Source', result.source_url]].forEach(([label, value]) => {
      const term = document.createElement('dt'); term.textContent = label; const description = document.createElement('dd'); description.textContent = String(value || '—'); info.append(term, description);
    }); dialog.append(info);
  });
}

async function addSearchResultsToCurator() {
  const chosen = [...explorer.searchSelected].map((index) => explorer.searchResults[index]).filter(Boolean); if (!chosen.length) return;
  const results = chosen.filter((result) => result.gallery_dl_compatible && result.gallery_dl_validated);
  if (results.length !== chosen.length) toast(`${chosen.length - results.length} CDN or unverified result${chosen.length - results.length === 1 ? '' : 's'} remain preview-only.`);
  if (!results.length) return;
  try { await api('/api/search/download', { method: 'POST', body: JSON.stringify({ results }) }); toast(`${results.length} result${results.length === 1 ? '' : 's'} added to Curator`); await refreshSources(); }
  catch (error) { toast(`Could not add search results: ${error.message}`, true); }
}

async function renderSearchPanel(panel) {
  panel.replaceChildren(makePanelHeading('Search', 'Search gallery-dl-compatible providers and external indexes in one place.'));
  const controls = document.createElement('form'); controls.className = 'discover-controls'; controls.noValidate = true;
  controls.innerHTML = '<input id="discover-query" type="search" placeholder="Search creators, galleries, posts, collections" autocomplete="off"><select id="discover-result-type"><option value="">All result types</option><option value="creator">Creator</option><option value="album">Album/Gallery</option><option value="post">Post</option><option value="collection">Collection</option></select><select id="discover-sort"><option value="relevance">Relevance</option><option value="date_desc">Newest</option><option value="date_asc">Oldest</option></select><button class="btn btn-accent" type="submit">Search</button>';
  controls.addEventListener('submit', (event) => { event.preventDefault(); runUnifiedSearch(); }); panel.append(controls);
  const providerPanel = document.createElement('details'); providerPanel.id = 'discover-providers'; providerPanel.className = 'discover-providers'; providerPanel.open = true;
  const summary = document.createElement('summary'); summary.textContent = 'Providers'; providerPanel.append(summary);
  const providerList = document.createElement('div'); providerList.className = 'discover-provider-list'; providerPanel.append(providerList); panel.append(providerPanel);
  let registry = []; let selected = new Set(['local', 'balbums', 'kemono', 'erome', 'redgifs', 'deviantart']);
  try {
    const [providerData, settings] = await Promise.all([api('/api/search/providers'), api('/api/settings')]);
    registry = providerData.providers || []; if (Array.isArray(settings.search_providers) && settings.search_providers.length) selected = new Set(settings.search_providers);
  } catch (_) {
    registry = [{ id: 'local', name: 'Curator library', availability: 'available' }, { id: 'balbums', name: 'Balbums / Bunkr', availability: 'available' }];
  }
  registry.forEach((provider) => {
    const label = document.createElement('label'); label.className = 'discover-provider';
    const check = document.createElement('input'); check.type = 'checkbox'; check.value = provider.id; check.checked = selected.has(provider.id); check.disabled = provider.availability === 'unavailable';
    check.addEventListener('change', () => { const values = explorerAll('#discover-providers input:checked').map((input) => input.value); api('/api/settings', { method: 'PATCH', body: JSON.stringify({ search_providers: values }) }).catch(() => {}); });
    const text = document.createElement('span'); text.textContent = provider.name || provider.id;
    const status = document.createElement('small');
    status.textContent = provider.authentication_required ? 'auth required'
      : (provider.capabilities || []).includes('search')
        ? (provider.availability || (provider.generated ? 'experimental' : 'available'))
        : 'direct URL only';
    label.append(check, text, status); providerList.append(label);
  });
  const bulk = document.createElement('div'); bulk.className = 'discover-bulk'; bulk.innerHTML = '<label><input id="discover-select-all" type="checkbox"> Select all</label><span id="discover-selection-count">0 selected</span>';
  const add = panelButton('Add to Curator'); add.addEventListener('click', addSearchResultsToCurator);
  const download = panelButton('Download Selected'); download.classList.add('btn-accent'); download.addEventListener('click', addSearchResultsToCurator);
  bulk.append(add, download); panel.append(bulk);
  explorerEl('#discover-select-all', bulk).addEventListener('change', (event) => { explorer.searchSelected.clear(); if (event.target.checked) explorer.searchResults.forEach((_, index) => explorer.searchSelected.add(index)); renderSearchResults(panel); });
  const status = document.createElement('p'); status.id = 'discover-status'; status.className = 'muted'; panel.append(status);
  if (explorer.searchResults.length) renderSearchResults(panel);
}

function sourceRow(source, creatorsOnly = false) {
  const row = document.createElement('article'); row.className = 'explorer-card source-card';
  const title = document.createElement('h3'); title.textContent = source.name;
  const meta = document.createElement('p'); meta.textContent = `${source.item_count || 0} items · ${source.status || 'ready'}${source.group_id ? ` · ${state.groupsById[source.group_id]?.name || 'Group'}` : ''}`;
  const url = document.createElement('p'); url.className = 'muted mono small'; url.textContent = source.url;
  const actions = document.createElement('div'); actions.className = 'explorer-card-actions';
  const browse = panelButton(creatorsOnly ? 'Open creator' : 'Browse'); browse.addEventListener('click', () => switchView({ type: 'creator', id: source.id }));
  const open = panelButton('Open source'); open.addEventListener('click', () => window.open(source.url, '_blank', 'noopener,noreferrer'));
  const sync = panelButton('Sync'); sync.addEventListener('click', () => resyncSource(source.id));
  actions.append(browse, open, sync); row.append(title, meta, url, actions); return row;
}

function renderSourcesPanel(panel, creatorsOnly) {
  panel.replaceChildren(makePanelHeading(creatorsOnly ? 'Creators' : 'Sources', creatorsOnly ? 'Creators and galleries already added to this Curator library.' : 'Every source uses Curator’s existing gallery-dl queue.'));
  const add = panelButton('+ Add source'); add.classList.add('btn-accent'); add.addEventListener('click', () => explorerEl('#add-source-btn')?.click()); panel.append(add);
  const list = document.createElement('div'); list.className = 'explorer-card-list'; state.sources.forEach((source) => list.append(sourceRow(source, creatorsOnly))); panel.append(list);
}

function assignSourcesToGroup(groupId) {
  return showExplorerDialog('Add sources to group', (node) => {
    const list = document.createElement('div'); list.className = 'explorer-picker-list';
    state.sources.filter((source) => source.group_id !== groupId).forEach((source) => {
      const button = panelButton(source.name); button.addEventListener('click', async () => {
        try { await api(`/api/sources/${source.id}/group`, { method: 'PATCH', body: JSON.stringify({ group_id: groupId }) }); await refreshSources(); await refreshGroups(); renderExplorerPanel('groups'); }
        catch (error) { toast(`Could not add source: ${error.message}`, true); }
      }); list.append(button);
    }); node.append(list);
  });
}

function renderGroupsPanel(panel) {
  panel.replaceChildren(makePanelHeading('Groups', 'Organize sources in nested groups. Group tags remain inherited by their media.'));
  const newGroup = panelButton('+ New group'); newGroup.classList.add('btn-accent'); newGroup.addEventListener('click', () => createGroup().then(() => renderExplorerPanel('groups'))); panel.append(newGroup);
  const list = document.createElement('div'); list.className = 'explorer-card-list group-card-list';
  state.groups.forEach((group) => {
    const card = document.createElement('article'); card.className = 'explorer-card group-card';
    const title = document.createElement('h3'); title.textContent = group.name;
    const meta = document.createElement('p'); meta.textContent = `${group.source_count || 0} direct sources${group.parent_id ? ` · in ${state.groupsById[group.parent_id]?.name || 'group'}` : ''}`;
    const tags = document.createElement('div'); tags.className = 'quick-tag-row';
    (group.tags || []).slice(0, 8).forEach((tag) => { const button = panelButton(tag, 'quick-tag'); button.addEventListener('click', () => { state.tagFilter = tag; mediaNavigation('all'); const select = explorerEl('#explorer-tag-filter'); if (select) select.value = tag; }); tags.append(button); });
    const actions = document.createElement('div'); actions.className = 'explorer-card-actions';
    const browse = panelButton('Browse'); browse.addEventListener('click', () => switchView({ type: 'group', id: group.id, name: group.name }));
    const add = panelButton('+ Add to group…'); add.addEventListener('click', () => assignSourcesToGroup(group.id));
    const tag = panelButton('+ Add tag'); tag.addEventListener('click', async () => { const name = prompt('Group tag:')?.trim(); if (name) { await addTagToGroup(group.id, name); renderExplorerPanel('groups'); } });
    actions.append(browse, add, tag); card.append(title, meta, tags, actions); list.append(card);
  }); panel.append(list);
}

async function reviewSourceTag(entry, action) {
  const normalizedName = action === 'edit' ? prompt('Normalize source tag:', entry.normalized_name || entry.raw_name || entry.name) : undefined;
  if (action === 'edit' && !normalizedName?.trim()) return;
  const remember = confirm('Remember this choice for this provider?');
  try { await api('/api/source-tags/review', { method: 'POST', body: JSON.stringify({ id: entry.id, action, normalized_name: normalizedName?.trim(), remember, scope: remember ? 'provider' : 'global' }) }); renderExplorerPanel('tags'); }
  catch (error) { toast(`Could not review source tag: ${error.message}`, true); }
}

async function renderTagsPanel(panel) {
  panel.replaceChildren(makePanelHeading('Tags', 'Quick filters use human tags first, then approved source tags and automatic suggestions.'));
  let tags = []; let quick = []; let review = [];
  try { tags = (await api('/api/tags')).tags || []; } catch (_) {}
  try { quick = (await api('/api/tags/quick')).tags || []; } catch (_) { quick = tags.slice(0, 16); }
  try { review = (await api('/api/source-tags/review?limit=30')).tags || []; } catch (_) {}
  const controls = document.createElement('div'); controls.className = 'tag-panel-controls';
  const add = panelButton('+ Add tag'); add.classList.add('btn-accent'); add.addEventListener('click', async () => { const tag = prompt('New tag:')?.trim(); if (!tag) return; if (explorer.selected.size) await doBulk('add_tag', { tag }); else toast('Select media first, then use Add tag.'); }); controls.append(add); panel.append(controls);
  const quickTitle = document.createElement('h3'); quickTitle.className = 'panel-subhead'; quickTitle.textContent = 'Common & recent'; panel.append(quickTitle);
  const quickRow = document.createElement('div'); quickRow.className = 'quick-tag-row';
  quick.forEach((tag) => { const name = typeof tag === 'string' ? tag : tag.name; const button = panelButton(name, 'quick-tag'); button.addEventListener('click', () => { state.tagFilter = name; mediaNavigation('all'); const filter = explorerEl('#explorer-tag-filter'); if (filter) filter.value = name; }); quickRow.append(button); }); panel.append(quickRow);
  if (review.length) {
    const heading = document.createElement('h3'); heading.className = 'panel-subhead'; heading.textContent = 'Source tag review'; panel.append(heading);
    const reviewList = document.createElement('div'); reviewList.className = 'source-tag-review-list';
    review.forEach((entry) => {
      const row = document.createElement('article'); row.className = 'source-tag-review'; const text = document.createElement('span'); text.textContent = `${entry.raw_name || entry.name} · ${entry.provider || 'source metadata'}`;
      const addButton = panelButton('Add'); addButton.addEventListener('click', () => reviewSourceTag(entry, 'add'));
      const editButton = panelButton('Edit'); editButton.addEventListener('click', () => reviewSourceTag(entry, 'edit'));
      const skipButton = panelButton('Skip'); skipButton.addEventListener('click', () => reviewSourceTag(entry, 'skip'));
      row.append(text, addButton, editButton, skipButton); reviewList.append(row);
    }); panel.append(reviewList);
  }
}

function renderRatingsPanel(panel) {
  panel.replaceChildren(makePanelHeading('Ratings', '1★ SFW · 2★ Slow · 3★ Medium · 4★ Fast · 5★ Cum. Human ratings override automated ratings wherever Curator sorts, filters, reviews, and plays media.'));
  const row = document.createElement('div'); row.className = 'rating-filter-row';
  const labels = { 1: 'SFW', 2: 'Slow', 3: 'Medium', 4: 'Fast', 5: 'Cum' };
  for (let value = 5; value >= 0; value--) {
    const button = panelButton(value ? `${'★'.repeat(value)} ${value} · ${labels[value]}` : 'Unrated', 'rating-filter');
    button.addEventListener('click', () => { explorer.active = 'media'; state.explorerSection = 'media'; explorer.nav = 'ratings'; state.view = { type: 'all' }; state.ratingStatus = value ? '' : 'unrated'; state.sortOrder = 'rating_desc'; state.maxRatingFilter = value ? String(value) : ''; updateExplorerLocation(value ? `${value}-star media` : 'Unrated media'); explorerLoadView(); }); row.append(button);
  } panel.append(row);
}

function stopActivityPolling() {
  if (explorer.activityTimer) { clearInterval(explorer.activityTimer); explorer.activityTimer = null; }
}

function stopAdminPolling() {
  if (settingsAdminTimer) { clearInterval(settingsAdminTimer); settingsAdminTimer = null; }
}
window.stopLocalAdminPolling = stopAdminPolling;

function adminActionButton(label, kind, confirmation, description) {
  const card = document.createElement('article'); card.className = 'explorer-card admin-action';
  const title = document.createElement('h3'); title.textContent = label;
  const detail = document.createElement('p'); detail.textContent = description;
  const button = panelButton(label); button.addEventListener('click', async () => {
    const typed = confirmation ? prompt(`Type ${confirmation} to continue:`) : '';
    if (confirmation && typed !== confirmation) { toast('Confirmation did not match.', true); return; }
    try {
      await api('/api/admin/jobs', { method: 'POST', body: JSON.stringify({ kind, confirmation: typed || '' }) });
      toast('Maintenance job queued.');
      const panel = explorerEl('#settings-local-admin'); if (panel) refreshAdminPanel(panel);
    } catch (error) { toast(`Could not start maintenance: ${error.message}`, true); }
  });
  card.append(title, detail, button); return card;
}

function renderAdminJobs(target, jobs) {
  target.replaceChildren();
  if (!jobs.length) { const empty = document.createElement('p'); empty.className = 'muted'; empty.textContent = 'No maintenance jobs yet.'; target.append(empty); return; }
  jobs.slice().reverse().forEach((job) => {
    const row = document.createElement('article'); row.className = `admin-job admin-job-${job.phase}`;
    const title = document.createElement('strong'); title.textContent = `${String(job.kind).replaceAll('_', ' ')} · ${job.phase}`;
    const detail = document.createElement('p'); detail.textContent = job.error || job.message || '';
    row.append(title, detail);
    if (job.backup_id) { const backup = document.createElement('small'); backup.className = 'mono muted'; backup.textContent = `Safety backup: ${job.backup_id}`; row.append(backup); }
    if (job.restart_required) { const restart = document.createElement('small'); restart.className = 'admin-restart'; restart.textContent = 'Restart Curator to apply this job.'; row.append(restart); }
    target.append(row);
  });
}

function renderAdminBackups(target, backups) {
  target.replaceChildren();
  if (!backups.length) { const empty = document.createElement('p'); empty.className = 'muted'; empty.textContent = 'No database/configuration backups have been created yet.'; target.append(empty); return; }
  backups.forEach((backup) => {
    const row = document.createElement('article'); row.className = 'admin-backup';
    const title = document.createElement('strong'); title.textContent = backup.id;
    const meta = document.createElement('small'); meta.className = 'mono muted'; meta.textContent = `${formatBytes(backup.size_bytes)} · ${formatDate(backup.created_at)}`;
    const actions = document.createElement('div'); actions.className = 'explorer-card-actions';
    const download = document.createElement('a'); download.className = 'btn btn-ghost'; download.textContent = 'Download'; download.href = `/api/admin/backups/${encodeURIComponent(backup.id)}`;
    const validate = panelButton('Validate'); validate.addEventListener('click', async () => {
      try { await api(`/api/admin/backups/${encodeURIComponent(backup.id)}/validate`, { method: 'POST' }); toast('Backup validation queued.'); }
      catch (error) { toast(`Could not validate backup: ${error.message}`, true); }
    });
    const restore = panelButton('Stage restore'); restore.addEventListener('click', async () => {
      const confirmation = prompt('Type RESTORE BACKUP to stage this restore:');
      if (confirmation !== 'RESTORE BACKUP') { toast('Confirmation did not match.', true); return; }
      try { await api(`/api/admin/backups/${encodeURIComponent(backup.id)}/restore`, { method: 'POST', body: JSON.stringify({ confirmation }) }); toast('Restore staged; restart after the job succeeds.'); }
      catch (error) { toast(`Could not stage restore: ${error.message}`, true); }
    });
    actions.append(download, validate, restore); row.append(title, meta, actions); target.append(row);
  });
}

function renderAdminPhar(target, phar) {
  target.replaceChildren();
  const heading = document.createElement('h3'); heading.className = 'panel-subhead'; heading.textContent = 'P-HAR'; target.append(heading);
  const detail = document.createElement('p'); detail.className = 'muted';
  const support = phar?.support || {};
  const accelerator = phar?.detected_gpu?.name ? ` / ${phar.detected_gpu.name}` : '';
  const selectedBackend = `${support.backend || phar?.backend || 'unknown'}${accelerator}`;
  detail.textContent = phar ? `${phar.phase.replaceAll('_', ' ')} · ${support.tier || 'unknown'} / ${selectedBackend} · ${phar.message}` : 'Loading P-HAR status…';
  target.append(detail);
  if (!phar) return;
  const actions = document.createElement('div'); actions.className = 'explorer-card-actions';
  const backend = document.createElement('select'); backend.className = 'panel-button'; backend.setAttribute('aria-label', 'P-HAR native backend');
  for (const value of ['auto', 'cuda', 'rocm']) { const option = document.createElement('option'); option.value = value; option.textContent = value === 'auto' ? 'Auto (CUDA first)' : value.toUpperCase(); option.selected = (phar.backend || 'auto') === value; backend.append(option); }
  const refresh = () => { const panel = target.closest('.settings-local-admin'); if (panel) refreshAdminPanel(panel); };
  const intent = panelButton(phar.requested ? 'Disable P-HAR' : 'Enable P-HAR setup');
  intent.addEventListener('click', async () => {
    try { await api('/api/admin/phar', { method: 'POST', body: JSON.stringify({ enabled: !phar.requested, backend: backend.value }) }); refresh(); }
    catch (error) { toast(`Could not update P-HAR: ${error.message}`, true); }
  });
  const install = panelButton('Install / resume'); install.disabled = !phar.requested;
  install.addEventListener('click', async () => {
    try { const result = await api('/api/admin/phar/install', { method: 'POST' }); toast(result.message || 'P-HAR setup evaluated.'); refresh(); }
    catch (error) { toast(`Could not evaluate P-HAR: ${error.message}`, true); }
  });
  const cancel = panelButton('Cancel'); cancel.disabled = !phar.requested;
  cancel.addEventListener('click', async () => {
    try { await api('/api/admin/phar/cancel', { method: 'POST' }); refresh(); }
    catch (error) { toast(`Could not cancel P-HAR setup: ${error.message}`, true); }
  });
  const repair = panelButton('Repair'); repair.disabled = !phar.requested;
  repair.addEventListener('click', async () => {
    try { const result = await api('/api/admin/phar/repair', { method: 'POST' }); toast(result.message || 'P-HAR repair evaluated.'); refresh(); }
    catch (error) { toast(`Could not repair P-HAR: ${error.message}`, true); }
  });
  const selfTest = panelButton('Self-test'); selfTest.disabled = !phar.requested;
  selfTest.addEventListener('click', async () => {
    try { const result = await api('/api/admin/phar/self-test', { method: 'POST' }); toast(result.message || 'P-HAR self-test finished.'); refresh(); }
    catch (error) { toast(`P-HAR self-test failed: ${error.message}`, true); }
  });
  actions.append(backend, intent, install, cancel, repair, selfTest); target.append(actions);
}

async function refreshAdminPanel(panel) {
  if (!window.isSettingsTabActive?.('local-admin') || document.hidden || !panel.isConnected) return;
  try {
    const [backups, jobs, phar] = await Promise.all([api('/api/admin/backups'), api('/api/admin/jobs'), api('/api/admin/phar')]);
    renderAdminPhar(explorerEl('#admin-phar', panel), phar);
    renderAdminBackups(explorerEl('#admin-backup-list', panel), backups.backups || []);
    renderAdminJobs(explorerEl('#admin-job-list', panel), jobs.jobs || []);
  } catch (error) {
    const status = explorerEl('#admin-status', panel); if (status) status.textContent = `Admin unavailable: ${error.message}`;
    stopAdminPolling();
  }
}

function renderAdminPanel(panel) {
  stopAdminPolling();
  panel.replaceChildren(makePanelHeading('Local Admin', 'Recovery and maintenance are available only on this device. Each destructive job first creates a database/configuration backup, pauses workers, and records progress below.'));
  const status = document.createElement('p'); status.id = 'admin-status'; status.className = 'muted'; panel.append(status);
  const phar = document.createElement('section'); phar.id = 'admin-phar'; phar.className = 'admin-phar'; panel.append(phar);
  const backupActions = document.createElement('div'); backupActions.className = 'explorer-card-actions';
  const create = panelButton('Create backup'); create.classList.add('btn-accent'); create.addEventListener('click', async () => {
    try { await api('/api/admin/backups', { method: 'POST' }); toast('Backup job queued.'); refreshAdminPanel(panel); }
    catch (error) { toast(`Could not create backup: ${error.message}`, true); }
  }); backupActions.append(create); panel.append(backupActions);
  const backupHeading = document.createElement('h3'); backupHeading.className = 'panel-subhead'; backupHeading.textContent = 'Backups'; panel.append(backupHeading);
  const backupList = document.createElement('div'); backupList.id = 'admin-backup-list'; backupList.className = 'admin-list'; panel.append(backupList);
  const actionsHeading = document.createElement('h3'); actionsHeading.className = 'panel-subhead'; actionsHeading.textContent = 'Maintenance'; panel.append(actionsHeading);
  const actions = document.createElement('div'); actions.className = 'explorer-card-list admin-actions';
  [
    ['Clear human rating overrides', 'clear_human_ratings', 'CLEAR HUMAN RATINGS', 'Return effective ratings to retained automatic recommendations.'],
    ['Reset ratings and classifier evidence', 'reset_ratings_and_evidence', 'RESET RATINGS', 'Clear all ratings/evidence and requeue eligible image analysis.'],
    ['Flatten group nesting', 'flatten_groups', 'FLATTEN GROUPS', 'Make every group top-level without changing assignments.'],
    ['Delete groups and assignments', 'delete_groups', 'DELETE GROUPS', 'Remove groups only; sources, media, and tags stay intact.'],
    ['Clear tag assignments and rules', 'clear_tag_assignments', 'CLEAR TAG ASSIGNMENTS', 'Keep tag names but remove media/group assignments and source-tag rules.'],
    ['Clear interactive-session history', 'clear_interactive_history', 'CLEAR SESSION HISTORY', 'Delete logged interactive and Cock Hero sessions.'],
    ['Rebuild thumbnails and caches', 'rebuild_caches', 'REBUILD CACHES', 'Clear derived cache files; originals are untouched.'],
    ['Reconcile library metadata', 'reconcile_library', '', 'Check the media library and restart file-size backfill.'],
    ['Factory reset database/settings', 'factory_reset', 'RESET CURATOR', 'Stages reset for restart; media, archives, P-HAR, and backups remain.'],
    ['Delete P-HAR environment', 'remove_phar_environment', 'DELETE P-HAR', 'Remove the managed P-HAR files and disable its setup request.'],
    ['Delete archives', 'remove_archives', 'DELETE ARCHIVES', 'Remove downloaded archive files; media remains. gallery-dl may reconsider older posts on a later sync.'],
  ].forEach(([label, kind, confirmation, description]) => actions.append(adminActionButton(label, kind, confirmation, description)));
  panel.append(actions);
  const jobsHeading = document.createElement('h3'); jobsHeading.className = 'panel-subhead'; jobsHeading.textContent = 'Jobs'; panel.append(jobsHeading);
  const jobs = document.createElement('div'); jobs.id = 'admin-job-list'; jobs.className = 'admin-list'; panel.append(jobs);
  refreshAdminPanel(panel);
  settingsAdminTimer = setInterval(() => refreshAdminPanel(panel), 1200);
}
window.renderSettingsLocalAdmin = renderAdminPanel;
async function refreshActivityPanel(panel) {
  if (explorer.active !== 'downloads' || document.hidden || !panel.isConnected) return;
  const status = explorerEl('#activity-status', panel); const list = explorerEl('#activity-source-list', panel);
  try {
    const data = await api('/api/downloads/status');
    if (explorer.active !== 'downloads' || !panel.isConnected) return;
    if (status) status.textContent = data.paused ? `Paused · ${data.paused_source_ids?.length || 0} source(s) ready to resume` : `${data.active_count || 0} active · ${data.queued_count || 0} queued · ${data.retrying_count || 0} retrying`;
    if (!list) return; list.replaceChildren();
    (data.sources || []).forEach((source) => {
      const card = document.createElement('article'); card.className = `activity-source activity-${source.phase}`;
      const heading = document.createElement('header'); const name = document.createElement('strong'); name.textContent = source.name;
      const phase = document.createElement('span'); phase.className = 'source-status'; phase.textContent = source.phase; heading.append(name, phase); card.append(heading);
      const progress = document.createElement('p'); progress.className = 'mono small';
      const total = source.known_total == null ? 'indeterminate total' : `${source.completed_count || 0} / ${source.known_total} (${Math.round(Number(source.percentage || 0))}%)`;
      progress.textContent = `${total}${source.queue_position ? ` · queue #${source.queue_position}` : ''}${source.current_filename ? ` · ${source.current_filename}` : ''}`; card.append(progress);
      if (source.known_total != null) { const bar = document.createElement('div'); bar.className = 'activity-progress'; const fill = document.createElement('span'); fill.style.width = `${Math.max(0, Math.min(100, Number(source.percentage || 0)))}%`; bar.append(fill); card.append(bar); }
      if (source.error || source.retry_at) { const detail = document.createElement('small'); detail.className = 'muted'; detail.textContent = source.error || `Retry scheduled at ${new Date(Number(source.retry_at) * 1000).toLocaleTimeString()}`; card.append(detail); }
      const actions = document.createElement('div'); actions.className = 'explorer-card-actions';
      const pause = panelButton(source.phase === 'paused' ? 'Resume' : 'Pause');
      pause.addEventListener('click', async () => { const endpoint = source.phase === 'paused' ? 'resume' : 'pause'; await api(`/api/downloads/sources/${source.id}/${endpoint}`, { method: 'POST' }); refreshActivityPanel(panel); });
      const sync = panelButton('Sync'); sync.addEventListener('click', () => resyncSource(source.id)); actions.append(pause, sync); card.append(actions); list.append(card);
    });
  } catch (error) { if (status) status.textContent = `Status unavailable: ${error.message}`; }
}
async function renderDownloadsPanel(panel) {
  stopActivityPolling();
  panel.replaceChildren(makePanelHeading('Activity', 'Source-level queue and indexing progress. Curator only polls while this view is visible.'));
  const status = document.createElement('p'); status.id = 'activity-status'; status.className = 'downloads-status'; status.textContent = 'Loading status…'; panel.append(status);
  const actions = document.createElement('div'); actions.className = 'explorer-card-actions';
  const pause = panelButton('Pause downloads'); const resume = panelButton('Resume downloads'); const resync = panelButton('Sync all sources');
  pause.addEventListener('click', async () => { await api('/api/downloads/pause', { method: 'POST' }); refreshActivityPanel(panel); });
  resume.addEventListener('click', async () => { await api('/api/downloads/resume', { method: 'POST' }); refreshActivityPanel(panel); });
  resync.addEventListener('click', () => resyncAllSources()); actions.append(pause, resume, resync); panel.append(actions);
  const list = document.createElement('div'); list.id = 'activity-source-list'; list.className = 'activity-source-list'; panel.append(list);
  await refreshActivityPanel(panel);
  explorer.activityTimer = setInterval(() => refreshActivityPanel(panel), 2000);
}

async function renderExplorerPanel(section) {
  if (!explorer.installed) return;
  if (section !== 'downloads') stopActivityPolling();
  if (section === 'media') return explorerLoadView();
  const request = ++explorer.panelRequest; setExplorerVisible(false);
  const panel = explorerEl('#explorer-panel'); if (!panel) return; panel.replaceChildren();
  if (section === 'search') await renderSearchPanel(panel);
  else if (section === 'sources') renderSourcesPanel(panel, false);
  else if (section === 'creators') renderSourcesPanel(panel, true);
  else if (section === 'groups') renderGroupsPanel(panel);
  else if (section === 'tags') await renderTagsPanel(panel);
  else if (section === 'ratings') renderRatingsPanel(panel);
  else if (section === 'downloads') await renderDownloadsPanel(panel);
  if (request !== explorer.panelRequest) return;
}

function installExplorerUi() {
  const sidebar = explorerEl('.sidebar'); const main = explorerEl('.main'); const legacyToolbar = explorerEl('.toolbar', main);
  if (!sidebar || !main || !legacyToolbar || explorer.installed) return;
  explorer.installed = true;

  // Keep the compatibility markup available for app.js, but make the
  // Explorer shell authoritative before moving any of the legacy controls.
  // This prevents the old gallery from flashing or sitting underneath the
  // new layout during startup.
  explorerEl('.app-shell')?.classList.add('explorer-shell');
  document.body.classList.add('explorer-ready');

  // Preserve old controls and their event listeners as compatibility hooks.
  const legacySidebar = document.createElement('div'); legacySidebar.className = 'legacy-sidebar'; legacySidebar.hidden = true;
  while (sidebar.firstChild) legacySidebar.append(sidebar.firstChild); sidebar.append(legacySidebar); sidebar.classList.add('explorer-sidebar');
  const navigation = document.createElement('div'); navigation.className = 'explorer-sidebar-content';
  navigation.innerHTML = '<header class="explorer-brand"><span class="brand-mark">C</span><span>CURATOR</span><button type="button" class="explorer-sidebar-close" aria-label="Close navigation">×</button></header><button id="explorer-add-source" class="explorer-add-source" type="button">+ Add source</button><nav class="explorer-navigation" aria-label="Curator navigation"><section><h2>Library</h2><button data-nav="all" type="button">All Media</button><button data-nav="images" type="button">Images</button><button data-nav="clips" type="button">Clips</button><button data-nav="videos" type="button">Videos</button></section><section><h2>Discover</h2><button data-nav="search" type="button">Search</button><button data-nav="sources" type="button">Sources</button><button data-nav="creators" type="button">Creators</button></section><section><h2>Organization</h2><button data-nav="groups" type="button">Groups</button><button data-nav="tags" type="button">Tags</button><button data-nav="ratings" type="button">Ratings</button><button data-nav="review" type="button">Review Queue</button></section><section><h2>Activity</h2><button data-nav="downloads" type="button">Downloads <span id="sidebar-download-count" class="nav-count" hidden></span></button><button data-nav="recent" type="button">Recent</button></section></nav><footer><button id="explorer-settings" type="button">Settings</button><span id="explorer-stats" class="mono small muted"></span></footer>';
  // The sidebar is intentionally source-centric.  Navigation and global
  // actions live in the compact Tools disclosure, leaving room for the
  // expandable groups → subgroups → sources hierarchy below it.
  const brandRoute = explorerEl('.explorer-brand > span:nth-child(2)', navigation); if (brandRoute) { brandRoute.id = 'explorer-route-label'; brandRoute.textContent = 'CURATOR / Library'; }
  const tools = explorerEl('.explorer-navigation', navigation);
  tools.innerHTML = '<details class="explorer-tools" open><summary>Tools</summary><section><h2>Library</h2><button data-nav="all" type="button">Library</button></section><section><h2>Discover</h2><button data-nav="search" type="button">Discover</button><button data-nav="sources" type="button">Sources</button><button data-nav="creators" type="button">Creators</button></section><section><h2>Organization</h2><button data-nav="groups" type="button">Groups</button><button data-nav="tags" type="button">Tags</button><button data-nav="ratings" type="button">Ratings</button><button data-nav="review" type="button">Review</button></section><section><h2>Activity</h2><button data-nav="downloads" type="button">Activity <span id="sidebar-download-count" class="nav-count" hidden></span></button><button data-nav="recent" type="button">Recent</button></section><section class="explorer-tool-actions"><h2>Actions</h2><button id="explorer-add-source" type="button">Add Source</button><button id="explorer-resync-all" type="button">Sync all</button><button id="explorer-pause-downloads" type="button">Pause / resume</button><button id="explorer-settings" type="button">Settings</button><button id="explorer-export" type="button">Export</button><button id="explorer-import" type="button">Import</button><a href="/api/log" target="_blank" rel="noopener noreferrer">View log</a></section></details><section class="explorer-source-tree"><h2>Source hierarchy</h2><div id="explorer-source-hierarchy"></div></section>';
  // Primary navigation belongs at the top of the main window. Keep only the
  // hierarchy section from the compatibility container so the sidebar stays
  // dedicated to groups and sources.
  const sourceTree = explorerEl('.explorer-source-tree', tools);
  tools.replaceWith(sourceTree);
  explorerEl('.explorer-sidebar-content > footer', navigation).hidden = true;
  sidebar.prepend(navigation);
  explorerAll('[data-nav]', navigation).forEach((button) => button.addEventListener('click', () => navigateTo(button.dataset.nav)));
  explorerEl('#explorer-add-source', navigation).addEventListener('click', () => explorerEl('#add-source-btn')?.click());
  explorerEl('#explorer-settings', navigation).addEventListener('click', openSettingsModal);
  explorerEl('.explorer-sidebar-close', navigation).addEventListener('click', closeSidebarDrawer);

  legacyToolbar.hidden = true; legacyToolbar.classList.add('legacy-toolbar');
  const toolbar = document.createElement('header'); toolbar.className = 'explorer-toolbar';
  toolbar.innerHTML = '<div class="explorer-toolbar-top"><div><p class="explorer-kicker">Library</p><h1 id="explorer-location">All Media</h1></div><div class="explorer-toolbar-actions"><button id="explorer-add-source-main" class="btn btn-ghost" type="button">+ Add source</button><label class="explorer-search"><span class="sr-only">Search library</span><input id="explorer-library-search" type="search" placeholder="Search library" autocomplete="off"></label><div class="explorer-play-split"><button id="explorer-play-primary" class="btn btn-accent" type="button">Play</button><button id="explorer-play-toggle" class="btn btn-accent" type="button" aria-label="Choose play mode" aria-haspopup="menu" aria-expanded="false">▾</button><div id="explorer-play-menu" role="menu" hidden><button type="button" data-play-mode="feed">Mobile Feed</button><button type="button" data-play-mode="slideshow">Slideshow</button><button type="button" data-play-mode="portrait">Portrait Wall</button><button type="button" data-play-mode="review">Review</button><button type="button" data-play-mode="goon">GOON</button></div></div></div></div><div class="explorer-toolbar-filters"><div class="explorer-type-buttons"><button type="button" data-type="all" class="explorer-type-filter active">All</button><button type="button" data-type="image" class="explorer-type-filter">Images</button><button type="button" data-type="clip" class="explorer-type-filter">Clips</button><button type="button" data-type="video" class="explorer-type-filter">Videos</button></div><select id="explorer-sort" aria-label="Sort media"><option value="default">Sort: default</option><optgroup label="Name"><option value="filename_asc">Name (A–Z)</option><option value="filename_desc">Name (Z–A)</option></optgroup><optgroup label="Date"><option value="date_desc">Date added (newest)</option><option value="date_asc">Date added (oldest)</option><option value="downloaded_desc">Date downloaded (newest)</option><option value="downloaded_asc">Date downloaded (oldest)</option><option value="modified_desc">Date modified (newest)</option><option value="modified_asc">Date modified (oldest)</option></optgroup><optgroup label="Media"><option value="duration_desc">Duration (longest)</option><option value="duration_asc">Duration (shortest)</option><option value="size_desc">File size (largest)</option><option value="size_asc">File size (smallest)</option><option value="rating_desc">Rating (highest)</option><option value="rating_asc">Rating (lowest)</option></optgroup><optgroup label="Source"><option value="creator_asc">Creator (A–Z)</option><option value="creator_desc">Creator (Z–A)</option><option value="source_asc">Source (A–Z)</option><option value="source_desc">Source (Z–A)</option></optgroup><option value="shuffle">Random</option></select><select id="explorer-tag-filter" aria-label="Filter by tag"><option value="">All tags</option></select><select id="explorer-max-rating" aria-label="Maximum rating"><option value="">All ratings</option><option value="4">Up to 4 stars</option><option value="3">Up to 3 stars</option><option value="2">Up to 2 stars</option><option value="1">Up to 1 star</option></select><select id="explorer-rating-status" aria-label="Rating status"><option value="">All review states</option><option value="unrated">Unrated</option><option value="auto">Auto rated</option><option value="needs_review">Needs review</option><option value="reviewed">Human reviewed</option></select></div><div id="explorer-bulk-bar" hidden><span id="explorer-selection-count" class="mono small"></span><button type="button" data-bulk="add-group">Add to Group</button><button type="button" data-bulk="add-tag">Add Tag</button><button type="button" data-bulk="set-rating">Set Rating</button><button type="button" data-bulk="move">Move</button><button type="button" data-bulk="delete" class="danger">Delete</button><button type="button" data-bulk="review">Review</button><button type="button" data-bulk="play">Play Selected</button><button type="button" data-bulk="refresh">Refresh Metadata</button><button type="button" data-bulk="open-source">Open Source</button><button type="button" data-bulk="clear">Clear</button></div>';
  legacyToolbar.before(toolbar);
  if (hasNativeHostBridge()) {
    const importFolder = document.createElement('button');
    importFolder.id = 'explorer-import-folder';
    importFolder.className = 'btn btn-ghost';
    importFolder.type = 'button';
    importFolder.textContent = 'Import folder';
    importFolder.addEventListener('click', importNativeFolder);
    explorerEl('.explorer-toolbar-actions', toolbar).insertBefore(importFolder, explorerEl('.explorer-search', toolbar));
  }
  const topNavigation = document.createElement('nav');
  topNavigation.className = 'explorer-top-navigation';
  topNavigation.setAttribute('aria-label', 'Primary navigation');
  topNavigation.innerHTML = '<button type="button" data-top-nav="library">Library</button><button type="button" data-top-nav="discover">Discover</button><button type="button" data-top-nav="organization">Organization</button><button type="button" data-top-nav="activity">Activity <span id="topnav-download-count" class="nav-count" hidden></span></button><button type="button" data-top-nav="settings">Settings</button>';
  explorerEl('.explorer-toolbar-top', toolbar).after(topNavigation);
  const topDestinations = { library: 'all', discover: 'search', organization: 'groups', activity: 'downloads' };
  explorerAll('[data-top-nav]', topNavigation).forEach((button) => button.addEventListener('click', () => {
    const destination = button.dataset.topNav;
    if (destination === 'settings') { openSettingsModal(); return; }
    navigateTo(topDestinations[destination] || 'all');
  }));
  const sizeBackfill = document.createElement('small');
  sizeBackfill.id = 'explorer-size-backfill';
  sizeBackfill.className = 'muted mono';
  sizeBackfill.hidden = true;
  explorerEl('.explorer-toolbar-top', toolbar).append(sizeBackfill);
  const sizeFilter = document.createElement('select');
  const vrPlayButton = document.createElement('button');
  vrPlayButton.type = 'button';
  vrPlayButton.dataset.playMode = 'vr';
  vrPlayButton.textContent = 'VR';
  explorerEl('#explorer-play-menu', toolbar).append(vrPlayButton);
  sizeFilter.id = 'explorer-size-filter';
  sizeFilter.setAttribute('aria-label', 'Filter by file size');
  sizeFilter.innerHTML = '<option value="">Any size</option><option value="under-10mb">Under 10 MB</option><option value="10mb-100mb">10–100 MB</option><option value="100mb-1gb">100 MB–1 GB</option><option value="over-1gb">Over 1 GB</option><option value="unknown">Unknown size</option>';
  explorerEl('.explorer-toolbar-filters', toolbar).append(sizeFilter);
  const layoutControls = document.createElement('div'); layoutControls.className = 'explorer-layout-controls';
  layoutControls.setAttribute('role', 'group'); layoutControls.setAttribute('aria-label', 'Library layout');
  layoutControls.innerHTML = '<button type="button" data-library-layout="grid" aria-pressed="false">Grid</button><button type="button" data-library-layout="table" aria-pressed="false">Table</button>';
  explorerEl('.explorer-toolbar-filters', toolbar).prepend(layoutControls);
  const columns = document.createElement('div'); columns.id = 'explorer-columns'; columns.className = 'explorer-columns';
  columns.innerHTML = '<span><input id="explorer-select-all" type="checkbox" aria-label="Select all media"></span><button type="button" data-sort="filename_asc">Name</button><button type="button" data-sort="creator_asc">Creator</button><button type="button" data-sort="source_asc">Source</button><button type="button" data-sort="duration_desc">Duration</button><button type="button" data-sort="size_desc">Size</button><button type="button" data-sort="rating_desc">Rating</button><button type="button" data-sort="date_desc">Date Added</button>';
  const panel = document.createElement('section'); panel.id = 'explorer-panel'; panel.className = 'explorer-panel'; panel.hidden = true;
  legacyToolbar.after(columns, panel); explorerEl('#grid').classList.add('explorer-rows');

  explorerEl('#explorer-add-source-main').addEventListener('click', () => explorerEl('#add-source-btn')?.click());
  explorerAll('.explorer-type-filter', toolbar).forEach((button) => button.addEventListener('click', () => {
    state.typeFilter = button.dataset.type; explorer.active = 'media'; state.explorerSection = 'media'; explorer.nav = ({ all: 'all', image: 'images', clip: 'clips', video: 'videos' })[state.typeFilter] || 'all';
    explorerAll('.explorer-type-filter', toolbar).forEach((item) => item.classList.toggle('active', item === button)); clearExplorerSelection(); updateNavigation(); explorerLoadView();
  }));
  explorerEl('#explorer-sort').addEventListener('change', (event) => { state.sortOrder = event.target.value; gridShuffleSeed = state.sortOrder === 'shuffle' ? 1 + Math.floor(Math.random() * 2147483645) : null; explorerLoadView(); });
  explorerEl('#explorer-tag-filter').addEventListener('change', (event) => { state.tagFilter = event.target.value; explorerLoadView(); });
  sizeFilter.addEventListener('change', (event) => { state.sizeFilter = event.target.value; explorerLoadView(); });
  explorerEl('#explorer-max-rating').addEventListener('change', (event) => { state.maxRatingFilter = event.target.value; explorerLoadView(); });
  explorerEl('#explorer-rating-status').addEventListener('change', (event) => { state.ratingStatus = event.target.value; explorerLoadView(); });
  explorerAll('[data-library-layout]', layoutControls).forEach((button) => button.addEventListener('click', () => setExplorerLayout(button.dataset.libraryLayout)));
  explorerEl('#explorer-library-search').addEventListener('input', (event) => { clearTimeout(explorer.searchTimer); explorer.searchTimer = setTimeout(() => { explorer.searchQuery = event.target.value; explorerLoadView(); }, 180); });
  explorerEl('#explorer-select-all').addEventListener('change', selectAllVisible);
  explorerAll('[data-sort]', columns).forEach((button) => button.addEventListener('click', () => { const sort = explorerEl('#explorer-sort'); sort.value = button.dataset.sort; sort.dispatchEvent(new Event('change')); }));
  explorerEl('#explorer-bulk-bar').addEventListener('click', (event) => { const button = event.target.closest('[data-bulk]'); if (button) handleBulkAction(button.dataset.bulk); });
  const playToggle = explorerEl('#explorer-play-toggle'); const playMenu = explorerEl('#explorer-play-menu');
  playToggle.addEventListener('click', () => { playMenu.hidden = !playMenu.hidden; playToggle.setAttribute('aria-expanded', String(!playMenu.hidden)); });
  explorerEl('#explorer-play-primary').addEventListener('click', () => launchPlayMode());
  explorerAll('[data-play-mode]', playMenu).forEach((button) => button.addEventListener('click', () => { playMenu.hidden = true; playToggle.setAttribute('aria-expanded', 'false'); setPlayMode(button.dataset.playMode); launchPlayMode(button.dataset.playMode); }));
  document.addEventListener('click', (event) => { if (!event.target.closest('.explorer-play-split')) { playMenu.hidden = true; playToggle.setAttribute('aria-expanded', 'false'); } });

  const slideshow = explorerEl('#slideshow'); const goonHud = document.createElement('section'); goonHud.id = 'goon-hud'; goonHud.hidden = true;
  goonHud.innerHTML = '<header><span id="goon-stage-title">GOON</span><span id="goon-stage-time" class="mono">0:00</span></header><p id="goon-stage-prompt"></p><div class="goon-readout"><span id="goon-intensity" class="mono">Count in</span><span id="goon-soundtrack" class="mono"></span></div><div class="goon-controls"><button id="goon-start" class="btn btn-accent" type="button">Start session</button><button id="goon-pause" class="btn btn-ghost" type="button">Pause</button><button id="goon-tap" class="btn btn-ghost" type="button">Tap tempo</button><label>BPM <input id="goon-bpm" type="number" min="40" max="300" step="0.1"></label><label>offset <input id="goon-offset" type="number" min="-30" max="30" step="0.01">s</label><label><input id="goon-metronome" type="checkbox"> metronome</label><label>vol <input id="goon-metronome-volume" type="range" min="0" max="1" step="0.05"></label><button id="goon-end" type="button">End session</button></div><input id="goon-seek" class="goon-seek" type="range" min="0" value="0" aria-label="Seek beat timeline"><div id="goon-beat-map" class="goon-beat-map" aria-label="Beat map"></div>';
  slideshow.append(goonHud);
  explorerEl('#goon-start').addEventListener('click', () => explorer.goon && goonStartClock(explorer.goon));
  explorerEl('#goon-pause').addEventListener('click', () => explorer.goon && goonPauseClock(explorer.goon));
  explorerEl('#goon-tap').addEventListener('click', () => explorer.goon && goonTapTempo(explorer.goon));
  explorerEl('#goon-bpm').addEventListener('change', (event) => explorer.goon && goonSetBpm(explorer.goon, event.target.value));
  explorerEl('#goon-offset').addEventListener('change', (event) => {
    const session = explorer.goon; if (!session) return;
    const beat = goonCurrentBeat(session);
    session.beatOffsetSecs = Number(event.target.value) || 0;
    goonRebase(session, beat);
    goonRecordTimingCorrection(session, 'offset');
  });
  explorerEl('#goon-metronome').addEventListener('change', (event) => {
    if (!explorer.goon) return; explorer.goon.metronome.enabled = event.target.checked; appSettings.metronome_enabled = event.target.checked;
    api('/api/settings', { method: 'PATCH', body: JSON.stringify({ metronome_enabled: event.target.checked }) }).catch(() => {});
  });
  explorerEl('#goon-metronome-volume').addEventListener('input', (event) => {
    if (!explorer.goon) return; const volume = Number(event.target.value); explorer.goon.metronome.volume = volume; appSettings.metronome_volume = volume;
    api('/api/settings', { method: 'PATCH', body: JSON.stringify({ metronome_volume: volume }) }).catch(() => {});
  });
  explorerEl('#goon-seek').addEventListener('input', (event) => {
    const session = explorer.goon; if (!session) return; goonRebase(session, Number(event.target.value));
    goonRecordTimingCorrection(session, 'seek');
    const index = goonStageIndex(session, Math.floor(session.anchorBeat)); if (index !== session.stageIndex) goonApplyStage(session, index); goonRenderBeatMap(session, session.anchorBeat);
  });
  explorerEl('#goon-end').addEventListener('click', endGoonSession);
  setExplorerLayout(explorer.layout, false); setPlayMode(explorer.playMode, false); updatePlayCapabilities();
  window.addEventListener('resize', updatePlayCapabilities, { passive: true });
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) stopActivityPolling();
    else if (explorer.active === 'downloads') renderExplorerPanel('downloads');
  });
  void restorePlayMode(); updateNavigation(); updateBulkUI();
}

// This script is loaded after the library markup and app.js.  Installing
// synchronously leaves the old controls in place for existing bindings while
// ensuring they are never rendered as a competing surface.
installExplorerUi();
