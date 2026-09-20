'use strict';

const el = (sel, root = document) => root.querySelector(sel);
const TOTAL_STEPS = 8;

let step = 1;
let status = null;          // last GET /api/oobe/status response
let defaultDataDir = null;  // cached on first load, for "Use default"
// Executable overrides confirmed via a Step 2/4 "Test" button — sent along
// with everything else in the single save before Finish.
const overrides = { gallery_dl_bin: null, ffprobe_bin: null };
// config.json fields (data_dir, gallery_dl_bin, ffprobe_bin) only take
// effect on next launch — flagged on the Finish summary when a save
// actually touched one of them, so nobody is left wondering why nothing
// visibly changed.
let restartNeeded = false;

// ── theme preview (same logic as app.js's applyTheme, duplicated here on
//    purpose: oobe.html is a standalone page and pulling in the rest of
//    app.js would drag in the whole app's DOM assumptions with it) ────────
let systemThemeMedia = null;
const LEGACY_THEME_MAP = {
  yotsuba: 'linen', 'yotsuba-b': 'midnight', futaba: 'ember', burichan: 'midnight',
  tomorrow: 'linen', photon: 'linen', light: 'linen', 'oled-dark': 'oled', dark: 'atelier-dark',
};
const GTK_THEME_FAMILIES = ['adwaita', 'yaru', 'arc', 'breeze'];
const GTK_ACCENTS = { blue: '#3584e4', teal: '#2190a4', green: '#3a944a', yellow: '#c88800', orange: '#e66100', red: '#e62b38', pink: '#d56199', purple: '#9141ac', slate: '#5e5c64' };
function normalizeTheme(theme) {
  return LEGACY_THEME_MAP[theme] || theme;
}
function clientAppearance() {
  const injected = window.__CURATOR_CLIENT_APPEARANCE__;
  return injected && typeof injected === 'object' ? injected : { gtk_name: null, prefers_dark: window.matchMedia('(prefers-color-scheme: dark)').matches, accent: null, font: null };
}
function applyClientAppearance(appearance, active) {
  const root = document.documentElement; root.style.removeProperty('--client-accent'); root.style.removeProperty('--client-font');
  if (!active) return;
  const raw = String(appearance.accent || '').trim().toLowerCase(); const accent = GTK_ACCENTS[raw] || (/^#[0-9a-f]{6}$/i.test(raw) ? raw : '');
  if (accent) root.style.setProperty('--client-accent', accent);
  const font = String(appearance.font || '').trim(); if (/^[\w\s,'-]{1,120}$/.test(font)) root.style.setProperty('--client-font', font);
}
function applyTheme(theme) {
  theme = normalizeTheme(theme);
  if (systemThemeMedia) { systemThemeMedia.onchange = null; systemThemeMedia = null; }
  const appearance = clientAppearance();
  if (theme === 'gtk-system') {
    const name = String(appearance.gtk_name || '').toLowerCase(); const family = GTK_THEME_FAMILIES.find((candidate) => name.includes(candidate)) || 'adwaita';
    document.documentElement.dataset.theme = `${family}-${appearance.prefers_dark ? 'dark' : 'light'}`;
    applyClientAppearance(appearance, true);
  } else if (theme === 'system') {
    systemThemeMedia = window.matchMedia('(prefers-color-scheme: light)');
    const resolve = () => {
      if (systemThemeMedia.matches) document.documentElement.dataset.theme = 'linen';
      else delete document.documentElement.dataset.theme;
      applyClientAppearance(appearance, false);
    };
    resolve();
    systemThemeMedia.onchange = resolve;
  } else if (theme === 'atelier-dark') {
    delete document.documentElement.dataset.theme;
    applyClientAppearance(appearance, false);
  } else {
    document.documentElement.dataset.theme = theme;
    applyClientAppearance(appearance, GTK_THEME_FAMILIES.some((family) => theme.startsWith(`${family}-`)));
  }
}

async function api(path, opts) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...opts,
  });
  let body = null;
  try { body = await res.json(); } catch { /* no body */ }
  if (!res.ok) {
    const message = (body && body.error) || `Request failed (${res.status})`;
    throw new Error(message);
  }
  if (!body || typeof body !== 'object') {
    throw new Error('Curator returned an invalid setup response. Restart Curator and try again.');
  }
  return body;
}

function showError(message) {
  const banner = el('#oobe-error-banner');
  if (!message) { banner.hidden = true; banner.textContent = ''; return; }
  banner.textContent = message;
  banner.hidden = false;
}

// ── dependency row rendering ──────────────────────────────────────────────

function renderDep(key, dep) {
  const icon = el(`#dep-${key}-icon`);
  const detail = el(`#dep-${key}-detail`);
  if (!icon || !detail) return;
  icon.classList.remove('ok', 'err', 'pending');
  detail.classList.remove('err-text');
  if (dep.found) {
    icon.textContent = '✓';
    icon.classList.add('ok');
    detail.textContent = dep.version ? `Found — ${dep.version}` : 'Found.';
  } else {
    icon.textContent = dep.required ? '✕' : '–';
    icon.classList.add(dep.required ? 'err' : 'pending');
    if (dep.required) detail.classList.add('err-text');
    detail.textContent = dep.detail || 'Not found.';
  }
  return dep.found;
}

async function testDependency(check, key, pathInputSel) {
  const input = pathInputSel ? el(pathInputSel) : null;
  const path = input && input.value.trim() ? input.value.trim() : undefined;
  const icon = el(`#dep-${key}-icon`);
  if (icon) { icon.textContent = '…'; icon.className = 'oobe-dep-icon pending'; }
  try {
    const dep = await api('/api/oobe/validate', {
      method: 'POST',
      body: JSON.stringify({ check, path }),
    });
    dep.required = check === 'gallery_dl';
    const found = renderDep(key, dep);
    if (found && path) {
      if (check === 'gallery_dl') overrides.gallery_dl_bin = path;
      if (check === 'ffprobe') overrides.ffprobe_bin = path;
    }
    return found;
  } catch (e) {
    if (icon) { icon.textContent = '✕'; icon.className = 'oobe-dep-icon err'; }
    el(`#dep-${key}-detail`).textContent = e.message;
    return false;
  }
}

// ── status load + step prefill ────────────────────────────────────────────

async function loadStatus() {
  status = await api('/api/oobe/status');
  status.settings.theme = normalizeTheme(status.settings.theme);
  applyTheme(status.settings.theme);

  const gd = status.dependencies.gallery_dl; gd.required = true;
  const fp = status.dependencies.ffmpeg; fp.required = false;
  const ns = status.dependencies.nsfw; ns.required = false;
  renderDep('gallery_dl', gd);
  renderDep('ffprobe', fp);
  renderDep('nsfw', ns);
  el('#dep-gallery_dl-override').hidden = gd.found;
  el('#dep-ffprobe-override').hidden = fp.found;

  // Step 3 — data directory
  if (defaultDataDir === null) defaultDataDir = status.data_dir.path;
  el('#data-dir-input').value = status.data_dir.path;

  // Step 4 mirrors the same gallery-dl result with configuration-specific copy
  el('#step4-icon').className = 'oobe-dep-icon ' + (gd.found ? 'ok' : 'err');
  el('#step4-icon').textContent = gd.found ? '✓' : '✕';
  el('#step4-name').textContent = gd.checked;
  el('#step4-detail').textContent = gd.found
    ? (gd.version ? `Working — ${gd.version}. Nothing else to configure here.` : 'Working.')
    : (gd.detail || 'Not found.');

  // Step 5
  el('#max-concurrent-input').value = status.settings.max_concurrent;

  // Step 6
  el('#nsfw-enabled-input').checked = !!status.settings.nsfw_filter_enabled;
  el('#phar-setup-requested-input').checked = !!status.config.phar?.requested;

  // Step 7
  el('#theme-input').value = status.settings.theme;
  el('#slideshow-speed-input').value = String(status.settings.default_slideshow_speed);
  el('#slideshow-loop-input').checked = !!status.settings.default_slideshow_loop;
  el('#slideshow-shuffle-input').checked = !!status.settings.default_slideshow_shuffle;
}

// ── step navigation ────────────────────────────────────────────────────────

function updateProgress() {
  const list = el('#oobe-progress');
  list.innerHTML = '';
  for (let i = 1; i <= TOTAL_STEPS; i++) {
    const li = document.createElement('li');
    if (i < step) li.className = 'done';
    else if (i === step) li.className = 'current';
    list.appendChild(li);
  }
}

function showStep(n) {
  step = n;
  document.querySelectorAll('.oobe-step').forEach((sec) => {
    sec.hidden = Number(sec.dataset.step) !== n;
  });
  el('#oobe-step-num').textContent = String(n);
  updateProgress();

  const nav = el('#oobe-nav');
  nav.hidden = n === 1;
  el('#oobe-back-btn').hidden = n === 1;
  el('#oobe-next-btn').hidden = n === TOTAL_STEPS;
  el('#oobe-finish-btn').hidden = n !== TOTAL_STEPS;

  if (n === 8) renderFinish();
}

async function goNext() {
  if (step === 7) {
    // Single save point: everything gathered across the wizard goes to the
    // server right before the Finish summary, rather than round-tripping on
    // every step. See oobe.rs build notes for why.
    await saveDraft();
  }
  if (step < TOTAL_STEPS) showStep(step + 1);
}

function goBack() {
  if (step > 1) showStep(step - 1);
}

async function saveDraft() {
  const body = {
    max_concurrent: Number(el('#max-concurrent-input').value) || undefined,
    theme: el('#theme-input').value,
    default_slideshow_speed: Number(el('#slideshow-speed-input').value),
    default_slideshow_loop: el('#slideshow-loop-input').checked,
    default_slideshow_shuffle: el('#slideshow-shuffle-input').checked,
    nsfw_filter_enabled: el('#nsfw-enabled-input').checked,
    phar_setup_requested: el('#phar-setup-requested-input').checked,
  };
  const dataDirValue = el('#data-dir-input').value.trim();
  if (dataDirValue && dataDirValue !== status.data_dir.path) body.data_dir = dataDirValue;
  if (overrides.gallery_dl_bin) body.gallery_dl_bin = overrides.gallery_dl_bin;
  if (overrides.ffprobe_bin) body.ffprobe_bin = overrides.ffprobe_bin;
  restartNeeded = !!(body.data_dir || body.gallery_dl_bin || body.ffprobe_bin);

  try {
    status = await api('/api/oobe/settings', { method: 'POST', body: JSON.stringify(body) });
    showError(null);
  } catch (e) {
    showError(e.message);
    throw e;
  }
}

function renderFinish() {
  const s = status;
  const gd = s.dependencies.gallery_dl;
  const fp = s.dependencies.ffmpeg;
  const ns = s.dependencies.nsfw;
  const phar = s.config.phar;
  const rows = [
    ['gallery-dl', gd.found ? 'Ready' : 'Missing', gd.found ? 'ok' : 'err'],
    ['ffmpeg / ffprobe', fp.found ? 'Ready' : 'Optional — not found', fp.found ? 'ok' : 'warn'],
    ['Data directory', s.data_dir.path, s.data_dir.writable ? 'ok' : 'err'],
    ['Content classifier', ns.found ? (s.settings.nsfw_filter_enabled ? 'Ready & enabled' : 'Ready, not enabled') : 'Not available', ns.found ? 'ok' : 'warn'],
    ['P-HAR', phar?.requested ? `${phar.phase || 'requested'} (${phar.backend || 'auto'})` : 'Not requested', phar?.ready ? 'ok' : (phar?.requested ? 'warn' : '')],
    ['Theme', s.settings.theme, ''],
    ['Concurrent downloads', String(s.settings.max_concurrent), ''],
  ];
  el('#oobe-summary').innerHTML = rows.map(([label, value, cls]) => `
    <div class="oobe-summary-row">
      <span class="label">${label}</span>
      <span class="value ${cls}">${escapeHtml(value)}</span>
    </div>`).join('');

  const finishBtn = el('#oobe-finish-btn');
  const blocker = el('#finish-blocker');
  if (!gd.found) {
    finishBtn.disabled = true;
    finishBtn.textContent = 'Finish Setup';
    blocker.hidden = false;
    blocker.textContent = gd.detail || 'gallery-dl is required before setup can finish.';
  } else {
    finishBtn.disabled = false;
    blocker.hidden = true;
    finishBtn.textContent = (fp.found && ns.found) ? 'Finish Setup' : 'Finish Anyway';
  }

  const restartNote = el('#finish-restart-note');
  restartNote.hidden = !restartNeeded;

  const detailsBits = [gd, fp, ns]
    .map((d) => `${d.checked}: found=${d.found}${d.version ? ' version=' + d.version : ''}${d.detail ? ' — ' + d.detail : ''}`)
    .join('\n');
  el('#finish-details-text').textContent = detailsBits;
  el('#finish-details').hidden = false;
}

function escapeHtml(str) {
  return String(str).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

async function finishSetup() {
  const btn = el('#oobe-finish-btn');
  btn.disabled = true;
  try {
    await api('/api/oobe/complete', { method: 'POST' });
    window.location.href = '/';
  } catch (e) {
    showError(e.message);
    btn.disabled = false;
    // Refresh the summary so the blocker text reflects the latest check.
    status = await api('/api/oobe/status');
    renderFinish();
  }
}

// ── wiring ─────────────────────────────────────────────────────────────────

document.addEventListener('click', async (e) => {
  const btn = e.target.closest('[data-action]');
  if (!btn) return;
  const action = btn.dataset.action;

  try {
    if (action === 'get-started') { showStep(2); await loadStatus(); }
    else if (action === 'skip-to-finish') { await saveDraft().catch(() => {}); showStep(8); }
    else if (action === 'next') await goNext();
    else if (action === 'back') goBack();
    else if (action === 'finish') await finishSetup();
    else if (action === 'test-gallery_dl') {
      btn.disabled = true;
      await testDependency('gallery_dl', 'gallery_dl', '#dep-gallery_dl-path');
      btn.disabled = false;
    }
    else if (action === 'test-ffprobe') {
      btn.disabled = true;
      await testDependency('ffprobe', 'ffprobe', '#dep-ffprobe-path');
      btn.disabled = false;
    }
    else if (action === 'retest-gallery_dl') {
      btn.disabled = true;
      await loadStatus();
      btn.disabled = false;
    }
    else if (action === 'use-default-dir') {
      el('#data-dir-input').value = defaultDataDir || '';
      el('#data-dir-status').textContent = '';
    }
    else if (action === 'verify-dir') {
      btn.disabled = true;
      const path = el('#data-dir-input').value.trim();
      const out = el('#data-dir-status');
      try {
        const result = await api('/api/oobe/validate', {
          method: 'POST',
          body: JSON.stringify({ check: 'data_dir', path }),
        });
        out.className = 'oobe-inline-status ' + (result.writable ? 'ok' : 'err');
        out.textContent = result.writable ? 'Writable.' : (result.error || 'Not writable.');
      } catch (err) {
        out.className = 'oobe-inline-status err';
        out.textContent = err.message;
      }
      btn.disabled = false;
    }
  } catch (err) {
    showError(err.message);
  }
});

el('#theme-input').addEventListener('change', (e) => applyTheme(e.target.value));

showStep(1);
loadStatus().catch((e) => showError(e.message));
