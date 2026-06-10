/* ──────────────────────────────────────────────────────────────────────────
   My Submissions page controller — list of every decided chunk (accepted +
   rejected). Tap a row to open the shared ChunkEditor in a modal, edit the
   text and re-decide (accept <-> reject). Re-fetches after each change.

   Filters (combined with AND):
     - status:        all | verified | rejected
     - recorded date: the day the audio was recorded (chunk.date)
     - reviewed date: the local day it was accepted/rejected (validated_at)
   ────────────────────────────────────────────────────────────────────────── */
const API = '/recordings/validate/api';
const AUDIO_BASE = '/recordings/validate/audio';

let items = [];
let fStatus = 'all';
let fRecorded = 'all';
let fReviewed = 'all';
let current = -1;       // index into items of the open chunk

const listEl     = document.getElementById('list');
const statusEl   = document.getElementById('status-filter');
const recSelEl   = document.getElementById('rec-date');
const revSelEl   = document.getElementById('rev-date');
const modal      = document.getElementById('modal');
const modalEditor = document.getElementById('modal-editor');

async function decide(endpoint, body) {
  const it = items[current]; if (!it) return;
  await postJSON(API + endpoint, { owner_id: it.owner_id, date: it.date, filename: it.filename, ...body });
  closeModal(); await load();
}

const editor = new ChunkEditor(modalEditor, {
  audioBase: AUDIO_BASE,
  acceptLabel: 'Accept',
  rejectLabel: 'Reject',
  issueLabel: 'Issue',
  onAccept: (text, labels) => decide('/accept', { transcription: text, labels }),
  onIssue:  (text)         => decide('/issue',  { transcription: text }),
  onReject: ()             => decide('/reject', {}),
});

// Load the label taxonomy so the modal editor shows the chips (re-accepting a
// chunk requires >=1 label, matching the validate page + the backend).
(async () => {
  try {
    const res = await fetch(API + '/labels');
    const json = await res.json();
    if (json.labels) editor.setLabels(json.labels);
  } catch (_) { /* chips just won't show; the backend still enforces */ }
})();

async function postJSON(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) { alert('Action failed (' + res.status + '). Please try again.'); throw new Error('request failed'); }
  return res.json();
}

async function load() {
  const res = await fetch(API + '/submissions');
  const json = await res.json();
  items = json.items || [];
  render();
}

// ── Reviewed timestamp -> local { date, time } ──────────────────────────────
function reviewedLocal(iso) {
  if (!iso) return null;
  // validated_at is stored UTC (naive ISO) — treat as UTC, show in local time
  const d = new Date(/[zZ]|[+-]\d\d:?\d\d$/.test(iso) ? iso : iso + 'Z');
  if (isNaN(d)) return null;
  const p = (n) => String(n).padStart(2, '0');
  return {
    date: `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`,
    time: `${p(d.getHours())}:${p(d.getMinutes())}`,
  };
}

function uniqSorted(values) {
  return [...new Set(values.filter(Boolean))].sort().reverse();
}

function fillSelect(sel, current, allLabel, dates) {
  const opts = [`<option value="all">${allLabel}</option>`]
    .concat(dates.map((d) => `<option value="${d}"${d === current ? ' selected' : ''}>${d}</option>`));
  sel.innerHTML = opts.join('');
  sel.value = current;
}

function render() {
  // status pills with counts
  const counts = {
    all: items.length,
    verified: items.filter((i) => i.status === 'verified').length,
    issue: items.filter((i) => i.status === 'issue').length,
    rejected: items.filter((i) => i.status === 'rejected').length,
  };
  statusEl.innerHTML = [['all', 'All'], ['verified', 'Accepted'], ['issue', 'Issue'], ['rejected', 'Rejected']]
    .map(([k, label]) => `<span class="sub-pill ${fStatus === k ? 'active' : ''}" data-f="${k}">${label} (${counts[k]})</span>`)
    .join('');

  // date dropdowns (rebuilt from data each render; reset to 'all' if value vanished)
  const recDates = uniqSorted(items.map((i) => i.date));
  const revDates = uniqSorted(items.map((i) => { const r = reviewedLocal(i.validated_at); return r && r.date; }));
  if (fRecorded !== 'all' && !recDates.includes(fRecorded)) fRecorded = 'all';
  if (fReviewed !== 'all' && !revDates.includes(fReviewed)) fReviewed = 'all';
  fillSelect(recSelEl, fRecorded, 'All recorded dates', recDates);
  fillSelect(revSelEl, fReviewed, 'All reviewed dates', revDates);

  if (items.length === 0) {
    listEl.innerHTML = `<div class="state-card"><span class="state-emoji">🗂️</span>
      <h3>No submissions yet</h3>
      <p>Accept or reject some chunks on the <a href="/recordings/validate" style="color:var(--accent)">Validate</a> page and they'll show up here.</p></div>`;
    return;
  }

  const shown = items.filter((i) => {
    if (fStatus !== 'all' && i.status !== fStatus) return false;
    if (fRecorded !== 'all' && i.date !== fRecorded) return false;
    if (fReviewed !== 'all') { const r = reviewedLocal(i.validated_at); if (!r || r.date !== fReviewed) return false; }
    return true;
  });

  listEl.innerHTML = shown.length
    ? `<div class="sub-count">${shown.length} of ${items.length} shown</div>` + shown.map(rowHtml).join('')
    : `<div class="state-card">No items match these filters.</div>`;
}

function rowHtml(it) {
  // Submissions rows are always decided -> show the saved verified text as-is
  // (empty stays empty; never fall back to the ASR default).
  const text = it.verified_transcription || '';
  const snippet = text ? escapeHtml(text.slice(0, 90)) : '<span class="sub-empty">— no transcription —</span>';
  const key = `${it.owner_id}|${it.date}|${it.filename}`;
  const mark = it.status === 'verified' ? '✓' : it.status === 'issue' ? '⚠' : '✕';
  const r = reviewedLocal(it.validated_at);
  const reviewed = r ? `${r.date} ${r.time}` : '—';
  return `<div class="sub-row" data-key="${key}">
      <span class="sub-status badge ${it.status}">${mark}</span>
      <div class="sub-info">
        <div class="sub-name">${it.filename}</div>
        <div class="sub-meta">
          <span class="sub-tag" title="Date recorded">🎙 ${it.date}</span>
          <span class="sub-tag" title="Date reviewed">✎ ${reviewed}</span>
        </div>
        <div class="sub-snippet" dir="auto">${snippet}</div>
      </div>
      <span class="sub-chev">›</span>
    </div>`;
}

// ── Events ──────────────────────────────────────────────────────────────────
statusEl.addEventListener('click', (e) => {
  const p = e.target.closest('.sub-pill'); if (!p) return;
  fStatus = p.dataset.f; render();
});
recSelEl.addEventListener('change', () => { fRecorded = recSelEl.value; render(); });
revSelEl.addEventListener('change', () => { fReviewed = revSelEl.value; render(); });

listEl.addEventListener('click', (e) => {
  const row = e.target.closest('.sub-row'); if (!row) return;
  const key = row.dataset.key;
  current = items.findIndex((i) => `${i.owner_id}|${i.date}|${i.filename}` === key);
  if (current >= 0) openModal(items[current]);
});

function openModal(it) {
  editor.load(it);
  modal.classList.add('open');
}
function closeModal() {
  editor.audio.pause();
  modal.classList.remove('open');
}

document.getElementById('modal-close').addEventListener('click', closeModal);
modal.addEventListener('click', (e) => { if (e.target === modal) closeModal(); });
document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && modal.classList.contains('open')) closeModal(); });

function escapeHtml(s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// ── Insights drawer ─────────────────────────────────────────────────────────
const insightsDrawer   = document.getElementById('insights-drawer');
const insightsBackdrop = document.getElementById('insights-backdrop');
const insightsBody     = document.getElementById('insights-body');
const insightsOwnerRow = document.getElementById('insights-owner-row');
const insightsOwnerSel = document.getElementById('insights-owner');
let insightsOwner = null;   // currently selected owner id (null until owners load)

// Single-unit, magnitude-driven duration. Each value scales by its OWN size, so
// a panel can read e.g. "60s of 10h audio verified":
//   < 100s          -> seconds   (e.g. 60s)
//   100s .. <100min -> minutes   (e.g. 45m)
//   >= 100min       -> hours, unbounded (e.g. 10h)
function fmtDur(seconds) {
  const s = seconds || 0;
  if (s < 100) return `${Math.round(s)}s`;
  if (s < 6000) return `${Math.round(s / 60)}m`;
  return `${Math.round(s / 3600)}h`;
}

function renderInsights(d) {
  const vSec = d.verified_seconds || 0;
  const rSec = d.remaining_seconds || 0;
  const totSec = vSec + rSec;
  const vCount = d.verified_count, rCount = d.remaining_count, totCount = vCount + rCount;

  // "measured" = at least some audio lengths are known. Until the background
  // backfill has measured anything (e.g. the first minutes after a deploy),
  // totSec is 0 — in that window the bar/split go by CHUNK COUNT and the audio
  // column shows "measuring…" instead of a misleading 0.
  const measured = totSec > 0;

  // One bar, two complementary segments: verified slice + remaining slice of the
  // same whole. By time when measured, else by count. No more two separate bars.
  const frac = measured ? vSec / totSec : (totCount > 0 ? vCount / totCount : 0);
  const pct = Math.round(frac * 100);
  const vWidth = frac * 100;
  const rWidth = (measured || totCount > 0) ? 100 - vWidth : 0;

  const caption = measured
    ? `<b>${fmtDur(vSec)}</b> of ${fmtDur(totSec)} audio verified`
    : `<b>${vCount.toLocaleString()}</b> of ${totCount.toLocaleString()} chunks verified`;

  const dur = (sec) => measured ? fmtDur(sec) : '<span class="ins-pending">measuring…</span>';

  const note = d.remaining_unmeasured
    ? `<div class="ins-note">Measuring audio length for ${d.remaining_unmeasured.toLocaleString()} more chunk${d.remaining_unmeasured === 1 ? '' : 's'} — the time totals will keep rising.</div>`
    : '';

  insightsBody.innerHTML = `
    <div class="ins-headline"><span class="ins-pct">${pct}%</span><span class="lbl">complete</span></div>
    <div class="ins-caption">${caption}</div>

    <div class="pbar">
      <div class="pbar-seg v" data-w="${vWidth.toFixed(1)}"></div>
      <div class="pbar-seg r" data-w="${rWidth.toFixed(1)}"></div>
    </div>
    <div class="pbar-legend">
      <span><i class="ins-dot v"></i>Verified</span>
      <span><i class="ins-dot r"></i>Remaining</span>
    </div>

    <table class="ins-table">
      <thead><tr><th>Status</th><th>Chunks</th><th>Audio</th></tr></thead>
      <tbody>
        <tr><td class="name"><i class="ins-dot v"></i>Verified</td><td class="num">${vCount.toLocaleString()}</td><td>${dur(vSec)}</td></tr>
        <tr><td class="name"><i class="ins-dot r"></i>Remaining</td><td class="num">${rCount.toLocaleString()}</td><td>${dur(rSec)}</td></tr>
        <tr class="total"><td>In queue</td><td class="num">${totCount.toLocaleString()}</td><td>${dur(totSec)}</td></tr>
      </tbody>
    </table>
    ${note}`;

  // Animate the bar segments from empty to their targets on next frame.
  requestAnimationFrame(() => {
    insightsBody.querySelectorAll('.pbar-seg').forEach((el) => { el.style.width = el.dataset.w + '%'; });
  });
}

// Populate the owner dropdown with the viewer + only the users who granted them
// access (default = "My own voices"). Hidden entirely when there's no one else.
async function loadInsightsOwners() {
  const res = await fetch(API + '/owners');
  if (!res.ok) throw new Error('request failed');
  const json = await res.json();
  const owners = json.owners || [];
  insightsOwnerSel.innerHTML = owners
    .map((o) => `<option value="${o.id}">${o.is_self ? 'My own voices' : escapeHtml(o.name)}</option>`)
    .join('');
  const self = owners.find((o) => o.is_self) || owners[0];
  insightsOwner = self ? self.id : json.viewer_id;
  insightsOwnerSel.value = insightsOwner;
  insightsOwnerRow.style.display = owners.length > 1 ? 'flex' : 'none';
}

async function loadInsightsData() {
  insightsBody.innerHTML = `<div class="ins-loading">Loading…</div>`;
  const res = await fetch(API + '/insights?owner=' + encodeURIComponent(insightsOwner));
  if (!res.ok) throw new Error('request failed');
  renderInsights(await res.json());
}

async function openInsights() {
  insightsBody.innerHTML = `<div class="ins-loading">Loading…</div>`;
  insightsDrawer.classList.add('open');
  insightsBackdrop.classList.add('open');
  insightsDrawer.setAttribute('aria-hidden', 'false');
  try {
    await loadInsightsOwners();   // refreshed each open so new grants show up
    await loadInsightsData();
  } catch (e) {
    insightsBody.innerHTML = `<div class="ins-loading">Couldn't load insights. Please try again.</div>`;
  }
}
function closeInsights() {
  insightsDrawer.classList.remove('open');
  insightsBackdrop.classList.remove('open');
  insightsDrawer.setAttribute('aria-hidden', 'true');
}

insightsOwnerSel.addEventListener('change', async () => {
  insightsOwner = insightsOwnerSel.value;
  try { await loadInsightsData(); }
  catch (e) { insightsBody.innerHTML = `<div class="ins-loading">Couldn't load insights. Please try again.</div>`; }
});

document.getElementById('insights-btn').addEventListener('click', openInsights);
document.getElementById('insights-close').addEventListener('click', closeInsights);
insightsBackdrop.addEventListener('click', closeInsights);
document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && insightsDrawer.classList.contains('open')) closeInsights(); });

// ── Start gate: nothing loads until the user presses the button ─────────────
const startEl = document.getElementById('start');
const contentEl = document.getElementById('content');
document.getElementById('start-btn').addEventListener('click', async () => {
  startEl.style.display = 'none';
  contentEl.style.display = '';
  await load();
});
