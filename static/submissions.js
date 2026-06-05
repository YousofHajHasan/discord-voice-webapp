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
  onAccept: (text) => decide('/accept', { transcription: text }),
  onIssue:  (text) => decide('/issue',  { transcription: text }),
  onReject: ()     => decide('/reject', {}),
});

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
  const text = (it.verified_transcription || it.transcription || '');
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

load();
