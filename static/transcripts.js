/* ──────────────────────────────────────────────────────────────────────────
   Transcript Fixes (admin) — paste chunk paths, listen, and correct each clip's
   verified_transcription. A PURE TEXT EDIT: the save endpoint touches only the
   text, never status / validator / wallet / leaderboard. Every endpoint it calls
   is admin-gated server-side (validate.py _require_admin).
   ────────────────────────────────────────────────────────────────────────── */
const API = '/recordings/validate/api';
const AUDIO_BASE = '/recordings/validate/audio';

const pathsEl = document.getElementById('paths');
const loadBtn = document.getElementById('load-btn');
const statEl  = document.getElementById('load-stat');
const clipsEl = document.getElementById('clips');

function esc(s) {
  return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;')
                  .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

async function loadClips() {
  const raw = pathsEl.value.trim();
  if (!raw) { statEl.textContent = 'Paste some chunk paths first.'; return; }
  loadBtn.disabled = true;
  statEl.textContent = 'Loading…';
  try {
    const res = await fetch(`${API}/admin/transcripts/lookup`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ paths: raw }),
    });
    if (!res.ok) throw new Error((await res.json().catch(() => ({}))).detail || res.statusText);
    const { items } = await res.json();
    render(items);
  } catch (e) {
    statEl.innerHTML = `<span class="miss">Couldn't load: ${esc(e.message)}</span>`;
  } finally {
    loadBtn.disabled = false;
  }
}

function render(items) {
  clipsEl.innerHTML = '';
  const found = items.filter((it) => it.found);
  const missing = items.filter((it) => !it.found);
  statEl.innerHTML = `Loaded <b>${found.length}</b> clip${found.length === 1 ? '' : 's'}`
    + (missing.length ? ` · <span class="miss">${missing.length} not found</span>` : '');

  items.forEach((it, i) => clipsEl.appendChild(it.found ? clipCard(it, i + 1) : missingCard(it)));
}

function missingCard(it) {
  const el = document.createElement('div');
  el.className = 'clip missing';
  el.innerHTML = `<div class="clip-head"><span class="badge del">not found</span>`
    + `<span class="clip-date">${esc(it.reason || 'unrecognized')}</span></div>`
    + `<div class="miss-line">${esc(it.input)}</div>`;
  return el;
}

function clipCard(it, idx) {
  const el = document.createElement('div');
  el.className = 'clip' + (it.is_deleted ? ' deleted' : '');

  const audioUrl = `${AUDIO_BASE}/${encodeURIComponent(it.owner_id)}/${encodeURIComponent(it.date)}/${encodeURIComponent(it.filename)}`;
  const rawText = (it.transcription || '').trim();
  const rawBlock = rawText
    ? `<p class="raw-note">Raw ASR: <b>${esc(rawText)}</b></p>` : '';
  const delBadge = it.is_deleted ? `<span class="badge del">deleted file</span>` : '';

  el.innerHTML = `
    <div class="clip-head">
      <span class="clip-idx">#${idx}</span>
      <span class="clip-name">${esc(it.filename)}</span>
      <span class="clip-date">${esc(it.date)}</span>
      <span class="badge ${esc(it.status)}">${esc(it.status)}</span>
      ${delBadge}
    </div>
    <audio controls preload="none" src="${audioUrl}"></audio>
    ${rawBlock}
    <div class="txt-label">Confirmed transcription</div>
    <textarea class="txt-box"></textarea>
    <div class="clip-actions">
      <button type="button" class="save-btn" disabled>Save</button>
      <span class="save-msg"></span>
    </div>`;

  const box     = el.querySelector('.txt-box');
  const saveBtn = el.querySelector('.save-btn');
  const msg     = el.querySelector('.save-msg');
  const original = it.verified_transcription || '';
  box.value = original;

  let saved = original;
  const refresh = () => {
    const dirty = box.value !== saved;
    saveBtn.disabled = !dirty;
    if (dirty) { msg.className = 'save-msg dirty'; msg.textContent = 'Unsaved changes'; }
    else if (msg.classList.contains('dirty')) { msg.className = 'save-msg'; msg.textContent = ''; }
  };
  box.addEventListener('input', refresh);

  saveBtn.addEventListener('click', async () => {
    saveBtn.disabled = true;
    msg.className = 'save-msg'; msg.textContent = 'Saving…';
    try {
      const res = await fetch(`${API}/admin/transcripts/save`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ owner_id: it.owner_id, date: it.date, filename: it.filename, text: box.value }),
      });
      if (!res.ok) throw new Error((await res.json().catch(() => ({}))).detail || res.statusText);
      saved = box.value;
      msg.className = 'save-msg ok'; msg.textContent = '✓ Saved';
    } catch (e) {
      msg.className = 'save-msg err'; msg.textContent = esc(e.message);
      saveBtn.disabled = false;
    }
  });

  return el;
}

loadBtn.addEventListener('click', loadClips);
