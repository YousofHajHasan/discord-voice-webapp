/* ──────────────────────────────────────────────────────────────────────────
   Validate page controller — sequential, windowed, prefetching, multi-owner.

   Scales to thousands of chunks per user:
     - Nothing loads until the user presses Start.
     - Loads BATCH (10) pending chunks at a time. Each fetch LEASES its chunks to
       you on the server, so two validators on the same owner get different
       chunks (no double work). See validation_db.claim_pending_window.
     - When you reach the (BATCH - PREFETCH_AT_REMAINING + 1)th item (the 8th of
       10), the next 10 are fetched in the BACKGROUND so there's no wait.
     - Decisions are applied LOCALLY — never a full re-fetch per action.
     - "Back" walks this session's buffer; full history is on My Submissions.

   Multi-owner: an owner dropdown picks WHOSE voices to validate (yourself, or
   anyone who granted you access). Switching owners releases your held leases and
   reloads. A small "Manage access" panel grants/revokes who may validate yours.
   ────────────────────────────────────────────────────────────────────────── */
const API = '/recordings/validate/api';
const AUDIO_BASE = '/recordings/validate/audio';
const BATCH = 10;
const PREFETCH_AT_REMAINING = 3;   // 3 left in the window == sitting on the 8th of 10

let buffer = [];        // chunks loaded this session (pending + decided-this-session)
let frontier = 0;       // index of first not-yet-decided chunk
let pos = 0;            // currently viewed index
let pendingTotal = 0;   // live count of pending in the selected owner's queue
let noMore = false;
let loading = false;
let inflight = null;
let started = false;
let activeOwner = null;  // discord_id of the owner currently being validated

const startEl   = document.getElementById('start');
const startBtn  = document.getElementById('start-btn');
const contentEl = document.getElementById('content');
const host      = document.getElementById('editor-host');
const banner    = document.getElementById('banner');
const navEl     = document.getElementById('nav');
const backBtn   = document.getElementById('back');
const fwdBtn    = document.getElementById('fwd');
const counter   = document.getElementById('counter');
const progress  = document.getElementById('progress');

// Owner picker + access management
const ownerSelect  = document.getElementById('owner-select');
const manageToggle = document.getElementById('manage-toggle');
const managePanel  = document.getElementById('manage');
const grantId      = document.getElementById('grant-id');
const grantBtn     = document.getElementById('grant-btn');
const grantMsg     = document.getElementById('grant-msg');
const delegateList = document.getElementById('delegates');

const key = (c) => `${c.owner_id}|${c.date}|${c.filename}`;
const esc = (s) => String(s).replace(/[&<>"']/g, (c) =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

const editor = new ChunkEditor(host, {
  audioBase: AUDIO_BASE,
  issueLabel: 'Issue',
  onAccept: (text, labels) => decide('/accept', 'verified', { transcription: text, labels }),
  onIssue:  (text)         => decide('/issue',  'issue',    { transcription: text }),
  onReject: ()             => decide('/reject', 'rejected', {}),
  onTrimAccept: (text, start, end, labels) => trimAccept(text, start, end, labels),
});

startBtn.onclick = start;
backBtn.onclick  = () => { if (pos > 0) { pos--; render(); } };
fwdBtn.onclick   = () => { const max = Math.min(frontier, buffer.length - 1); if (pos < max) { pos++; render(); } };

// ── Owner picker ──────────────────────────────────────────────────────────────

async function loadOwners() {
  try {
    const res = await fetch(`${API}/owners`);
    const json = await res.json();
    const owners = json.owners || [];
    ownerSelect.innerHTML = owners.map((o) => {
      const name = o.is_self ? 'My own voices' : o.name;
      const label = name + (o.pending ? ` · ${o.pending} pending` : '');
      return `<option value="${esc(o.id)}">${esc(label)}</option>`;
    }).join('');
    activeOwner = owners.length ? owners[0].id : (json.viewer_id || null);
    if (activeOwner) ownerSelect.value = activeOwner;
  } catch (_) { /* dropdown stays empty; Start will retry */ }
}

async function loadLabels() {
  try {
    const res = await fetch(`${API}/labels`);
    const json = await res.json();
    if (json.labels) editor.setLabels(json.labels);
  } catch (_) { /* chips just won't show; the backend still enforces >=1 */ }
}

ownerSelect.onchange = () => {
  const next = ownerSelect.value;
  if (!next || next === activeOwner) return;
  releaseClaims(activeOwner);     // free leases on the owner we're leaving
  activeOwner = next;
  resetSession();
  if (started) {
    banner.innerHTML = '';
    fetchBatch().then(() => { frontier = 0; pos = 0; render(); });
  }
};

function resetSession() {
  buffer = []; frontier = 0; pos = 0; pendingTotal = 0;
  noMore = false; loading = false; inflight = null;
}

function releaseClaims(owner) {
  if (!owner) return;
  try {
    navigator.sendBeacon(`${API}/release`,
      new Blob([JSON.stringify({ owner })], { type: 'application/json' }));
  } catch (_) { /* best-effort — the 15-min lease frees them anyway */ }
}

window.addEventListener('pagehide', () => releaseClaims(activeOwner));

// ── Access management panel ───────────────────────────────────────────────────

manageToggle.onclick = async () => {
  const open = managePanel.style.display === 'none';
  managePanel.style.display = open ? '' : 'none';
  manageToggle.classList.toggle('active', open);
  if (open) await loadDelegates();
};

async function loadDelegates() {
  try {
    const res = await fetch(`${API}/delegates`);
    const json = await res.json();
    renderDelegates(json.delegates || []);
  } catch (_) { /* ignore */ }
}

function renderDelegates(list) {
  if (!list.length) {
    delegateList.innerHTML =
      `<li class="delegate-empty">No one yet. Add a Discord ID above to let someone validate your voices.</li>`;
    return;
  }
  delegateList.innerHTML = list.map((d) => `
    <li class="delegate-item">
      <span><span class="delegate-name">${esc(d.name)}</span>${
        d.name !== d.id ? `<span class="delegate-id">${esc(d.id)}</span>` : ''}</span>
      <button class="delegate-revoke" data-id="${esc(d.id)}">Revoke</button>
    </li>`).join('');
  delegateList.querySelectorAll('.delegate-revoke')
    .forEach((b) => { b.onclick = () => revoke(b.dataset.id); });
}

grantBtn.onclick = grant;
grantId.onkeydown = (e) => { if (e.key === 'Enter') grant(); };

async function grant() {
  const id = grantId.value.trim();
  if (!id) return;
  try {
    const res = await fetch(`${API}/grant`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ delegate_id: id }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) { showGrantMsg(json.detail || 'Could not grant access.', false); return; }
    grantId.value = '';
    showGrantMsg('Access granted.', true);
    renderDelegates(json.delegates || []);
  } catch (_) { showGrantMsg('Network error — try again.', false); }
}

async function revoke(id) {
  try {
    const res = await fetch(`${API}/revoke`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ delegate_id: id }),
    });
    const json = await res.json().catch(() => ({}));
    if (res.ok) renderDelegates(json.delegates || []);
  } catch (_) { /* ignore */ }
}

function showGrantMsg(msg, ok) {
  grantMsg.textContent = msg;
  grantMsg.className = 'manage-msg ' + (ok ? 'ok' : 'err');
}

// ── Validation flow ───────────────────────────────────────────────────────────

async function start() {
  if (started) return;
  if (!activeOwner) await loadOwners();
  if (!activeOwner) { alert('No voices available to validate yet.'); return; }
  started = true;
  startEl.style.display = 'none';
  contentEl.style.display = '';
  await fetchBatch();
  frontier = 0; pos = 0;
  render();
}

function fetchBatch() {
  if (loading || noMore) return inflight || Promise.resolve();
  loading = true;
  inflight = (async () => {
    try {
      const res = await fetch(`${API}/queue?limit=${BATCH}&owner=${encodeURIComponent(activeOwner)}`);
      if (!res.ok) return;
      const json = await res.json();
      const items = json.items || [];
      if (json.pending_total != null) pendingTotal = json.pending_total;
      if (items.length === 0) { noMore = true; return; }   // nothing new available to me
      const have = new Set(buffer.map(key));
      for (const it of items) if (!have.has(key(it))) buffer.push(it);
    } finally {
      loading = false; inflight = null;
    }
  })();
  return inflight;
}

async function decide(endpoint, newStatus, body) {
  const it = buffer[pos];
  if (!it) return;
  const wasFrontier = pos === frontier;

  const res = await fetch(API + endpoint, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ owner_id: it.owner_id, date: it.date, filename: it.filename, ...body }),
  });

  if (res.status === 409) {
    // Someone else validated this one (our lease lapsed). Drop it and move on.
    buffer.splice(pos, 1);
    if (frontier > pos) frontier--;
    pendingTotal = Math.max(0, pendingTotal - 1);
    if (pos > buffer.length - 1) pos = Math.max(0, buffer.length - 1);
    render();
    if (!noMore && (buffer.length - frontier) <= PREFETCH_AT_REMAINING) fetchBatch().then(() => render(false));
    flash('That chunk was just validated by someone else — skipping it.');  // shown last, clears itself
    return;
  }
  if (!res.ok) { alert('Action failed (' + res.status + '). Please try again.'); return; }

  it.status = newStatus;                                  // apply locally — no full refetch
  if (body.transcription !== undefined) it.verified_transcription = body.transcription;
  if (body.labels) it.labels = body.labels;               // so Back-nav shows the chosen labels

  if (wasFrontier) {
    frontier++;
    pendingTotal = Math.max(0, pendingTotal - 1);
    if (!noMore && (buffer.length - frontier) <= PREFETCH_AT_REMAINING) {
      if (frontier >= buffer.length) await fetchBatch();        // genuinely out of data — wait
      else fetchBatch().then(() => render(false));              // prefetch in background
    }
    pos = frontier < buffer.length ? frontier : buffer.length - 1;
  }
  render();
}

// Trim & Accept: cut the head/tail, save the result as a verified _updated chunk,
// then advance. Mirrors decide()'s local-apply + frontier-advance (no refetch);
// the server returns the new chunk, which replaces the original in the buffer so
// Back-navigation shows the cleaned, verified clip.
async function trimAccept(text, start, end, labels) {
  const it = buffer[pos];
  if (!it) return;
  const wasFrontier = pos === frontier;

  const res = await fetch(`${API}/trim_accept`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      owner_id: it.owner_id, date: it.date, filename: it.filename,
      start, end, transcription: text, labels,
    }),
  });

  if (res.status === 409) {
    buffer.splice(pos, 1);
    if (frontier > pos) frontier--;
    pendingTotal = Math.max(0, pendingTotal - 1);
    if (pos > buffer.length - 1) pos = Math.max(0, buffer.length - 1);
    render();
    if (!noMore && (buffer.length - frontier) <= PREFETCH_AT_REMAINING) fetchBatch().then(() => render(false));
    flash('That chunk was just validated by someone else — skipping it.');
    return;
  }
  if (!res.ok) { alert('Trim failed (' + res.status + '). Please try again.'); return; }

  const json = await res.json().catch(() => ({}));
  if (json.chunk) buffer[pos] = json.chunk;   // swap in the trimmed, verified clip (carries labels)
  else { it.status = 'verified'; if (labels) it.labels = labels; }

  if (wasFrontier) {
    frontier++;
    pendingTotal = Math.max(0, pendingTotal - 1);
    if (!noMore && (buffer.length - frontier) <= PREFETCH_AT_REMAINING) {
      if (frontier >= buffer.length) await fetchBatch();        // genuinely out of data — wait
      else fetchBatch().then(() => render(false));              // prefetch in background
    }
    pos = frontier < buffer.length ? frontier : buffer.length - 1;
  }
  render();
}

let flashTimer = null;
function flash(msg) {
  banner.innerHTML = `<div class="state-card" style="padding:14px 18px;margin-bottom:16px;"><p>${esc(msg)}</p></div>`;
  clearTimeout(flashTimer);
  flashTimer = setTimeout(() => render(false), 3500);   // render() resets the banner
}

// reloadEditor=false is used by background prefetch / flash timeout so it never
// reloads the current clip's audio or clobbers text the user is typing.
function render(reloadEditor = true) {
  if (buffer.length === 0) {
    progress.innerHTML = '';
    banner.innerHTML = (noMore && pendingTotal > 0) ? doneCard(true) : emptyCard();
    host.style.display = 'none';
    navEl.style.display = 'none';
    return;
  }

  const reviewed = frontier;
  const remaining = pendingTotal;
  const denom = reviewed + remaining;
  const pct = denom ? Math.round(reviewed / denom * 100) : 100;
  progress.innerHTML =
    `<div class="vp-bar"><div class="vp-fill" style="width:${pct}%"></div></div>
     <div class="vp-stats">${reviewed} reviewed this session · ${remaining} pending in queue</div>`;

  const allDone = frontier >= buffer.length && noMore;
  banner.innerHTML = allDone ? doneCard(pendingTotal > 0) : '';
  host.style.display = '';
  navEl.style.display = '';

  const it = buffer[pos];
  if (reloadEditor) {
    const reviewing = it.status !== 'pending';
    editor.setActionLabels(reviewing ? 'Save' : 'Accept', 'Reject');
    editor.load(it);
  }

  const max = Math.min(frontier, buffer.length - 1);
  backBtn.disabled = pos <= 0;
  fwdBtn.disabled  = pos >= max;
  const reviewing = it.status !== 'pending';
  counter.textContent = reviewing
    ? `Reviewing · #${pos + 1} of ${buffer.length} loaded`
    : `Current · ${remaining} pending left`;
}

function emptyCard() {
  return `<div class="state-card"><span class="state-emoji">🎙️</span>
    <h3>Nothing to validate</h3>
    <p>New speech chunks appear here as they're recorded and processed.</p></div>`;
}
function doneCard(othersBusy) {
  if (othersBusy) {
    return `<div class="state-card" style="padding:22px;margin-bottom:18px;">
      <span class="state-emoji">⏳</span>
      <h3>You're caught up here</h3>
      <p>The remaining chunks are being validated by someone else right now.
         Check back later, pick another voice above, or open
         <a href="/recordings/validate/submissions" style="color:var(--accent)">My Submissions</a>.</p></div>`;
  }
  return `<div class="state-card" style="padding:22px;margin-bottom:18px;">
    <span class="state-emoji">✅</span>
    <h3>All caught up</h3>
    <p>No more pending chunks. Use Back to revisit this session, or open
       <a href="/recordings/validate/submissions" style="color:var(--accent)">My Submissions</a>.</p></div>`;
}

// Fetch the label taxonomy + populate the owner dropdown up front.
loadLabels();
loadOwners();
