/* ──────────────────────────────────────────────────────────────────────────
   Validate page controller — sequential, windowed, prefetching.

   Scales to thousands of chunks per user:
     - Nothing loads until the user presses Start.
     - Loads BATCH (10) pending chunks at a time via a keyset cursor.
     - When the user reaches the (BATCH - PREFETCH_AT_REMAINING + 1)th item
       (the 8th of 10), the next 10 are fetched in the BACKGROUND so there's
       no wait.
     - Decisions are applied LOCALLY — never a full re-fetch per action.
     - "Back" walks this session's buffer; full history is on My Submissions.
   ────────────────────────────────────────────────────────────────────────── */
const API = '/recordings/validate/api';
const AUDIO_BASE = '/recordings/validate/audio';
const BATCH = 10;
const PREFETCH_AT_REMAINING = 3;   // 3 left in the window == sitting on the 8th of 10

let buffer = [];        // chunks loaded this session (pending + decided-this-session)
let frontier = 0;       // index of first not-yet-decided chunk
let pos = 0;            // currently viewed index
let pendingTotal = 0;   // live count of pending in the queue
let noMore = false;
let loading = false;
let inflight = null;
let started = false;

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

const key = (c) => `${c.owner_id}|${c.date}|${c.filename}`;

const editor = new ChunkEditor(host, {
  audioBase: AUDIO_BASE,
  issueLabel: 'Issue',
  onAccept: (text) => decide('/accept', 'verified', { transcription: text }),
  onIssue:  (text) => decide('/issue',  'issue',    { transcription: text }),
  onReject: ()     => decide('/reject', 'rejected', {}),
});

startBtn.onclick = start;
backBtn.onclick  = () => { if (pos > 0) { pos--; render(); } };
fwdBtn.onclick   = () => { const max = Math.min(frontier, buffer.length - 1); if (pos < max) { pos++; render(); } };

async function start() {
  if (started) return;
  started = true;
  startEl.style.display = 'none';
  contentEl.style.display = '';
  await fetchBatch();
  frontier = 0; pos = 0;
  render();
}

async function postJSON(url, body) {
  const res = await fetch(url, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  });
  if (!res.ok) { alert('Action failed (' + res.status + '). Please try again.'); throw new Error('request failed'); }
  return res.json();
}

function cursorParam() {
  if (buffer.length === 0) return '';
  const last = buffer[buffer.length - 1];   // buffer is globally sorted; last == max key
  return `&after_owner=${encodeURIComponent(last.owner_id)}`
       + `&after_date=${encodeURIComponent(last.date)}`
       + `&after_filename=${encodeURIComponent(last.filename)}`;
}

function fetchBatch() {
  if (loading || noMore) return inflight || Promise.resolve();
  loading = true;
  inflight = (async () => {
    try {
      const res = await fetch(`${API}/queue?limit=${BATCH}${cursorParam()}`);
      if (!res.ok) return;
      const json = await res.json();
      const items = json.items || [];
      if (json.pending_total != null) pendingTotal = json.pending_total;
      if (items.length === 0) { noMore = true; return; }
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

  await postJSON(API + endpoint, { owner_id: it.owner_id, date: it.date, filename: it.filename, ...body });

  it.status = newStatus;                                  // apply locally — no full refetch
  if (body.transcription !== undefined) it.verified_transcription = body.transcription;

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

// reloadEditor=false is used by background prefetch so it never reloads the
// current clip's audio or clobbers text the user is typing.
function render(reloadEditor = true) {
  if (buffer.length === 0) {
    progress.innerHTML = '';
    banner.innerHTML = emptyCard();
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
  banner.innerHTML = allDone ? doneCard() : '';
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
function doneCard() {
  return `<div class="state-card" style="padding:22px;margin-bottom:18px;">
    <span class="state-emoji">✅</span>
    <h3>All caught up</h3>
    <p>No more pending chunks. Use Back to revisit this session, or open
       <a href="/recordings/validate/submissions" style="color:var(--accent)">My Submissions</a>.</p></div>`;
}
