/* ──────────────────────────────────────────────────────────────────────────
   Validate page controller — sequential one-chunk-at-a-time queue.

   Rules:
     - "frontier" = the first still-pending chunk (the one you must decide).
     - You cannot move forward past the frontier.
     - You can move Back through chunks you already accepted, edit + re-save,
       or reject them.
   The server is the source of truth: after every accept/reject we re-fetch
   state so local/remote can't drift.
   ────────────────────────────────────────────────────────────────────────── */
const API = '/recordings/validate/api';
const AUDIO_BASE = '/recordings/validate/audio';

let items = [];     // pending + verified, ordered owner->date->filename
let frontier = 0;   // index of first pending (== items.length when none left)
let pos = 0;        // currently viewed index

const host     = document.getElementById('editor-host');
const banner   = document.getElementById('banner');
const navEl    = document.getElementById('nav');
const backBtn  = document.getElementById('back');
const fwdBtn   = document.getElementById('fwd');
const counter  = document.getElementById('counter');
const progress = document.getElementById('progress');

async function decide(endpoint, body) {
  const it = items[pos];
  if (!it) return;
  const wasPending = it.status === 'pending';
  await postJSON(API + endpoint, { owner_id: it.owner_id, date: it.date, filename: it.filename, ...body });
  // issue/reject drop the item out of the queue (like reject); accept keeps it.
  await refresh(wasPending ? 'frontier' : 'stay');
}

const editor = new ChunkEditor(host, {
  audioBase: AUDIO_BASE,
  issueLabel: 'Issue',
  onAccept: (text) => decide('/accept', { transcription: text }),
  onIssue:  (text) => decide('/issue',  { transcription: text }),
  onReject: ()     => decide('/reject', {}),
});

backBtn.onclick = () => { if (pos > 0) { pos--; render(); } };
fwdBtn.onclick  = () => { const max = Math.min(frontier, items.length - 1); if (pos < max) { pos++; render(); } };

async function postJSON(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) { alert('Action failed (' + res.status + '). Please try again.'); throw new Error('request failed'); }
  return res.json();
}

async function refresh(jumpTo) {
  const res = await fetch(API + '/state');
  const json = await res.json();
  items = json.items || [];

  frontier = items.findIndex(i => i.status === 'pending');
  if (frontier === -1) frontier = items.length;

  if (jumpTo === 'frontier') pos = frontier;
  pos = Math.max(0, Math.min(pos, items.length - 1));
  render();
}

function render() {
  const verified = items.filter(i => i.status === 'verified').length;
  const pending  = items.length - verified;
  const pct = items.length ? Math.round(verified / items.length * 100) : 0;
  progress.innerHTML =
    `<div class="vp-bar"><div class="vp-fill" style="width:${pct}%"></div></div>
     <div class="vp-stats">${verified} verified · ${pending} pending</div>`;

  if (items.length === 0) {
    banner.innerHTML = emptyCard();
    host.style.display = 'none';
    navEl.style.display = 'none';
    return;
  }

  const allDone = frontier >= items.length;
  banner.innerHTML = allDone ? doneCard() : '';
  host.style.display = '';
  navEl.style.display = '';

  const it = items[pos];
  const reviewing = it.status === 'verified';
  editor.setActionLabels(reviewing ? 'Save' : 'Accept', 'Reject');
  editor.load(it);

  const max = Math.min(frontier, items.length - 1);
  backBtn.disabled = pos <= 0;
  fwdBtn.disabled  = pos >= max;
  counter.textContent = `${reviewing ? 'Reviewing' : 'Current'} · #${pos + 1} of ${items.length}`;
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
    <p>Every pending chunk is done. Use Back to revisit, or open
       <a href="/recordings/validate/submissions" style="color:var(--accent)">My Submissions</a>.</p></div>`;
}

refresh('frontier');
