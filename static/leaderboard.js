/* ──────────────────────────────────────────────────────────────────────────
   Leaderboard page controller — ranks validators by minutes of audio they
   validated (the same basis the Wallet pays on), across Today / This week /
   This month / All-time. The data is gated server-side to users who've validated
   at least one clip (any decision); everyone else gets {eligible:false} and sees the unlock
   state. Auto-refreshes while the tab is visible so "Today" feels live.
   ────────────────────────────────────────────────────────────────────────── */
const API = '/recordings/validate/api';
const ME = window.CURRENT_USER_ID || '';
const REFRESH_MS = 25000;
const DEFAULT_AVATAR = 'https://cdn.discordapp.com/embed/avatars/0.png';
const WINDOW_LABEL = { today: 'today', week: 'this week', month: 'this month', all: 'all-time' };

const boardEl = document.getElementById('board');
const tabsEl  = document.getElementById('lb-tabs');

let currentWindow = 'today';
let refreshTimer = null;

const esc = (s) => String(s).replace(/[&<>"']/g, (c) =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

// Score is seconds of audio; show minutes, rolling up to hours once it's big.
function fmtTime(seconds) {
  const m = (Number(seconds) || 0) / 60;
  if (m < 60) return `${Math.round(m)} min`;
  const h = Math.floor(m / 60), rem = Math.round(m % 60);
  return rem ? `${h}h ${rem}m` : `${h}h`;
}
function fmtChunks(n) { n = Number(n) || 0; return `${n.toLocaleString()} clip${n === 1 ? '' : 's'}`; }
const avatarOf = (e) => e.avatar || DEFAULT_AVATAR;

// 'YYYY-MM-DD' -> 'Jun 16' (the day a champion was crowned).
function fmtDate(iso) {
  const p = String(iso || '').split('-').map(Number);
  if (p.length !== 3 || !p[0]) return iso || '';
  const mon = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][p[1] - 1] || '';
  return `${mon} ${p[2]}`;
}

// Reigning champion banner — the most recent day that produced a winner. One winner
// takes the full bonus; an exact tie splits it (the server already divided the amount).
function championHtml(champ) {
  if (!champ || !champ.winners || !champ.winners.length) return '';
  const ws = champ.winners;
  const each = Number(ws[0].amount) || 0;
  const youWon = ws.some((w) => w.id === ME);
  const avas = ws.slice(0, 3).map((w) =>
    `<img class="ch-ava" src="${esc(w.avatar || DEFAULT_AVATAR)}" onerror="this.src='${DEFAULT_AVATAR}'" alt="">`).join('');
  const names = ws.map((w) => esc(w.name)).join(' & ');
  const prize = ws.length === 1 ? `+$${each.toFixed(2)}` : `split $${(each * ws.length).toFixed(2)}`;
  return `<div class="lb-champ${youWon ? ' you' : ''}">
    <div class="ch-crown">👑</div>
    <div class="ch-avas">${avas}</div>
    <div class="ch-txt">
      <div class="ch-title">${ws.length > 1 ? 'Daily champions' : 'Daily champion'} · ${fmtDate(champ.date)}</div>
      <div class="ch-name">${names}${youWon ? '<span class="you-tag">you</span>' : ''}</div>
    </div>
    <div class="ch-prize">${prize}</div>
  </div>`;
}

async function getJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error('request failed (' + res.status + ')');
  return res.json();
}

function lockedHtml() {
  return `<div class="lb-locked">
    <div class="ic">🔒</div>
    <h2>Validate one clip to unlock</h2>
    <p>The leaderboard opens up once you've validated your first clip.<br>Get on the board — and start earning.</p>
    <a class="lb-cta" href="/recordings/validate">Start validating →</a>
  </div>`;
}

function podiumHtml(top) {
  // Classic podium order: 2nd, 1st, 3rd (1st centered + elevated).
  const order = [top[1], top[0], top[2]];
  const cls = ['second', 'first', 'third'];
  const medal = ['🥈', '🥇', '🥉'];
  return `<div class="podium">` + order.map((e, i) => {
    if (!e) return '';
    const you = e.id === ME ? '<span class="you-tag">you</span>' : '';
    return `<div class="spot ${cls[i]}${e.id === ME ? ' you' : ''}">
      ${you}
      <div class="medal">${medal[i]}</div>
      <img class="ava" src="${esc(avatarOf(e))}" alt="" onerror="this.src='${DEFAULT_AVATAR}'">
      <div class="nm">${esc(e.name)}</div>
      <div class="sc">${fmtTime(e.seconds)}</div>
      <div class="cn">${fmtChunks(e.chunks)}</div>
    </div>`;
  }).join('') + `</div>`;
}

function rowHtml(e) {
  const you = e.id === ME;
  return `<div class="lb-row${you ? ' you' : ''}">
    <div class="lb-rank">${e.rank}</div>
    <img class="lb-ava" src="${esc(avatarOf(e))}" alt="" onerror="this.src='${DEFAULT_AVATAR}'">
    <div class="lb-name">${esc(e.name)}${you ? '<span class="me-tag">you</span>' : ''}</div>
    <div class="lb-score">${fmtTime(e.seconds)}<span class="cn">${fmtChunks(e.chunks)}</span></div>
  </div>`;
}

// Your sticky position callout — your rank, or a nudge if you're not on the board
// for this window yet.
function youBarHtml(data) {
  const me = data.me;
  const label = WINDOW_LABEL[data.window] || 'this window';
  if (!me) {
    return `<div class="you-bar"><span class="yr">—</span>
      <span>You haven't validated ${label} yet. Validate a clip to claim your spot!</span></div>`;
  }
  const ahead = data.entries.find((e) => e.rank === me.rank - 1);
  const gap = ahead ? Math.max(0, ahead.seconds - me.seconds) : 0;
  const chase = (ahead && gap > 0)
    ? ` · ${fmtTime(gap)} behind ${esc(ahead.name)}`
    : (me.rank === 1 ? ' · you\'re on top 🔥' : '');
  return `<div class="you-bar"><span class="yr">#${me.rank}</span>
    <span>You · ${fmtTime(me.seconds)} ${label}${chase}</span></div>`;
}

function render(data) {
  if (data.eligible === false) { boardEl.innerHTML = lockedHtml(); return; }

  const champ = championHtml(data.champion);
  const entries = data.entries || [];
  if (!entries.length) {
    boardEl.innerHTML = champ
      + `<div class="lb-empty">No one has validated ${WINDOW_LABEL[data.window] || 'yet'} — be the first on the board! 🚀</div>`
      + youBarHtml(data);
    return;
  }

  const total = `<div class="lb-total">Together · <b>${fmtTime(data.total_seconds)}</b> of audio across
    <b>${data.participants}</b> validator${data.participants === 1 ? '' : 's'} ${WINDOW_LABEL[data.window] || ''}.</div>`;

  const top = entries.slice(0, 3);
  const rest = entries.slice(3);
  const podium = podiumHtml(top);
  const list = rest.length ? `<div class="lb-list">${rest.map(rowHtml).join('')}</div>` : '';

  boardEl.innerHTML = champ + total + podium + list + youBarHtml(data);
}

async function load(win) {
  try {
    const data = await getJSON(API + '/leaderboard?window=' + encodeURIComponent(win));
    if (win === currentWindow) render(data);   // ignore stale responses after a fast tab switch
  } catch (e) {
    if (win === currentWindow) boardEl.innerHTML = `<div class="lb-loading">Couldn't load the leaderboard. Reload to retry.</div>`;
  }
}

tabsEl.querySelectorAll('.lb-tab').forEach((btn) => {
  btn.onclick = () => {
    if (btn.classList.contains('active')) return;
    tabsEl.querySelectorAll('.lb-tab').forEach((b) => b.classList.toggle('active', b === btn));
    currentWindow = btn.dataset.window;
    boardEl.innerHTML = `<div class="lb-loading">Loading the standings…</div>`;
    load(currentWindow);
  };
});

// Live-ish: refresh while the page is visible; pause when hidden to save calls.
function startRefresh() { stopRefresh(); refreshTimer = setInterval(() => load(currentWindow), REFRESH_MS); }
function stopRefresh()  { if (refreshTimer) { clearInterval(refreshTimer); refreshTimer = null; } }
document.addEventListener('visibilitychange', () => {
  if (document.hidden) stopRefresh();
  else { load(currentWindow); startRefresh(); }
});

load(currentWindow);
startRefresh();
