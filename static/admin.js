/* ──────────────────────────────────────────────────────────────────────────
   Admin page controller — whole-dataset + per-contributor validation stats and
   the admin-list manager. Every endpoint it calls is admin-gated server-side
   (validate.py _require_admin); this script only renders what it's allowed.
   ────────────────────────────────────────────────────────────────────────── */
const API = '/recordings/validate/api';
const ME = window.CURRENT_USER_ID || '';

const overviewEl = document.getElementById('overview');
const addIdEl    = document.getElementById('admin-add-id');
const addBtnEl   = document.getElementById('admin-add-btn');
const msgEl      = document.getElementById('admin-msg');
const listEl     = document.getElementById('admin-list');

function esc(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;')
                  .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// Single-unit, magnitude-driven duration — identical to the Insights drawer so
// the two views read the same: <100s -> s, <100min -> m, else h.
function fmtDur(seconds) {
  const s = seconds || 0;
  if (s < 100) return `${Math.round(s)}s`;
  if (s < 6000) return `${Math.round(s / 60)}m`;
  return `${Math.round(s / 3600)}h`;
}

// Completion fraction by audio TIME once anything is measured, else by chunk
// COUNT (mirrors renderInsights — avoids a misleading 0% before the backfill).
function completion(vSec, rSec, vCount, rCount) {
  const totSec = vSec + rSec;
  if (totSec > 0) return Math.round((vSec / totSec) * 100);
  const totCount = vCount + rCount;
  return totCount > 0 ? Math.round((vCount / totCount) * 100) : 0;
}

async function getJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error('request failed (' + res.status + ')');
  return res.json();
}
async function postJSON(url, body) {
  const res = await fetch(url, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const json = await res.json().catch(() => ({}));
  return { ok: res.ok, json };
}

// ── Dataset overview (cards + split bar + per-contributor table) ─────────────
function renderOverview(d) {
  const t = d.totals || {};
  const vSec = t.verified_seconds || 0, rSec = t.remaining_seconds || 0;
  const vCount = t.verified_count || 0, rCount = t.remaining_count || 0;
  const totSec = vSec + rSec, totCount = vCount + rCount;
  const pct = completion(vSec, rSec, vCount, rCount);
  const vWidth = totSec > 0 ? (vSec / totSec) * 100
                            : (totCount > 0 ? (vCount / totCount) * 100 : 0);

  const note = t.remaining_unmeasured
    ? `<div class="a-note">Measuring audio length for ${t.remaining_unmeasured.toLocaleString()} more chunk${t.remaining_unmeasured === 1 ? '' : 's'} — the time totals will keep rising.</div>`
    : '';

  const cards = `
    <div class="stat-cards">
      <div class="stat-card"><div class="stat-num">${pct}<small>%</small></div><div class="stat-lbl">Verified</div></div>
      <div class="stat-card"><div class="stat-num">${fmtDur(vSec)}</div><div class="stat-lbl">Audio verified</div></div>
      <div class="stat-card"><div class="stat-num">${fmtDur(rSec)}</div><div class="stat-lbl">Audio remaining</div></div>
      <div class="stat-card"><div class="stat-num">${fmtDur(totSec)}</div><div class="stat-lbl">In-queue total</div></div>
    </div>`;

  const bar = `
    <div class="pbar">
      <div class="pbar-seg v" data-w="${vWidth.toFixed(1)}"></div>
      <div class="pbar-seg r" data-w="${(100 - vWidth).toFixed(1)}"></div>
    </div>
    <div class="pbar-legend">
      <span><i class="ins-dot v"></i>Verified · ${vCount.toLocaleString()} chunks</span>
      <span><i class="ins-dot r"></i>Remaining · ${rCount.toLocaleString()} chunks</span>
    </div>${note}`;

  const users = d.users || [];
  const rowFor = (u, isTotal) => {
    const p = completion(u.verified_seconds, u.remaining_seconds, u.verified_count, u.remaining_count);
    const named = u.name && u.name !== u.owner_id;
    const nameCell = isTotal
      ? `<span class="u-name">All contributors</span>`
      : (named ? `<span class="u-name">${esc(u.name)}</span>`
               : `<span class="u-id">${esc(u.owner_id)}</span>`);
    const pctCell = isTotal
      ? `<span class="u-pct">${p}%</span>`
      : `<span class="u-pct"><span class="u-mini"><i style="width:${p}%"></i></span>${p}%</span>`;
    return `<tr${isTotal ? ' class="total-row"' : ''}>
      <td>${nameCell}</td>
      <td><span class="u-num">${u.verified_count.toLocaleString()}</span></td>
      <td>${fmtDur(u.verified_seconds)}</td>
      <td><span class="u-num">${u.remaining_count.toLocaleString()}</span></td>
      <td>${fmtDur(u.remaining_seconds)}</td>
      <td>${pctCell}</td>
    </tr>`;
  };

  const table = users.length ? `
    <div class="section-title">Per contributor (${users.length})</div>
    <div class="tbl-wrap">
      <table class="u-table">
        <thead><tr>
          <th>Contributor</th><th>Verified</th><th>Audio</th><th>Remaining</th><th>Audio</th><th>Done</th>
        </tr></thead>
        <tbody>
          ${users.map((u) => rowFor(u, false)).join('')}
          ${rowFor({ verified_count: vCount, verified_seconds: vSec, remaining_count: rCount, remaining_seconds: rSec }, true)}
        </tbody>
      </table>
    </div>` : `<div class="a-loading">No chunks in the dataset yet.</div>`;

  overviewEl.innerHTML = cards + bar + table;
  // Animate the bar from empty to target on the next frame.
  requestAnimationFrame(() => {
    overviewEl.querySelectorAll('.pbar-seg').forEach((el) => { el.style.width = el.dataset.w + '%'; });
  });
}

async function loadOverview() {
  try { renderOverview(await getJSON(API + '/admin/stats')); }
  catch (e) { overviewEl.innerHTML = `<div class="a-loading">Couldn't load stats. Reload to retry.</div>`; }
}

// ── Admin-list management ────────────────────────────────────────────────────
function renderAdmins(list) {
  const single = list.length <= 1;   // the last admin can't be removed
  listEl.innerHTML = list.map((a) => {
    const named = a.name && a.name !== a.id;
    const you = a.id === ME ? `<span class="admin-you">you</span>` : '';
    return `<li class="admin-item">
      <div class="admin-meta">
        <span class="admin-name">${named ? esc(a.name) : esc(a.id)}${you}</span>
        ${named ? `<span class="admin-id">${esc(a.id)}</span>` : ''}
      </div>
      <button class="admin-remove" data-id="${esc(a.id)}"${single ? ' disabled title="Last remaining admin"' : ''}>Remove</button>
    </li>`;
  }).join('');
  listEl.querySelectorAll('.admin-remove').forEach((b) => {
    if (!b.disabled) b.onclick = () => removeAdmin(b.dataset.id);
  });
}

async function loadAdmins() {
  try { renderAdmins((await getJSON(API + '/admin/admins')).admins || []); }
  catch (e) { /* leave the list empty; the page still shows stats */ }
}

async function addAdmin() {
  const id = addIdEl.value.trim();
  if (!id) return;
  const { ok, json } = await postJSON(API + '/admin/admins', { discord_id: id });
  if (!ok) { showMsg(json.detail || 'Could not add admin.', false); return; }
  addIdEl.value = '';
  showMsg('Admin added.', true);
  renderAdmins(json.admins || []);
}

async function removeAdmin(id) {
  if (id === ME && !confirm('Remove yourself as admin? You will lose access to this page.')) return;
  const { ok, json } = await postJSON(API + '/admin/admins/remove', { discord_id: id });
  if (!ok) { showMsg(json.detail || 'Could not remove admin.', false); return; }
  if (id === ME) { window.location = '/recordings/validate/submissions'; return; }
  showMsg('Admin removed.', true);
  renderAdmins(json.admins || []);
}

function showMsg(msg, ok) {
  msgEl.textContent = msg;
  msgEl.className = 'manage-msg ' + (ok ? 'ok' : 'err');
}

addBtnEl.onclick = addAdmin;
addIdEl.onkeydown = (e) => { if (e.key === 'Enter') addAdmin(); };

loadOverview();
loadAdmins();
