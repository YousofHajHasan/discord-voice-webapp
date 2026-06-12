/* ──────────────────────────────────────────────────────────────────────────
   Wallet page controller — the validator's earnings, withdraw action, CliQ
   payout alias, and withdrawal history. Earnings are computed server-side from
   the audio this user validated; this page only renders the wallet and lets
   them request a payout. Admins approve/reject from the admin page.

   Withdraw flow: click → POST /wallet/withdraw. If the server replies 428 the
   user has no CliQ alias yet, so we pop the alias modal and (on save) retry the
   withdrawal automatically. One pending withdrawal at a time (server-enforced).
   ────────────────────────────────────────────────────────────────────────── */
const API = '/recordings/validate/api';

const walletEl   = document.getElementById('wallet');
const aliasModal = document.getElementById('alias-modal');
const aliasInput = document.getElementById('alias-input');
const aliasMsg   = document.getElementById('alias-modal-msg');
const aliasTitle = document.getElementById('alias-modal-title');

let wallet = null;
let withdrawAfterAlias = false;   // true when the alias modal was opened mid-withdraw

const esc = (s) => String(s).replace(/[&<>"']/g, (c) =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

function fmtUsd(n) { return '$' + (Number(n) || 0).toFixed(2); }
function fmtHours(h) { const n = Number(h) || 0; return Number.isInteger(n) ? String(n) : n.toFixed(1); }

// Magnitude-driven single-unit duration (mirrors the Insights / admin views).
function fmtDur(seconds) {
  const s = Number(seconds) || 0;
  if (s < 100) return `${Math.round(s)}s`;
  if (s < 6000) return `${Math.round(s / 60)}m`;
  return `${Math.round(s / 3600)}h`;
}

// Stored timestamps are naive UTC ISO — treat as UTC, render in local time.
function fmtDate(iso) {
  if (!iso) return '—';
  const d = new Date(/[zZ]|[+-]\d\d:?\d\d$/.test(iso) ? iso : iso + 'Z');
  if (isNaN(d)) return '—';
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

async function getJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error('request failed (' + res.status + ')');
  return res.json();
}
async function postJSON(url, body) {
  const res = await fetch(url, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body || {}),
  });
  const json = await res.json().catch(() => ({}));
  return { ok: res.ok, status: res.status, json };
}

async function load() {
  try { wallet = await getJSON(API + '/wallet'); render(); }
  catch (e) { walletEl.innerHTML = `<div class="wallet-loading">Couldn't load your wallet. Reload to retry.</div>`; }
}

function statusBadge(s) {
  const cls = s === 'paid' ? 'paid' : s === 'rejected' ? 'rejected' : 'pending';
  const label = s === 'paid' ? 'Paid' : s === 'rejected' ? 'Rejected' : 'Pending';
  return `<span class="badge ${cls}">${label}</span>`;
}

function render() {
  const w = wallet;
  const avail = w.available_usd || 0;
  const min = w.min_withdrawal_usd || 0;
  const belowMin = !w.has_pending && !w.can_withdraw;
  const pct = (belowMin && min > 0) ? Math.min(100, Math.round((avail / min) * 100)) : 0;

  let withdrawCtl;
  if (w.has_pending) {
    withdrawCtl = `<button class="withdraw-btn" disabled>Withdraw</button>
      <span class="withdraw-hint warn">A withdrawal is pending an admin's approval.</span>`;
  } else if (w.can_withdraw) {
    withdrawCtl = `<button class="withdraw-btn" id="withdraw-btn">Withdraw ${fmtUsd(avail)}</button>
      <span class="withdraw-hint">Sent to your CliQ alias once an admin approves it.</span>`;
  } else {
    withdrawCtl = `<button class="withdraw-btn" disabled>Withdraw</button>
      <span class="withdraw-hint">Reach ${fmtUsd(min)} to withdraw — you have ${fmtUsd(avail)}.</span>`;
  }

  const aliasHtml = w.cliq_alias
    ? `<div class="alias-value">${esc(w.cliq_alias)}</div>`
    : `<div class="alias-value empty">Not set yet — you'll add it on your first withdrawal.</div>`;

  const txns = w.transactions || [];
  const txnHtml = txns.length ? `
    <div class="txn-wrap"><table class="txn-table">
      <thead><tr><th>Requested</th><th>Amount</th><th>To CliQ</th><th>Status</th><th>Decided</th></tr></thead>
      <tbody>
        ${txns.map((t) => `<tr>
          <td>${fmtDate(t.created_at)}</td>
          <td><span class="txn-amount">${fmtUsd(t.amount_usd)}</span></td>
          <td>${t.cliq_alias ? esc(t.cliq_alias) : '—'}</td>
          <td>${statusBadge(t.status)}</td>
          <td>${t.decided_at ? fmtDate(t.decided_at) : '—'}</td>
        </tr>`).join('')}
      </tbody>
    </table></div>` : `<div class="wallet-loading">No withdrawals yet.</div>`;

  walletEl.innerHTML = `
    <div class="wallet-hero">
      <div class="hero-label">Available to withdraw</div>
      <div class="hero-amount">${fmtUsd(avail)}</div>
      <div class="hero-sub">Rate ${fmtUsd(w.rate_usd)} / ${fmtHours(w.rate_hours)}h of validated audio · ${fmtDur(w.validated_seconds)} validated so far</div>
    </div>

    <div class="wallet-cards">
      <div class="wallet-card"><div class="num">${fmtUsd(w.earned_usd)}</div><div class="lbl">Earned (lifetime)</div></div>
      <div class="wallet-card"><div class="num">${fmtUsd(w.paid_usd)}</div><div class="lbl">Paid out</div></div>
      <div class="wallet-card"><div class="num">${fmtDur(w.validated_seconds)}</div><div class="lbl">Audio validated</div></div>
    </div>

    <div class="withdraw-row">${withdrawCtl}</div>
    ${belowMin ? `<div class="min-bar"><div class="min-fill" style="width:${pct}%"></div></div>` : ''}

    <div class="alias-card">
      <div>
        <div class="alias-label">CliQ payout alias</div>
        ${aliasHtml}
      </div>
      <button class="alias-edit" id="alias-edit-btn">${w.cliq_alias ? 'Edit' : 'Add alias'}</button>
    </div>

    <div class="section-title">Transactions</div>
    ${txnHtml}`;

  const wb = document.getElementById('withdraw-btn');
  if (wb) wb.onclick = doWithdraw;
  document.getElementById('alias-edit-btn').onclick = () => openAlias(false);
}

async function doWithdraw() {
  const wb = document.getElementById('withdraw-btn');
  if (wb) wb.disabled = true;
  const { ok, status, json } = await postJSON(API + '/wallet/withdraw', {});
  if (ok) { wallet = json.wallet || wallet; render(); return; }
  if (status === 428) {            // no alias on file — collect it, then retry
    openAlias(true);
    if (wb) wb.disabled = false;
    return;
  }
  alert(json.detail || 'Could not withdraw. Please try again.');
  load();   // refresh to reflect reality (e.g. dropped below the minimum)
}

// ── Alias modal ───────────────────────────────────────────────────────────────
function openAlias(thenWithdraw) {
  withdrawAfterAlias = !!thenWithdraw;
  aliasInput.value = (wallet && wallet.cliq_alias) || '';
  aliasMsg.textContent = '';
  aliasTitle.textContent = (wallet && wallet.cliq_alias) ? 'Edit your CliQ alias' : 'Add your CliQ alias';
  aliasModal.classList.add('open');
  setTimeout(() => aliasInput.focus(), 30);
}
function closeAlias() { aliasModal.classList.remove('open'); withdrawAfterAlias = false; }

async function saveAlias() {
  const val = aliasInput.value.trim();
  if (!val) { aliasMsg.textContent = 'Enter your CliQ alias.'; return; }
  const { ok, json } = await postJSON(API + '/wallet/alias', { cliq_alias: val });
  if (!ok) { aliasMsg.textContent = json.detail || 'Could not save. Try again.'; return; }
  wallet = json.wallet || wallet;
  const retry = withdrawAfterAlias;
  closeAlias();
  render();
  if (retry) doWithdraw();
}

document.getElementById('alias-save').onclick = saveAlias;
document.getElementById('alias-cancel').onclick = closeAlias;
aliasInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') saveAlias(); });
aliasModal.addEventListener('click', (e) => { if (e.target === aliasModal) closeAlias(); });
document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && aliasModal.classList.contains('open')) closeAlias(); });

load();
