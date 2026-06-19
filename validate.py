"""
Validation feature — self-contained APIRouter.

Mounted from main.py via app.include_router(router). Owns:
  - the /validate sequential page and /validate/submissions management page
  - JSON endpoints the page controllers call (state, submissions, accept, reject)
  - a grant-aware audio stream (separate from the dashboard's serve_chunk so the
    existing dashboard behaviour is untouched)

Routes are defined without the /recordings prefix; nginx + the app's root_path
handle that, exactly like the routes in main.py.
"""
import os
import re

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates

import validation_db as vdb
import discord_bot

router = APIRouter()
templates = Jinja2Templates(directory="templates")

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


@router.get("/static/{filename:path}")
async def static_files(filename: str):
    """
    Serve shared CSS/JS from the static/ dir via a plain route (not a
    StaticFiles mount, which breaks under the app's root_path prefix).
    normpath + prefix check guards against path traversal.
    """
    safe = os.path.normpath(os.path.join(STATIC_DIR, filename))
    if not safe.startswith(STATIC_DIR + os.sep) or not os.path.isfile(safe):
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(safe)


def get_current_user(request: Request):
    # Defined locally (not imported from main) to avoid a circular import.
    return request.session.get("user")


def _require_user(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return user


def _require_admin(request: Request):
    """Gate an admin-only route: 401 if logged out, 403 if not an admin. The admin
    list lives in the DB (validation_db.is_admin) so it needs no redeploy, and this
    server-side check is the real gate — hiding the button in the template is only
    cosmetic."""
    user = _require_user(request)
    if not vdb.is_admin(user["id"]):
        raise HTTPException(status_code=403, detail="Admins only")
    return user


def _safe(*parts):
    """Reject path-traversal in URL/body segments."""
    for p in parts:
        if not p or ".." in p or "/" in p:
            raise HTTPException(status_code=400, detail="Invalid path")


# ── Pages ─────────────────────────────────────────────────────────────────────

@router.get("/validate", response_class=HTMLResponse)
async def validate_page(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/recordings/")
    return templates.TemplateResponse(
        "validate.html",
        {"request": request, "user": user, "is_admin": vdb.is_admin(user["id"])},
    )


@router.get("/validate/submissions", response_class=HTMLResponse)
async def submissions_page(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/recordings/")
    return templates.TemplateResponse(
        "submissions.html",
        {"request": request, "user": user, "is_admin": vdb.is_admin(user["id"])},
    )


@router.get("/validate/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    """Admin-only dataset stats + admin management. Logged-in non-admins are
    bounced to their own submissions; the gate is enforced again on every
    /validate/api/admin/* call."""
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/recordings/")
    if not vdb.is_admin(user["id"]):
        return RedirectResponse("/recordings/validate/submissions")
    return templates.TemplateResponse(
        "admin.html", {"request": request, "user": user, "is_admin": True}
    )


@router.get("/validate/admin/transcripts", response_class=HTMLResponse)
async def transcripts_fix_page(request: Request):
    """Admin-only transcript correction tool: paste chunk paths, listen, and edit
    each clip's verified_transcription directly — a pure text edit, NOT a
    (re-)validation (see vdb.admin_set_verified_transcription)."""
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/recordings/")
    if not vdb.is_admin(user["id"]):
        return RedirectResponse("/recordings/validate/submissions")
    return templates.TemplateResponse(
        "transcripts.html", {"request": request, "user": user, "is_admin": True}
    )


@router.get("/validate/wallet", response_class=HTMLResponse)
async def wallet_page(request: Request):
    """The validator's earnings + withdrawal page. Any logged-in user; every
    number comes from /validate/api/wallet, and admins approve the payouts from
    the separate admin page."""
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/recordings/")
    return templates.TemplateResponse(
        "wallet.html",
        {"request": request, "user": user, "is_admin": vdb.is_admin(user["id"])},
    )


@router.get("/validate/leaderboard", response_class=HTMLResponse)
async def leaderboard_page(request: Request):
    """Validator competition board. Any logged-in user can open the page; the data
    itself is gated server-side to users who've verified at least one clip (see
    /validate/api/leaderboard) — others see an 'unlock by validating' state."""
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/recordings/")
    return templates.TemplateResponse(
        "leaderboard.html",
        {"request": request, "user": user, "is_admin": vdb.is_admin(user["id"])},
    )


# ── JSON API ──────────────────────────────────────────────────────────────────

@router.get("/validate/api/queue")
async def api_queue(request: Request, limit: int = 10, owner: str = ""):
    """
    Lease + return the next window of pending chunks (default 10) for ONE owner,
    plus that owner's total pending. `owner` defaults to the viewer themselves;
    pass another id (from /owners) to validate someone who granted you access.

    Each call leases its chunks to the caller, so two validators on the same
    owner get DIFFERENT chunks (see vdb.claim_pending_window). Just call it again
    to get the next batch — chunks you already hold aren't re-sent.
    """
    user = _require_user(request)
    owner_id = owner or user["id"]
    _safe(owner_id)
    items, total = vdb.claim_pending_window(user["id"], owner_id, limit)
    return {"viewer_id": user["id"], "owner_id": owner_id, "items": items, "pending_total": total}


@router.get("/validate/api/owners")
async def api_owners(request: Request):
    """Owners the viewer may validate (themselves + anyone who granted them) — for the dropdown."""
    user = _require_user(request)
    return {"viewer_id": user["id"], "owners": vdb.get_accessible_owners(user["id"])}


@router.get("/validate/api/delegates")
async def api_delegates(request: Request):
    """People the viewer has granted validate-access to — for the manage panel."""
    user = _require_user(request)
    return {"delegates": vdb.get_delegates(user["id"])}


@router.post("/validate/api/grant")
async def api_grant(request: Request):
    user = _require_user(request)
    body = await request.json()
    delegate_id = str(body.get("delegate_id", "")).strip()
    if not vdb.grant_access(user["id"], delegate_id):
        raise HTTPException(status_code=400, detail="Enter a valid Discord user ID (numbers only, not yourself).")
    return {"ok": True, "delegates": vdb.get_delegates(user["id"])}


@router.post("/validate/api/revoke")
async def api_revoke(request: Request):
    user = _require_user(request)
    body = await request.json()
    delegate_id = str(body.get("delegate_id", "")).strip()
    vdb.revoke_access(user["id"], delegate_id)
    return {"ok": True, "delegates": vdb.get_delegates(user["id"])}


@router.post("/validate/api/release")
async def api_release(request: Request):
    """
    Free the viewer's un-decided leases (on owner-switch / page leave) so chunks
    return to the pool immediately instead of waiting out the 15-min lease.
    Tolerant of an empty/garbage body since it's also called via sendBeacon.
    """
    user = _require_user(request)
    owner = ""
    try:
        body = await request.json()
        owner = str(body.get("owner", "")).strip()
    except Exception:
        pass
    vdb.release_my_claims(user["id"], owner or None)
    return {"ok": True}


@router.get("/validate/api/submissions")
async def api_submissions(request: Request):
    user = _require_user(request)
    return {"viewer_id": user["id"], "items": vdb.get_submissions(user["id"])}


@router.get("/validate/api/insights")
async def api_insights(request: Request, owner: str = ""):
    """
    Per-owner validation totals for the popup. `owner` defaults to the viewer's
    own voices; pass another id (from /owners) to see a user who granted you
    access. Access is enforced — you can only read owners you may validate.
    """
    user = _require_user(request)
    owner_id = owner or user["id"]
    _safe(owner_id)
    if not vdb.can_access(user["id"], owner_id):
        raise HTTPException(status_code=403, detail="No access to this owner")
    return vdb.get_insights(owner_id)


@router.get("/validate/api/labels")
async def api_labels(request: Request):
    """The content-classification taxonomy (key + display label + description) the
    validator picks from. Single source of truth: validation_db.LABELS."""
    _require_user(request)
    return {"labels": vdb.LABELS}


# ── Wallet / earnings / withdrawals ───────────────────────────────────────────

@router.get("/validate/api/wallet")
async def api_wallet(request: Request):
    """Live earnings, available balance, CliQ alias, and withdrawal history for the
    logged-in validator — computed from the audio they personally validated."""
    user = _require_user(request)
    return vdb.get_wallet(user["id"])


@router.post("/validate/api/wallet/alias")
async def api_wallet_alias(request: Request):
    """Set / update the viewer's CliQ payout alias (captured on the first
    withdrawal, editable any time from the Wallet page)."""
    user = _require_user(request)
    body = await request.json()
    alias = vdb.set_cliq_alias(user["id"], str(body.get("cliq_alias", "")))
    if alias is None:
        raise HTTPException(status_code=400, detail="Enter a valid CliQ alias.")
    return {"ok": True, "cliq_alias": alias, "wallet": vdb.get_wallet(user["id"])}


@router.post("/validate/api/wallet/withdraw")
async def api_wallet_withdraw(request: Request):
    """Request a payout of the full available balance. A 428 means 'set your CliQ
    alias first' so the client can prompt for it and retry."""
    user = _require_user(request)
    result, wallet = vdb.request_withdrawal(user["id"])
    if result == "ok":
        return JSONResponse({"ok": True, "wallet": wallet})
    if result == "no_alias":
        raise HTTPException(status_code=428, detail="Add your CliQ alias to withdraw.")
    if result == "below_min":
        raise HTTPException(
            status_code=400,
            detail=f"You need at least ${wallet['min_withdrawal_usd']:.2f} available to withdraw.",
        )
    if result == "has_pending":
        raise HTTPException(status_code=409, detail="You already have a withdrawal awaiting approval.")
    raise HTTPException(status_code=400, detail="Could not create the withdrawal.")


@router.get("/validate/api/leaderboard")
async def api_leaderboard(request: Request):
    """Ranked validators for a time window (today|week|month|all). Gated to users
    who've validated at least one clip (any decision) — others get {eligible:false}
    with no data, so a logged-in stranger can't see everyone's stats."""
    user = _require_user(request)
    window = request.query_params.get("window", "today")
    if not vdb.has_validated(user["id"]):
        return {"eligible": False, "window": window}
    data = vdb.get_leaderboard(window, user["id"])
    data["eligible"] = True
    return data


# ── Admin: dataset stats + admin-list management ──────────────────────────────
# Every route here is gated by _require_admin (403 for non-admins); the
# template-level hiding of the Admin button/nav is only cosmetic.

@router.get("/validate/api/admin/stats")
async def api_admin_stats(request: Request):
    """Whole-dataset + per-owner verified/remaining counts and audio totals."""
    _require_admin(request)
    return vdb.get_dataset_stats()


@router.post("/validate/api/admin/transcripts/lookup")
async def api_admin_transcripts_lookup(request: Request):
    """Resolve a pasted list of chunk paths to their current rows (order kept).
    Read-only — surfaces each clip's verified_transcription for editing."""
    _require_admin(request)
    body = await request.json()
    raw = body.get("paths")
    if isinstance(raw, str):
        raw = raw.splitlines()
    if not isinstance(raw, list):
        raise HTTPException(status_code=400, detail="Send `paths` as a list or newline-separated string.")
    return {"items": vdb.lookup_chunks_for_fix(raw)}


@router.post("/validate/api/admin/transcripts/save")
async def api_admin_transcripts_save(request: Request):
    """Overwrite ONE chunk's verified_transcription. Pure text edit: no status,
    validator, duration, wallet, or leaderboard side effects."""
    _require_admin(request)
    body = await request.json()
    owner_id = str(body.get("owner_id", "")).strip()
    date = str(body.get("date", "")).strip()
    filename = str(body.get("filename", "")).strip()
    if not (owner_id and date and filename):
        raise HTTPException(status_code=400, detail="owner_id, date and filename are required.")
    _safe(owner_id, date, filename)
    # `text` may be any string, including empty (an explicit clear). Reject only a
    # missing field so an accidental empty body can't blank a transcript.
    if "text" not in body:
        raise HTTPException(status_code=400, detail="`text` is required.")
    result = vdb.admin_set_verified_transcription(owner_id, date, filename, str(body.get("text") or ""))
    if result == "notfound":
        raise HTTPException(status_code=404, detail="No such chunk.")
    return {"ok": True}


@router.get("/validate/api/admin/admins")
async def api_admin_list(request: Request):
    _require_admin(request)
    return {"admins": vdb.list_admins()}


@router.post("/validate/api/admin/admins")
async def api_admin_add(request: Request):
    user = _require_admin(request)
    body = await request.json()
    new_id = str(body.get("discord_id", "")).strip()
    if not vdb.add_admin(user["id"], new_id):
        raise HTTPException(status_code=400, detail="Enter a valid Discord user ID (numbers only).")
    return {"ok": True, "admins": vdb.list_admins()}


@router.post("/validate/api/admin/admins/remove")
async def api_admin_remove(request: Request):
    _require_admin(request)
    body = await request.json()
    target_id = str(body.get("discord_id", "")).strip()
    result = vdb.remove_admin(target_id)
    if result == "last":
        raise HTTPException(status_code=400, detail="Can't remove the last admin.")
    if result == "notfound":
        raise HTTPException(status_code=404, detail="That user isn't an admin.")
    return {"ok": True, "admins": vdb.list_admins()}


# ── Admin: payout approval ────────────────────────────────────────────────────
# Same _require_admin gate as the stats/admin-list routes. The user-facing wallet
# only CREATES pending withdrawals; flipping them to paid/rejected happens here.

@router.get("/validate/api/admin/payouts")
async def api_admin_payouts(request: Request):
    """Pending withdrawals to act on, plus recent decided ones (history)."""
    _require_admin(request)
    return vdb.list_payouts()


def _withdrawal_id(body) -> int:
    try:
        return int(body.get("id"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid withdrawal id")


def _payout_result(result: str):
    if result == "ok":
        return JSONResponse({"ok": True})
    if result == "notfound":
        raise HTTPException(status_code=404, detail="Withdrawal not found.")
    if result == "notpending":
        raise HTTPException(status_code=409, detail="That withdrawal was already decided.")
    raise HTTPException(status_code=400, detail="Could not update the withdrawal.")


@router.post("/validate/api/admin/payouts/approve")
async def api_admin_payout_approve(request: Request):
    """Mark a pending withdrawal PAID — do this after actually sending the money."""
    admin = _require_admin(request)
    body = await request.json()
    return _payout_result(vdb.approve_payout(admin["id"], _withdrawal_id(body)))


@router.post("/validate/api/admin/payouts/reject")
async def api_admin_payout_reject(request: Request):
    """Reject a pending withdrawal; its amount returns to the user's available."""
    admin = _require_admin(request)
    body = await request.json()
    note = str(body.get("note", "")).strip() or None
    return _payout_result(vdb.reject_payout(admin["id"], _withdrawal_id(body), note))


# ── Admin: broadcast a DM to users ────────────────────────────────────────────
# Same _require_admin gate. Sending runs in a background task (discord_bot); these
# routes just kick it off and report progress. Needs DISCORD_BOT_TOKEN in the env
# and the bot to share a server with each recipient (see discord_bot.py).

MAX_BROADCAST_CHARS = 2000   # Discord's hard limit on a single message's content


def _parse_ids(raw) -> list:
    """Pull Discord snowflakes out of free text — commas, spaces, or newlines all
    work, so the admin can paste a list. Keeps pure-digit tokens 15–20 long."""
    return [tok for tok in re.split(r"[^\d]+", str(raw or "")) if 15 <= len(tok) <= 20]


@router.get("/validate/api/admin/broadcast/recipients")
async def api_admin_broadcast_recipients(request: Request):
    """The default recipient list (all web-app users) + whether messaging is wired
    up (bot token present), so the UI can disable the composer if not."""
    _require_admin(request)
    return {"users": vdb.list_all_users(), "bot_ready": discord_bot.is_configured()}


@router.post("/validate/api/admin/broadcast")
async def api_admin_broadcast(request: Request):
    """Start a broadcast to the selected users ∪ any pasted raw IDs. Returns a
    job_id the client polls for live progress. 409 if one is already running."""
    admin = _require_admin(request)
    if not discord_bot.is_configured():
        raise HTTPException(status_code=503, detail="Messaging isn't configured (set DISCORD_BOT_TOKEN).")

    body = await request.json()
    message = (body.get("message") or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Enter a message to send.")
    if len(message) > MAX_BROADCAST_CHARS:
        raise HTTPException(status_code=400, detail=f"Message is too long (max {MAX_BROADCAST_CHARS} characters).")

    selected = [str(x) for x in (body.get("user_ids") or [])]
    extra = _parse_ids(body.get("extra_ids"))
    # Dedupe, preserving order (selected first, then pasted extras).
    seen, recipient_ids = set(), []
    for i in selected + extra:
        if i and i not in seen:
            seen.add(i)
            recipient_ids.append(i)
    if not recipient_ids:
        raise HTTPException(status_code=400, detail="Pick at least one recipient.")

    names = vdb.usernames_for(recipient_ids)
    recipients = [{"id": i, "name": names.get(i)} for i in recipient_ids]

    job_id = discord_bot.start_broadcast(admin["id"], message, recipients)
    if job_id is None:
        raise HTTPException(status_code=409, detail="A broadcast is already in progress.")
    return {"ok": True, "job_id": job_id, "total": len(recipients)}


@router.get("/validate/api/admin/broadcast/status")
async def api_admin_broadcast_status(request: Request):
    """Live progress (status, counts, per-recipient results) for a running or
    finished broadcast."""
    _require_admin(request)
    try:
        job_id = int(request.query_params.get("job_id"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid job id")
    status = discord_bot.job_status(job_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Broadcast not found")
    return status


@router.get("/validate/api/admin/broadcasts")
async def api_admin_broadcasts(request: Request):
    """Recent broadcast history for the admin panel."""
    _require_admin(request)
    return {"broadcasts": vdb.list_broadcasts()}


def _decide_response(result: str, ok_status: str):
    """Map a vdb decision result -> HTTP response. 409 lets the client skip a
    chunk another validator already decided, without an error popup."""
    if result == "ok":
        return JSONResponse({"ok": True, "status": ok_status})
    if result == "conflict":
        raise HTTPException(status_code=409, detail="This chunk was already validated by someone else.")
    if result == "nolabels":
        raise HTTPException(status_code=400, detail="Select at least one label — and 'Normal' must be on its own.")
    raise HTTPException(status_code=404, detail="Chunk not found or access denied")


@router.post("/validate/api/accept")
async def api_accept(request: Request):
    user = _require_user(request)
    body = await request.json()
    owner_id = str(body.get("owner_id", ""))
    date = str(body.get("date", ""))
    filename = str(body.get("filename", ""))
    _safe(owner_id, date, filename)
    text = (body.get("transcription") or "").strip()
    labels = body.get("labels")
    return _decide_response(vdb.accept_chunk(user["id"], owner_id, date, filename, text, labels), "verified")


@router.post("/validate/api/reject")
async def api_reject(request: Request):
    user = _require_user(request)
    body = await request.json()
    owner_id = str(body.get("owner_id", ""))
    date = str(body.get("date", ""))
    filename = str(body.get("filename", ""))
    _safe(owner_id, date, filename)
    return _decide_response(vdb.reject_chunk(user["id"], owner_id, date, filename), "rejected")


@router.post("/validate/api/issue")
async def api_issue(request: Request):
    user = _require_user(request)
    body = await request.json()
    owner_id = str(body.get("owner_id", ""))
    date = str(body.get("date", ""))
    filename = str(body.get("filename", ""))
    _safe(owner_id, date, filename)
    text = (body.get("transcription") or "").strip()
    return _decide_response(vdb.issue_chunk(user["id"], owner_id, date, filename, text), "issue")


@router.post("/validate/api/skip")
async def api_skip(request: Request):
    """Pass on a chunk: it's not validated (stays pending for others), released to
    the pool immediately, and never re-served to this viewer."""
    user = _require_user(request)
    body = await request.json()
    owner_id = str(body.get("owner_id", ""))
    date = str(body.get("date", ""))
    filename = str(body.get("filename", ""))
    _safe(owner_id, date, filename)
    result = vdb.skip_chunk(user["id"], owner_id, date, filename)
    if result == "ok":
        return JSONResponse({"ok": True})
    if result == "denied":
        raise HTTPException(status_code=403, detail="No access to this owner")
    raise HTTPException(status_code=404, detail="Chunk not found")


@router.post("/validate/api/trim_accept")
async def api_trim_accept(request: Request):
    """
    One-step "Trim & Accept": cut dead air / noise off the head and/or tail of a
    chunk and accept it in a single action. Writes "{stem}_updated.wav",
    soft-deletes the original, and returns the new VERIFIED chunk so the client
    can swap it into its buffer. `start`/`end` are the kept window in seconds.
    """
    user = _require_user(request)
    body = await request.json()
    owner_id = str(body.get("owner_id", ""))
    date = str(body.get("date", ""))
    filename = str(body.get("filename", ""))
    _safe(owner_id, date, filename)
    try:
        start = float(body.get("start", 0))
        end = float(body.get("end", 0))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid trim range")
    text = (body.get("transcription") or "").strip()
    labels = body.get("labels")

    result, chunk = vdb.trim_accept_chunk(user["id"], owner_id, date, filename, start, end, text, labels)
    if result == "ok":
        return JSONResponse({"ok": True, "status": "verified", "chunk": chunk})
    if result == "conflict":
        raise HTTPException(status_code=409, detail="This chunk was already validated by someone else.")
    if result == "nolabels":
        raise HTTPException(status_code=400, detail="Select at least one label — and 'Normal' must be on its own.")
    if result == "denied":
        raise HTTPException(status_code=404, detail="Chunk not found or access denied")
    raise HTTPException(status_code=404, detail="Chunk not found, missing audio, or empty trim range")


# ── Grant-aware audio stream ──────────────────────────────────────────────────

@router.get("/validate/audio/{owner_id}/{date}/{filename}")
async def validate_audio(owner_id: str, date: str, filename: str, request: Request):
    user = _require_user(request)
    _safe(owner_id, date, filename)

    path = vdb.resolve_chunk_file(user["id"], owner_id, date, filename)
    if not path:
        raise HTTPException(status_code=404, detail="Chunk not found")

    file_size = os.path.getsize(path)
    range_header = request.headers.get("range")

    def iter_file(start=0, end=None):
        chunk_size = 1024 * 256
        with open(path, "rb") as f:
            f.seek(start)
            remaining = (end - start + 1) if end is not None else None
            while True:
                to_read = min(chunk_size, remaining) if remaining is not None else chunk_size
                data = f.read(to_read)
                if not data:
                    break
                if remaining is not None:
                    remaining -= len(data)
                yield data
                if remaining is not None and remaining <= 0:
                    break

    if range_header:
        rng = range_header.replace("bytes=", "")
        start_str, end_str = rng.split("-")
        start = int(start_str)
        end = int(end_str) if end_str else file_size - 1
        headers = {
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(end - start + 1),
            "Content-Type": "audio/wav",
        }
        return StreamingResponse(iter_file(start, end), status_code=206, headers=headers)

    return StreamingResponse(
        iter_file(),
        media_type="audio/wav",
        headers={"Accept-Ranges": "bytes", "Content-Length": str(file_size)},
    )
