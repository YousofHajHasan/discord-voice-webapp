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

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates

import validation_db as vdb

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
    return templates.TemplateResponse("validate.html", {"request": request, "user": user})


@router.get("/validate/submissions", response_class=HTMLResponse)
async def submissions_page(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/recordings/")
    return templates.TemplateResponse("submissions.html", {"request": request, "user": user})


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


def _decide_response(result: str, ok_status: str):
    """Map a vdb decision result -> HTTP response. 409 lets the client skip a
    chunk another validator already decided, without an error popup."""
    if result == "ok":
        return JSONResponse({"ok": True, "status": ok_status})
    if result == "conflict":
        raise HTTPException(status_code=409, detail="This chunk was already validated by someone else.")
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
    return _decide_response(vdb.accept_chunk(user["id"], owner_id, date, filename, text), "verified")


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

    result, chunk = vdb.trim_accept_chunk(user["id"], owner_id, date, filename, start, end, text)
    if result == "ok":
        return JSONResponse({"ok": True, "status": "verified", "chunk": chunk})
    if result == "conflict":
        raise HTTPException(status_code=409, detail="This chunk was already validated by someone else.")
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
