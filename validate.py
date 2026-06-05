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

@router.get("/validate/api/state")
async def api_state(request: Request):
    user = _require_user(request)
    return {"viewer_id": user["id"], "items": vdb.get_validation_state(user["id"])}


@router.get("/validate/api/submissions")
async def api_submissions(request: Request):
    user = _require_user(request)
    return {"viewer_id": user["id"], "items": vdb.get_submissions(user["id"])}


@router.post("/validate/api/accept")
async def api_accept(request: Request):
    user = _require_user(request)
    body = await request.json()
    owner_id = str(body.get("owner_id", ""))
    date = str(body.get("date", ""))
    filename = str(body.get("filename", ""))
    _safe(owner_id, date, filename)
    text = (body.get("transcription") or "").strip()
    if not vdb.accept_chunk(user["id"], owner_id, date, filename, text):
        raise HTTPException(status_code=404, detail="Chunk not found or access denied")
    return JSONResponse({"ok": True, "status": "verified"})


@router.post("/validate/api/reject")
async def api_reject(request: Request):
    user = _require_user(request)
    body = await request.json()
    owner_id = str(body.get("owner_id", ""))
    date = str(body.get("date", ""))
    filename = str(body.get("filename", ""))
    _safe(owner_id, date, filename)
    if not vdb.reject_chunk(user["id"], owner_id, date, filename):
        raise HTTPException(status_code=404, detail="Chunk not found or access denied")
    return JSONResponse({"ok": True, "status": "rejected"})


@router.post("/validate/api/issue")
async def api_issue(request: Request):
    user = _require_user(request)
    body = await request.json()
    owner_id = str(body.get("owner_id", ""))
    date = str(body.get("date", ""))
    filename = str(body.get("filename", ""))
    _safe(owner_id, date, filename)
    text = (body.get("transcription") or "").strip()
    if not vdb.issue_chunk(user["id"], owner_id, date, filename, text):
        raise HTTPException(status_code=404, detail="Chunk not found or access denied")
    return JSONResponse({"ok": True, "status": "issue"})


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
