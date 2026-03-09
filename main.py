import os
import threading
import time
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from starlette.middleware.sessions import SessionMiddleware
from pathlib import Path

from auth import get_discord_oauth_url, exchange_code, get_discord_user
from database import (
    init_db, upsert_user, log_audio_file,
    register_chunk, get_chunks_for_user, delete_chunk,
    get_pending_chunks, claim_chunks, set_transcription, release_stale_claims,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(root_path="/recordings")
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")

app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ["SESSION_SECRET"],
    https_only=True,
    same_site="lax",
    max_age=604800  # 7 days
)

templates = Jinja2Templates(directory="templates")

RECORDINGS_PATH = Path(os.environ.get("RECORDINGS_PATH", "/app/recordings"))
TRANSCRIPTION_API_KEY = os.environ.get("TRANSCRIPTION_API_KEY", "")

CHUNK_SCAN_INTERVAL = 35  # seconds — slightly after VAD's 30s cycle


def _require_api_key(request: Request):
    """Validates X-API-Key header for script-facing endpoints."""
    if not TRANSCRIPTION_API_KEY:
        raise HTTPException(status_code=500, detail="TRANSCRIPTION_API_KEY not configured on server")
    key = request.headers.get("X-API-Key", "")
    if key != TRANSCRIPTION_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


def _scan_and_register_all_chunks():
    """
    Background thread: scans every user's chunks/ folder and registers any
    new .wav files into the DB. Runs every CHUNK_SCAN_INTERVAL seconds.
    This is the ONLY place disk is scanned after startup — the API poll
    endpoints just read from the DB.
    """
    while True:
        time.sleep(CHUNK_SCAN_INTERVAL)
        try:
            if not RECORDINGS_PATH.exists():
                continue
            for user_dir in RECORDINGS_PATH.iterdir():
                if not user_dir.is_dir():
                    continue
                discord_id = user_dir.name
                if not discord_id.isdigit():
                    continue  # skip anything that's not a pure numeric ID folder
                chunks_root = user_dir / "chunks"
                if not chunks_root.exists():
                    continue
                for date_dir in chunks_root.iterdir():
                    if date_dir.is_dir():
                        for wav in date_dir.glob("chunk_*.wav"):
                            register_chunk(discord_id, date_dir.name, wav.name, str(wav))

            # Release any claims that have been held for too long
            freed = release_stale_claims()
            if freed:
                logger.info(f"Background: released {freed} stale chunk claim(s)")
        except Exception as e:
            logger.error(f"Background chunk scan error: {e}")


@app.on_event("startup")
async def startup():
    init_db()
    if RECORDINGS_PATH.exists():
        for user_dir in RECORDINGS_PATH.iterdir():
            if not user_dir.is_dir():
                continue
            discord_id = user_dir.name
            if not discord_id.isdigit():
                continue  # skip anything that's not a pure numeric ID folder

            # Register processed VAD chunks
            chunks_root = user_dir / "chunks"
            if chunks_root.exists():
                for date_dir in chunks_root.iterdir():
                    if date_dir.is_dir():
                        for wav in date_dir.glob("chunk_*.wav"):
                            register_chunk(discord_id, date_dir.name, wav.name, str(wav))

    # Start background thread that keeps the DB in sync with new VAD chunks
    t = threading.Thread(target=_scan_and_register_all_chunks, daemon=True)
    t.start()
    logger.info(f"Background chunk scanner started (every {CHUNK_SCAN_INTERVAL}s)")


def get_current_user(request: Request):
    user = request.session.get("user")
    if not user:
        return None
    return user


def get_user_recordings(user_id: str):
    """
    Returns chunks_per_date from DB.
    chunks_per_date: { "YYYY-MM-DD": [{"filename": "chunk_001.wav", "transcription": "..."}, ...] }
    """
    return get_chunks_for_user(user_id)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user = get_current_user(request)
    if user:
        return RedirectResponse("/recordings/dashboard")
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/login")
async def login():
    url = get_discord_oauth_url()
    return RedirectResponse(url)


@app.get("/callback")
async def callback(request: Request, code: str = None, error: str = None):
    if error or not code:
        return RedirectResponse("/recordings/")

    token_data = await exchange_code(code)
    if not token_data:
        return RedirectResponse("/recordings/")

    discord_user = await get_discord_user(token_data["access_token"])
    if not discord_user:
        return RedirectResponse("/recordings/")

    discord_id = discord_user["id"]
    username = discord_user["username"]
    avatar = discord_user.get("avatar")
    avatar_url = f"https://cdn.discordapp.com/avatars/{discord_id}/{avatar}.png" if avatar else "https://cdn.discordapp.com/embed/avatars/0.png"

    upsert_user(discord_id, username, avatar_url)

    request.session["user"] = {
        "id": discord_id,
        "username": username,
        "avatar": avatar_url
    }

    logger.info(f"User logged in: {username} ({discord_id})")
    return RedirectResponse("/recordings/dashboard")


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/recordings/")

    chunks_per_date = get_user_recordings(user["id"])

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "user": user,
        "chunks_per_date": chunks_per_date,
    })


@app.get("/audio/{user_id}/chunks/{date}/{filename}")
async def serve_chunk(user_id: str, date: str, filename: str, request: Request):
    current_user = get_current_user(request)
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if current_user["id"] != user_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    if "/" in filename or ".." in filename or "/" in date or ".." in date:
        raise HTTPException(status_code=400, detail="Invalid path")

    audio_file = None
    if RECORDINGS_PATH.exists():
        for user_dir in RECORDINGS_PATH.iterdir():
            if user_dir.name == user_id:
                candidate = user_dir / "chunks" / date / filename
                if candidate.exists():
                    audio_file = candidate
                break

    if not audio_file:
        raise HTTPException(status_code=404, detail="Chunk not found")

    logger.info(f"Chunk served: {current_user['username']} - {date}/{filename}")

    file_size    = audio_file.stat().st_size
    range_header = request.headers.get("range")

    def iter_file(start=0, end=None):
        chunk_size = 1024 * 256
        with open(audio_file, "rb") as f:
            f.seek(start)
            remaining = (end - start + 1) if end else None
            while True:
                to_read = min(chunk_size, remaining) if remaining else chunk_size
                data = f.read(to_read)
                if not data:
                    break
                if remaining:
                    remaining -= len(data)
                yield data
                if remaining is not None and remaining <= 0:
                    break

    if range_header:
        range_val = range_header.replace("bytes=", "")
        start_str, end_str = range_val.split("-")
        start = int(start_str)
        end   = int(end_str) if end_str else file_size - 1
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


# ── Dashboard chunk poll (Discord session auth) ───────────────────────────────

@app.get("/api/chunks/{user_id}")
async def api_chunks(user_id: str, request: Request):
    """
    Returns the current chunks_per_date dict for a user as JSON.
    Reads from DB only — disk scanning is handled by the background thread.
    """
    current_user = get_current_user(request)
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if current_user["id"] != user_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    chunks_per_date = get_chunks_for_user(user_id)
    return {"chunks_per_date": chunks_per_date}


@app.delete("/api/chunks/{user_id}/{date}/{filename}")
async def api_delete_chunk(user_id: str, date: str, filename: str, request: Request):
    """
    Hard-deletes a single chunk: removes the .wav file from disk and marks it
    is_deleted=True in the DB so it won't reappear on the next poll.
    """
    current_user = get_current_user(request)
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if current_user["id"] != user_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    if any(".." in x or "/" in x for x in (date, filename)):
        raise HTTPException(status_code=400, detail="Invalid path")

    result = delete_chunk(user_id, date, filename)
    if result is None:
        raise HTTPException(status_code=404, detail="Chunk not found")

    logger.info(f"Chunk deleted by {current_user['username']} ({user_id}): {date}/{filename}")
    return JSONResponse({"ok": True, "deleted": f"{date}/{filename}"})


# ── Transcription script endpoints (API-key auth) ─────────────────────────────

@app.get("/api/script/chunks/pending")
async def script_pending_chunks(request: Request):
    """
    Returns all chunks that have no transcription yet.
    Used by the remote transcription script to know what to process next.
    Response: { "pending": [{"discord_id", "date", "filename", "claimed_by"}, ...] }
    """
    _require_api_key(request)
    pending = get_pending_chunks()
    logger.info(f"Pending chunks requested — {len(pending)} items returned")
    return {"pending": pending}


@app.post("/api/script/chunks/claim")
async def script_claim_chunks(request: Request):
    """
    Atomically claims up to `batch_size` unclaimed chunks for the given machine_id.
    Already-claimed chunks by the same machine are also returned (resume support).
    Body: { "machine_id": "my-pc", "batch_size": 20 }
    Response: { "claimed": [{"discord_id", "date", "filename"}, ...] }
    """
    _require_api_key(request)
    body = await request.json()
    machine_id = body.get("machine_id", "").strip()
    if not machine_id:
        raise HTTPException(status_code=422, detail="'machine_id' is required")
    batch_size = min(int(body.get("batch_size", 20)), 100)  # cap at 100

    claimed = claim_chunks(machine_id, batch_size)
    logger.info(f"Claim: machine '{machine_id}' claimed {len(claimed)} chunks")
    return {"claimed": claimed}


@app.get("/api/script/download/{user_id}/{date}/{filename}")
async def script_download_chunk(user_id: str, date: str, filename: str, request: Request):
    """
    Streams a chunk .wav file to the remote transcription script.
    Authenticated with X-API-Key — no browser session required.
    """
    _require_api_key(request)

    if "/" in filename or ".." in filename or "/" in date or ".." in date:
        raise HTTPException(status_code=400, detail="Invalid path")

    audio_file = None
    if RECORDINGS_PATH.exists():
        for user_dir in RECORDINGS_PATH.iterdir():
            if user_dir.name == user_id:
                candidate = user_dir / "chunks" / date / filename
                if candidate.exists():
                    audio_file = candidate
                break

    if not audio_file:
        raise HTTPException(status_code=404, detail="Chunk not found")

    logger.info(f"Script download: {user_id}/{date}/{filename}")

    file_size = audio_file.stat().st_size

    def iter_file():
        with open(audio_file, "rb") as f:
            while chunk := f.read(1024 * 256):
                yield chunk

    return StreamingResponse(
        iter_file(),
        media_type="audio/wav",
        headers={"Content-Length": str(file_size), "Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/script/transcriptions/{user_id}/{date}/{filename}")
async def script_submit_transcription(user_id: str, date: str, filename: str, request: Request):
    """
    Receives a transcription result from the remote script and saves it to the DB.
    Body: { "transcription": "..." }
    """
    _require_api_key(request)

    if "/" in filename or ".." in filename or "/" in date or ".." in date:
        raise HTTPException(status_code=400, detail="Invalid path")

    body = await request.json()
    text = body.get("transcription", "").strip()
    if not text:
        raise HTTPException(status_code=422, detail="'transcription' field is required and must not be empty")

    ok = set_transcription(user_id, date, filename, text)
    if not ok:
        raise HTTPException(status_code=404, detail="Chunk not found or already deleted")

    logger.info(f"Transcription saved: {user_id}/{date}/{filename} ({len(text.split())} words)")
    return JSONResponse({"ok": True, "chunk": f"{date}/{filename}", "words": len(text.split())})


@app.get("/logout")
async def logout(request: Request):
    user = get_current_user(request)
    if user:
        logger.info(f"User logged out: {user['username']} ({user['id']})")
    request.session.clear()
    return RedirectResponse("/recordings/")
