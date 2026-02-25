import os
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from starlette.middleware.sessions import SessionMiddleware
from pathlib import Path

from auth import get_discord_oauth_url, exchange_code, get_discord_user
from database import init_db, upsert_user, log_audio_file

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


@app.on_event("startup")
async def startup():
    init_db()
    if RECORDINGS_PATH.exists():
        for user_dir in RECORDINGS_PATH.iterdir():
            if user_dir.is_dir():
                parts = user_dir.name.rsplit("_", 1)
                if len(parts) == 2:
                    discord_id = parts[1]
                    username = parts[0]
                    for f in user_dir.iterdir():
                        if f.suffix == ".mp3":
                            log_audio_file(discord_id, username, str(f))


def get_current_user(request: Request):
    user = request.session.get("user")
    if not user:
        return None
    return user


def get_user_recordings(user_id: str):
    """Returns (full_recording_path, dated_recordings_sorted_newest_first)"""
    full_recording = None
    dated_recordings = []

    if RECORDINGS_PATH.exists():
        for user_dir in RECORDINGS_PATH.iterdir():
            if user_dir.name.endswith(f"_{user_id}"):
                for f in user_dir.iterdir():
                    if f.suffix == ".mp3":
                        if f.name == "Full_Recording.mp3":
                            full_recording = f.name
                        else:
                            dated_recordings.append(f.name)
                break

    # Sort newest first (filenames are YYYY-MM-DD.mp3 so lexicographic sort works)
    dated_recordings.sort(reverse=True)
    return full_recording, dated_recordings


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

    full_recording, dated_recordings = get_user_recordings(user["id"])

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "user": user,
        "full_recording": full_recording,
        "dated_recordings": dated_recordings,
    })


@app.get("/audio/{user_id}/{filename}")
async def serve_audio(user_id: str, filename: str, request: Request):
    current_user = get_current_user(request)
    if not current_user:
        logger.warning(f"Unauthenticated audio access attempt for user_id={user_id}")
        raise HTTPException(status_code=401, detail="Unauthorized")

    if current_user["id"] != user_id:
        logger.warning(f"Forbidden: {current_user['username']} ({current_user['id']}) tried to access audio of {user_id}")
        raise HTTPException(status_code=403, detail="Forbidden")

    # Security: prevent path traversal
    if "/" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    audio_file = None
    if RECORDINGS_PATH.exists():
        for user_dir in RECORDINGS_PATH.iterdir():
            if user_dir.name.endswith(f"_{user_id}"):
                candidate = user_dir / filename
                if candidate.exists():
                    audio_file = candidate
                break

    if not audio_file:
        raise HTTPException(status_code=404, detail="Recording not found")

    logger.info(f"Audio served to: {current_user['username']} ({current_user['id']}) - {filename}")

    file_size = audio_file.stat().st_size
    range_header = request.headers.get("range")

    def iter_file(start=0, end=None):
        chunk_size = 1024 * 256  # 256KB chunks
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
        end = int(end_str) if end_str else file_size - 1

        headers = {
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(end - start + 1),
            "Content-Type": "audio/mpeg",
        }
        return StreamingResponse(iter_file(start, end), status_code=206, headers=headers)

    return StreamingResponse(
        iter_file(),
        media_type="audio/mpeg",
        headers={"Accept-Ranges": "bytes", "Content-Length": str(file_size)}
    )


@app.get("/logout")
async def logout(request: Request):
    user = get_current_user(request)
    if user:
        logger.info(f"User logged out: {user['username']} ({user['id']})")
    request.session.clear()
    return RedirectResponse("/recordings/")
