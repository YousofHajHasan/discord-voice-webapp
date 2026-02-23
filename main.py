import os
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from pathlib import Path

from auth import get_discord_oauth_url, exchange_code, get_discord_user
from database import init_db, upsert_user, get_user, log_audio_file

app = FastAPI(root_path="/recordings")

app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ["SESSION_SECRET"],
    https_only=True,
    same_site="lax"
)

templates = Jinja2Templates(directory="templates")

RECORDINGS_PATH = Path(os.environ.get("RECORDINGS_PATH", "/app/recordings"))


@app.on_event("startup")
async def startup():
    init_db()
    # Index existing recordings into DB
    if RECORDINGS_PATH.exists():
        for user_dir in RECORDINGS_PATH.iterdir():
            if user_dir.is_dir():
                parts = user_dir.name.rsplit("_", 1)
                if len(parts) == 2:
                    discord_id = parts[1]
                    username = parts[0]
                    audio_file = user_dir / "Full_Recording.mp3"
                    if audio_file.exists():
                        log_audio_file(discord_id, username, str(audio_file))


def get_current_user(request: Request):
    user = request.session.get("user")
    if not user:
        return None
    return user


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user = get_current_user(request)
    if user:
        return RedirectResponse("/dashboard")
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/login")
async def login():
    url = get_discord_oauth_url()
    return RedirectResponse(url)


@app.get("/callback")
async def callback(request: Request, code: str = None, error: str = None):
    if error or not code:
        return RedirectResponse("/")

    token_data = await exchange_code(code)
    if not token_data:
        return RedirectResponse("/")

    discord_user = await get_discord_user(token_data["access_token"])
    if not discord_user:
        return RedirectResponse("/")

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

    return RedirectResponse("/dashboard")


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/")

    # Find their recording folder
    recording_exists = False
    if RECORDINGS_PATH.exists():
        for user_dir in RECORDINGS_PATH.iterdir():
            if user_dir.name.endswith(f"_{user['id']}"):
                audio_file = user_dir / "Full_Recording.mp3"
                if audio_file.exists():
                    recording_exists = True
                break

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "user": user,
        "recording_exists": recording_exists
    })


@app.get("/audio/{user_id}")
async def serve_audio(user_id: str, request: Request):
    current_user = get_current_user(request)
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Users can only access their own audio
    if current_user["id"] != user_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    audio_file = None
    if RECORDINGS_PATH.exists():
        for user_dir in RECORDINGS_PATH.iterdir():
            if user_dir.name.endswith(f"_{user_id}"):
                candidate = user_dir / "Full_Recording.mp3"
                if candidate.exists():
                    audio_file = candidate
                break

    if not audio_file:
        raise HTTPException(status_code=404, detail="Recording not found")

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
    request.session.clear()
    return RedirectResponse("/")
