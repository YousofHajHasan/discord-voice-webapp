"""
LOCAL DEV RUNNER — not for production.

Lets you test the dashboard / validate / submissions pages locally without
Discord OAuth or the VPS, using real audio from your disk.

How it stays safe:
  - The login bypass is a *runtime monkeypatch* applied only here. The shipped
    modules (main.py / validate.py) contain no bypass code.
  - This file is never imported by the production entrypoint (the Docker CMD is
    `uvicorn main:app`), so it can't affect the deployed app.

Run:
    .venv/bin/python dev.py
then open  http://localhost:8000/

Useful overrides (env vars):
    DEV_USER_ID, DEV_USERNAME, DEV_AUDIO_SRC, DEV_CHUNK_COUNT, PORT, RECORDINGS_PATH
"""
import os
import shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent

DEV_USER_ID  = os.environ.get("DEV_USER_ID", "416638591482068993")
DEV_USERNAME = os.environ.get("DEV_USERNAME", "localdev")
DEV_AVATAR   = "https://cdn.discordapp.com/embed/avatars/0.png"
REC_DIR      = Path(os.environ.get("RECORDINGS_PATH", str(HERE / "dev_recordings")))
AUDIO_SRC    = Path(os.environ.get("DEV_AUDIO_SRC", "/home/yousof/Desktop/Discord/clean_audio/0-3s"))
CHUNK_COUNT  = int(os.environ.get("DEV_CHUNK_COUNT", "15"))
PORT         = int(os.environ.get("PORT", "8000"))

# Env that the app modules read at import time. OAuth values are dummies because
# login is bypassed. setdefault() so real env always wins.
os.environ.setdefault("RECORDINGS_PATH", str(REC_DIR))
os.environ.setdefault("DB_PATH", str(HERE / "dev.db"))
os.environ.setdefault("SESSION_SECRET", "dev-secret-not-for-prod-0123456789")
os.environ.setdefault("DISCORD_CLIENT_ID", "dev")
os.environ.setdefault("DISCORD_CLIENT_SECRET", "dev")
os.environ.setdefault("DISCORD_REDIRECT_URI", f"http://localhost:{PORT}/callback")


def seed_recordings():
    """Copy a few real wavs into <user>/chunks/<date>/chunk_NNN.wav (idempotent)."""
    chunks_root = REC_DIR / DEV_USER_ID / "chunks"
    if chunks_root.exists() and any(chunks_root.rglob("chunk_*.wav")):
        print(f"[dev] recordings already present at {REC_DIR} — leaving as-is")
        return
    if not AUDIO_SRC.is_dir():
        print(f"[dev] WARNING: audio source not found: {AUDIO_SRC}  (no chunks seeded)")
        return
    srcs = sorted(AUDIO_SRC.glob("*.wav"))[:CHUNK_COUNT]
    if not srcs:
        print(f"[dev] WARNING: no .wav files in {AUDIO_SRC}")
        return

    today = datetime.now(timezone.utc).date()
    dates = [str(today - timedelta(days=1)), str(today)]  # two days -> exercises grouping/filter
    split = max(1, len(srcs) // 2)
    buckets = {dates[0]: srcs[:split], dates[1]: srcs[split:]}
    total = 0
    for date_str, files in buckets.items():
        if not files:
            continue
        d = REC_DIR / DEV_USER_ID / "chunks" / date_str
        d.mkdir(parents=True, exist_ok=True)
        for i, src in enumerate(files, start=1):
            shutil.copy(src, d / f"chunk_{i:03d}.wav")
            total += 1
    print(f"[dev] seeded {total} chunks into {REC_DIR}/{DEV_USER_ID}/chunks/ across {dates}")


def main():
    # Safety: never let the login-bypass run against the production volumes.
    # In the container DB_PATH/RECORDINGS_PATH are /app/... — refuse there.
    for var in ("DB_PATH", "RECORDINGS_PATH"):
        if os.environ.get(var, "").startswith("/app"):
            raise SystemExit(
                f"dev.py refused to start: {var}={os.environ[var]} looks like production.\n"
                "This is a LOCAL-ONLY tool — it bypasses Discord login."
            )

    seed_recordings()

    import main as app_main
    import validate

    dev_user = {"id": DEV_USER_ID, "username": DEV_USERNAME, "avatar": DEV_AVATAR}
    app_main.get_current_user = lambda *_: dev_user   # bypass Discord login (ignores request)
    validate.get_current_user = lambda *_: dev_user

    print("\n" + "=" * 60)
    print("  LOCAL DEV MODE — Discord login bypassed")
    print(f"  User : {DEV_USERNAME} ({DEV_USER_ID})")
    print(f"  Open : http://localhost:{PORT}/")
    print("=" * 60 + "\n")

    import uvicorn
    uvicorn.run(app_main.app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
