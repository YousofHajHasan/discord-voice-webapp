"""
LOCAL DEV RUNNER — not for production.

Lets you test the dashboard / validate / submissions pages locally without
Discord OAuth or the VPS, using real audio from your disk — including the
delegated multi-validator flow.

How it stays safe:
  - The login bypass is a *runtime monkeypatch* applied only here. The shipped
    modules (main.py / validate.py) contain no bypass code.
  - This file is never imported by the production entrypoint (the Docker CMD is
    `uvicorn main:app`), so it can't affect the deployed app.

Run:
    .venv/bin/python dev.py
then open  http://localhost:8000/

To test the multi-validator / delegated-access feature, open the printed
/dev/as/<id> links in SEPARATE browsers (or one normal + one incognito) so each
window is a different user — then validate the SAME owner in both and watch the
10-at-a-time leases split with no overlap.

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
CHUNK_COUNT  = int(os.environ.get("DEV_CHUNK_COUNT", "30"))
PORT         = int(os.environ.get("PORT", "8000"))

# Extra seeded identities so the owner dropdown + "Manage access" panel have
# something to show. All ids are numeric — the disk scanner only registers
# numeric-id folders and grant_access() only accepts numeric ids.
TEAMMATE_ID = "100000000000000002"   # a delegate (validates the dev user's voices)
ALICE_ID    = "100000000000000003"   # another owner (grants the dev user access)
DEV_USERS = {
    DEV_USER_ID: DEV_USERNAME,
    TEAMMATE_ID: "teammate",
    ALICE_ID:    "alice",
}

# Env that the app modules read at import time. OAuth values are dummies because
# login is bypassed. setdefault() so real env always wins.
os.environ.setdefault("RECORDINGS_PATH", str(REC_DIR))
os.environ.setdefault("DB_PATH", str(HERE / "dev.db"))
os.environ.setdefault("SESSION_SECRET", "dev-secret-not-for-prod-0123456789")
os.environ.setdefault("DISCORD_CLIENT_ID", "dev")
os.environ.setdefault("DISCORD_CLIENT_SECRET", "dev")
os.environ.setdefault("DISCORD_REDIRECT_URI", f"http://localhost:{PORT}/callback")


def _seed_user(uid: str, srcs: list) -> int:
    """Copy wavs into <uid>/chunks/<date>/chunk_NNN.wav across two dates. Idempotent."""
    chunks_root = REC_DIR / uid / "chunks"
    if chunks_root.exists() and any(chunks_root.rglob("chunk_*.wav")):
        return 0  # already seeded for this user
    if not srcs:
        return 0
    today = datetime.now(timezone.utc).date()
    dates = [str(today - timedelta(days=1)), str(today)]  # two days -> exercises grouping/filter
    split = max(1, len(srcs) // 2)
    buckets = {dates[0]: srcs[:split], dates[1]: srcs[split:]}
    total = 0
    for date_str, files in buckets.items():
        if not files:
            continue
        d = REC_DIR / uid / "chunks" / date_str
        d.mkdir(parents=True, exist_ok=True)
        for i, src in enumerate(files, start=1):
            shutil.copy(src, d / f"chunk_{i:03d}.wav")
            total += 1
    return total


def seed_recordings():
    """Seed the dev user (A) and 'alice' (C) with DIFFERENT real wavs."""
    if not AUDIO_SRC.is_dir():
        print(f"[dev] WARNING: audio source not found: {AUDIO_SRC}  (no chunks seeded)")
        return
    wavs = sorted(AUDIO_SRC.glob("*.wav"))
    if not wavs:
        print(f"[dev] WARNING: no .wav files in {AUDIO_SRC}")
        return
    a = _seed_user(DEV_USER_ID, wavs[:CHUNK_COUNT])
    c = _seed_user(ALICE_ID, wavs[CHUNK_COUNT:CHUNK_COUNT + 15])
    if a or c:
        print(f"[dev] seeded {a} chunks for {DEV_USERNAME}, {c} for alice, into {REC_DIR}")
    else:
        print(f"[dev] recordings already present at {REC_DIR} — leaving as-is")


def seed_users_and_grants():
    """Create demo users + access grants so the dropdown/manage panel are populated."""
    from database import upsert_user
    import validation_db as vdb
    for uid, name in DEV_USERS.items():
        upsert_user(uid, name, DEV_AVATAR)
    # alice (C) lets the dev user (A) validate her voices -> alice shows in A's dropdown
    vdb.grant_access(ALICE_ID, DEV_USER_ID)
    # dev user (A) lets teammate (B) validate his voices  -> teammate shows in A's manage
    #                                                         panel, and A shows in B's dropdown
    vdb.grant_access(DEV_USER_ID, TEAMMATE_ID)


def _setup_app():
    """Wire the dev bypass + seed data, return the FastAPI app (no server start)."""
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
    from database import init_db, upsert_user
    from fastapi.responses import RedirectResponse

    # Insert demo users/grants before serving (init_db is idempotent — startup()
    # calls it again harmlessly).
    init_db()
    seed_users_and_grants()

    default_user = {"id": DEV_USER_ID, "username": DEV_USERNAME, "avatar": DEV_AVATAR}

    # Cookie-based login bypass: identity comes from a plain `dev_uid` cookie set
    # by /dev/as/<id>, else defaults to the dev user. A PLAIN (non-Secure) cookie
    # is used on purpose — the app's real session cookie is https_only=True, so it
    # would be dropped over http://localhost. Because it's per-cookie-jar, TWO
    # browsers can act as TWO validators and you can watch the claim/lease split
    # chunks live. Runtime-only; the shipped main.py/validate.py have no bypass.
    def _dev_current_user(request=None):
        if request is not None:
            try:
                uid = request.cookies.get("dev_uid")
                if uid:
                    return {"id": uid, "username": DEV_USERS.get(uid, uid), "avatar": DEV_AVATAR}
            except Exception:
                pass
        return default_user

    app_main.get_current_user = _dev_current_user
    validate.get_current_user = _dev_current_user

    @app_main.app.get("/dev/as/{uid}")
    def _dev_login_as(uid: str):
        """DEV-ONLY: become user <uid> in this browser (sets a plain dev_uid cookie)."""
        upsert_user(uid, DEV_USERS.get(uid, uid), DEV_AVATAR)
        resp = RedirectResponse("/recordings/validate")
        resp.set_cookie("dev_uid", uid, max_age=604800, samesite="lax")
        return resp

    return app_main.app


def main():
    app = _setup_app()

    base = f"http://localhost:{PORT}/recordings"
    print("\n" + "=" * 66)
    print("  LOCAL DEV MODE — Discord login bypassed")
    print(f"  Default user : {DEV_USERNAME} ({DEV_USER_ID})")
    print(f"  Open         : http://localhost:{PORT}/")
    print("  ── Multi-validator test (open each in a SEPARATE browser / incognito):")
    print(f"     be {DEV_USERNAME:<9}: {base}/dev/as/{DEV_USER_ID}")
    print(f"     be teammate : {base}/dev/as/{TEAMMATE_ID}")
    print(f"     be alice    : {base}/dev/as/{ALICE_ID}")
    print("  ── Try: as localdev validate 'My own voices'; as teammate validate")
    print("     'localdev' — same owner, chunks split 10-at-a-time, no overlap.")
    print("     As localdev the dropdown also shows 'alice' (she granted you access).")
    print("=" * 66 + "\n")

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
