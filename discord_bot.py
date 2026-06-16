"""
Discord DM sender + admin-broadcast orchestration — REST only, no gateway.

Why REST and not discord.py: the app already calls Discord's HTTP API with httpx
for OAuth (auth.py), and DMing is the same shape — open a DM channel, then post a
message — just with a *bot* token instead of a bearer token. That avoids running a
persistent gateway/WebSocket client inside a request/response web app (which would
also duplicate itself per worker and complicate deploys). The two calls are:

    POST /users/@me/channels      {"recipient_id": id}     -> a DM channel id
    POST /channels/{id}/messages  {"content": message}     -> the DM

Sending runs as ONE background asyncio task at a time (the admin UI polls
job_status for live progress). Each broadcast is also persisted via validation_db
(create_broadcast / finalize_broadcast) so history survives restarts; the in-memory
_jobs map below only holds live progress for the poll endpoint.

Discord constraints (theirs, not ours): a bot can only DM users it shares a server
with, and users may refuse DMs from server members — both come back as 403 and are
recorded per-recipient instead of aborting the whole run.
"""
import os
import asyncio
import httpx

import validation_db as vdb

DISCORD_API = "https://discord.com/api/v10"
BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN")  # unset => feature disabled (no crash)

# Courtesy pace between recipients, and the cap we'll honor a 429's Retry-After up
# to — so an odd/hostile value can't wedge the job for minutes.
SEND_DELAY_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 30.0
REQUEST_TIMEOUT_SECONDS = 20.0

# Live progress for in-flight / recently finished jobs, keyed by broadcast id. The
# DB (validation_db) is the durable record; this is only what the status endpoint
# polls. `_active` holds the one running job's id — the one-at-a-time lock.
_jobs: dict[int, dict] = {}
_active: dict[str, "int | None"] = {"id": None}


def is_configured() -> bool:
    """True when a bot token is present; the UI disables broadcasting otherwise."""
    return bool(BOT_TOKEN)


def _headers() -> dict:
    return {
        "Authorization": f"Bot {BOT_TOKEN}",
        "Content-Type": "application/json",
        # Discord asks API clients to send a descriptive User-Agent.
        "User-Agent": "discord-voice-webapp (broadcast, 1.0)",
    }


def _retry_after(resp) -> float:
    """Seconds to wait after a 429, from the JSON body (preferred) or header."""
    try:
        ra = float(resp.json().get("retry_after"))
    except Exception:
        try:
            ra = float(resp.headers.get("Retry-After", "1"))
        except (TypeError, ValueError):
            ra = 1.0
    return max(0.0, min(ra, MAX_BACKOFF_SECONDS))


def _explain(resp, default: str) -> str:
    """Human-readable failure reason for the per-recipient report."""
    if resp.status_code == 403:
        return "Can't DM (DMs closed or no shared server)"
    if resp.status_code == 404:
        return "Unknown user"
    return f"{default} (HTTP {resp.status_code})"


async def _fetch_username(client, user_id) -> "str | None":
    """Best-effort display name for a raw pasted id (None on any failure)."""
    try:
        r = await client.get(f"{DISCORD_API}/users/{user_id}", headers=_headers())
        if r.status_code == 200:
            d = r.json()
            return d.get("global_name") or d.get("username")
    except Exception:
        pass
    return None


async def _attempt_dm(client, user_id, content) -> dict:
    """
    One open-channel + send attempt. Returns:
      {ok, error, rate_limited, retry_after}
    On a 429 the caller backs off `retry_after` seconds and retries once.
    """
    ch = await client.post(f"{DISCORD_API}/users/@me/channels",
                           headers=_headers(), json={"recipient_id": str(user_id)})
    if ch.status_code == 429:
        return {"ok": False, "error": None, "rate_limited": True, "retry_after": _retry_after(ch)}
    if ch.status_code not in (200, 201):
        return {"ok": False, "error": _explain(ch, "Couldn't open DM"),
                "rate_limited": False, "retry_after": 0.0}

    channel_id = ch.json().get("id")
    msg = await client.post(f"{DISCORD_API}/channels/{channel_id}/messages",
                            headers=_headers(), json={"content": content})
    if msg.status_code == 429:
        return {"ok": False, "error": None, "rate_limited": True, "retry_after": _retry_after(msg)}
    if msg.status_code not in (200, 201):
        return {"ok": False, "error": _explain(msg, "Couldn't send message"),
                "rate_limited": False, "retry_after": 0.0}
    return {"ok": True, "error": None, "rate_limited": False, "retry_after": 0.0}


async def _run_job(job_id: int, recipients: list, message: str) -> None:
    """Background sender: DM each recipient in turn, updating live progress, then
    persist the final outcome. Never raises out (it's a fire-and-forget task) and
    always clears the one-at-a-time lock."""
    job = _jobs[job_id]
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
            for rec in recipients:
                uid = str(rec["id"])
                name = rec.get("name") or await _fetch_username(client, uid) or uid

                res = await _attempt_dm(client, uid, message)
                if res["rate_limited"]:                       # back off, then one retry
                    await asyncio.sleep(res["retry_after"] or 1.0)
                    res = await _attempt_dm(client, uid, message)

                ok = res["ok"]
                job["results"].append({
                    "id": uid, "name": name,
                    "status": "sent" if ok else "failed",
                    "error": None if ok else (res["error"] or "Rate limited"),
                })
                job["done"] += 1
                job["sent" if ok else "failed"] += 1
                await asyncio.sleep(SEND_DELAY_SECONDS)
    except Exception as e:                                     # surface, don't crash
        job["error"] = str(e)
    finally:
        job["status"] = "done"
        try:
            vdb.finalize_broadcast(job_id, job["sent"], job["failed"], job["results"])
        except Exception as e:
            job["error"] = job.get("error") or str(e)
        _active["id"] = None


def start_broadcast(admin_id: str, message: str, recipients: list) -> "int | None":
    """
    Persist the broadcast, register live progress, and schedule the sender.
    Returns the new broadcast id (the job id), or None if one is already running
    (the one-at-a-time lock).

    Runs synchronously up to asyncio.create_task so the lock's check-and-set can't
    interleave with another request on the single event loop. Must be called from
    within the running loop (i.e. from an async route).
    """
    if _active["id"] is not None:
        return None
    broadcast_id = vdb.create_broadcast(admin_id, message, len(recipients))
    _jobs[broadcast_id] = {
        "status": "running", "total": len(recipients),
        "done": 0, "sent": 0, "failed": 0, "results": [], "error": None,
    }
    _active["id"] = broadcast_id
    asyncio.create_task(_run_job(broadcast_id, recipients, message))
    return broadcast_id


def job_status(job_id: int) -> "dict | None":
    """Live progress for a job (from memory), falling back to the persisted row
    once it's finished or after a restart. None if unknown."""
    j = _jobs.get(job_id)
    if j is not None:
        keys = ("status", "total", "done", "sent", "failed", "results", "error")
        return {"id": job_id, **{k: j[k] for k in keys}}
    return vdb.get_broadcast(job_id)
