"""
Validation / dataset-building DB helpers.

Kept separate from database.py (which owns the models + engine) so the
validation workflow can evolve on its own. Everything here is small and
composable and goes through the shared SessionLocal.

Status model (Chunk.validation_status):
    pending  — not yet looked at (default)
    verified — human accepted; verified_transcription holds the confirmed text
    issue    — has understandable speech but needs trimming/editing; .wav kept,
               verified_transcription holds any partial text / note. Hidden from
               the validate queue and the dashboard, but shown in submissions.
    rejected — no understandable speech; soft-rejected. .wav stays on disk but
               it's hidden from the queue and the dashboard.
"""
import os
import json
from datetime import datetime, timezone, timedelta

from sqlalchemy import or_, and_, update, select, func, case
from database import (
    SessionLocal, Chunk, AccessGrant, Admin, User, Withdrawal, PayoutProfile, Broadcast, Skip,
    _resolve_filepath, measure_chunk_duration, trim_wav, RECORDINGS_PATH,
    VALIDATION_LEASE_MINUTES, LABEL_KEYS, LABEL_NORMAL_KEY,
)

# Statuses that count as "still to validate" for the Insights remaining total.
# issue is included on purpose (flagged-but-not-finished work — see get_insights).
REMAINING_STATUSES = ["pending", "issue"]

# ── Payouts / earnings ────────────────────────────────────────────────────────
# Validators are paid for the AUDIO THEY VALIDATED — every decision counts
# (accept, reject, AND issue are all real listening work). ONE global rate,
# env-tunable: PAY_PER_HOUR USD per hour of validated audio ($8 / hour default).
# Earnings are computed live (see get_wallet); a Withdrawal row is the only thing
# persisted. Money is in USD, rounded to cents at the edges.
PAY_PER_HOUR = float(os.environ.get("PAY_PER_HOUR", "8"))
PAY_RATE_PER_SEC = PAY_PER_HOUR / 3600.0
MIN_WITHDRAWAL_USD = float(os.environ.get("MIN_WITHDRAWAL_USD", "5"))

# Decisions that count as completed validation work toward earnings.
PAID_STATUSES = ["verified", "rejected", "issue"]
# Withdrawal statuses whose amount is "spent" (subtracted from available).
SPENT_WITHDRAWAL_STATUSES = ["pending", "paid"]

# ── Content-classification taxonomy ───────────────────────────────────────────
# Single source of truth for the validator's label chips: key (== DB column ==
# JSON/JS key, see database.LABEL_KEYS) + display label + one-line description.
# Re-phrasing a label is a one-line change here and needs no DB migration — the
# internal keys stay frozen. `exclusive` marks "Normal" as the mutually-exclusive
# "nothing special" choice.
LABELS = [
    {"key": "label_laugh",   "label": "Laughter",            "desc": "Audible laughing."},
    {"key": "label_abusive", "label": "Abusive / profanity", "desc": "Insults, slurs, or strong profanity."},
    {"key": "label_gaming",  "label": "Gaming talk",         "desc": "Gaming / stream chatter."},
    {"key": "label_sports",  "label": "Sports talk",         "desc": "Sports discussion."},
    {"key": "label_english", "label": "Contains English",    "desc": "English words mixed in (code-switching)."},
    {"key": "label_singing", "label": "Singing",             "desc": "Someone is actually singing."},
    {"key": "label_normal",  "label": "Normal",              "desc": "Ordinary speech — nothing special.", "exclusive": True},
]
# Guard against drift between the UI taxonomy and the DB columns.
assert {l["key"] for l in LABELS} == set(LABEL_KEYS), "LABELS keys must match database.LABEL_KEYS"


def _clean_labels(labels):
    """
    Normalize a client label selection into {key: bool} for every LABEL_KEY, or
    return None if the selection is INVALID for verifying a chunk:
      - nothing selected (>=1 is required), or
      - `normal` combined with any other label (the exclusive "nothing special"
        choice).
    Unknown keys in the input are ignored.
    """
    labels = labels or {}
    out = {k: bool(labels.get(k)) for k in LABEL_KEYS}
    chosen = [k for k, v in out.items() if v]
    if not chosen:
        return None
    if LABEL_NORMAL_KEY in chosen and len(chosen) > 1:
        return None
    return out


# ── Access grants (delegated validation) ──────────────────────────────────────

def grant_access(owner_id: str, delegate_id: str) -> bool:
    """
    Allow `delegate_id` to validate `owner_id`'s chunks. Idempotent. Returns
    False for an empty id, a self-grant, or a non-numeric id (Discord ids are
    numeric snowflakes) so the endpoint can reject bad input.
    """
    delegate_id = (delegate_id or "").strip()
    if not delegate_id or not delegate_id.isdigit() or delegate_id == owner_id:
        return False
    gid = f"{owner_id}:{delegate_id}"
    with SessionLocal() as db:
        if not db.get(AccessGrant, gid):
            db.add(AccessGrant(id=gid, owner_id=owner_id, delegate_id=delegate_id))
            db.commit()
    return True


def revoke_access(owner_id: str, delegate_id: str):
    gid = f"{owner_id}:{delegate_id}"
    with SessionLocal() as db:
        g = db.get(AccessGrant, gid)
        if g:
            db.delete(g)
            db.commit()


def get_accessible_owner_ids(viewer_id: str) -> list:
    """Owners `viewer_id` may validate: themselves + anyone who granted them."""
    owners = {viewer_id}
    with SessionLocal() as db:
        rows = db.query(AccessGrant).filter(AccessGrant.delegate_id == viewer_id).all()
        owners.update(r.owner_id for r in rows)
    return list(owners)


def can_access(viewer_id: str, owner_id: str) -> bool:
    if viewer_id == owner_id:
        return True
    with SessionLocal() as db:
        return db.get(AccessGrant, f"{owner_id}:{viewer_id}") is not None


def get_accessible_owners(viewer_id: str) -> list:
    """
    Owners the viewer may validate, shaped for the dropdown: the viewer first
    ("My own voices"), then everyone who granted them access (alphabetical).
    Each entry: {id, name (username or id), is_self, pending (count)}.
    """
    owner_ids = get_accessible_owner_ids(viewer_id)        # set incl. viewer
    with SessionLocal() as db:
        names = {
            u.discord_id: u.username
            for u in db.query(User).filter(User.discord_id.in_(owner_ids)).all()
        }
        counts = dict(
            db.query(Chunk.discord_id, func.count(Chunk.id))
            .filter(
                Chunk.discord_id.in_(owner_ids),
                Chunk.is_deleted == False,
                Chunk.validation_status == "pending",
            )
            .group_by(Chunk.discord_id)
            .all()
        )
    others = sorted(
        (o for o in owner_ids if o != viewer_id),
        key=lambda o: (names.get(o) or o).lower(),
    )
    return [
        {
            "id": o,
            "name": names.get(o) or o,
            "is_self": o == viewer_id,
            "pending": int(counts.get(o, 0)),
        }
        for o in [viewer_id] + others
    ]


def get_delegates(owner_id: str) -> list:
    """People `owner_id` has granted validate-access to (for the manage panel)."""
    with SessionLocal() as db:
        ids = [
            r.delegate_id
            for r in db.query(AccessGrant)
            .filter(AccessGrant.owner_id == owner_id)
            .order_by(AccessGrant.created_at.asc())
            .all()
        ]
        names = (
            {u.discord_id: u.username
             for u in db.query(User).filter(User.discord_id.in_(ids)).all()}
            if ids else {}
        )
    return [{"id": d, "name": names.get(d) or d} for d in ids]


def release_my_claims(viewer_id: str, owner_id: str = None) -> int:
    """
    Free a validator's still-pending leases immediately (on owner-switch or page
    leave) instead of waiting for the 15-min timeout. Scoped to one owner when
    given. Decided chunks are untouched (their lease was already cleared).
    """
    with SessionLocal() as db:
        q = db.query(Chunk).filter(
            Chunk.validation_claimed_by == viewer_id,
            Chunk.validation_status == "pending",
        )
        if owner_id:
            q = q.filter(Chunk.discord_id == owner_id)
        n = q.update(
            {Chunk.validation_claimed_by: None, Chunk.validation_claimed_at: None},
            synchronize_session=False,
        )
        if n:
            db.commit()
    return n


# ── Serialization ─────────────────────────────────────────────────────────────

def _serialize(row) -> dict:
    return {
        "owner_id": row.discord_id,
        "date": row.date,
        "filename": row.filename,
        "transcription": row.transcription,                 # raw ASR text (may be None)
        "verified_transcription": row.verified_transcription,
        "status": row.validation_status or "pending",
        "validated_at": row.validated_at.isoformat() if row.validated_at else None,
        "labels": {k: bool(getattr(row, k, False)) for k in LABEL_KEYS},
    }


# ── Read queues ───────────────────────────────────────────────────────────────

def claim_pending_window(viewer_id: str, owner_id: str, limit: int = 10):
    """
    Atomically LEASE up to `limit` of `owner_id`'s pending chunks to `viewer_id`
    and return just those freshly-leased chunks, plus the owner's total pending.
    This is what prevents two validators working the same owner from colliding.

    Race-free across concurrent validators (and uvicorn workers): the lease is a
    single conditional UPDATE — "claim the next N pending chunks that are
    unclaimed or whose lease went stale" — and SQLite serializes writers. So if
    two validators claim the same owner at once, one UPDATE commits first; the
    other's subquery then no longer sees those rows as claimable and it gets the
    NEXT window instead. No overlap is possible. Abandoned leases free up after
    VALIDATION_LEASE_MINUTES (see database.release_stale_validation_claims).

    Only freshly-leased rows are returned (identified by leaser + claim
    timestamp); chunks the viewer is already holding keep their original lease
    and aren't re-sent — the client buffers them for Back-navigation. Rows whose
    .wav vanished are marked is_deleted so the queue can't jam on them.
    """
    if not can_access(viewer_id, owner_id):
        return [], 0
    limit = max(1, min(int(limit), 200))
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=VALIDATION_LEASE_MINUTES)
    # Chunks this viewer has SKIPPED are permanently out of their queue (they may
    # already have been decided by someone else — see skip_chunk).
    my_skips = select(Skip.chunk_id).where(Skip.viewer_id == viewer_id)

    with SessionLocal() as db:
        # The next claimable window: pending, not deleted, not skipped-by-me, and
        # either unclaimed or with an expired lease. (Rows already leased by this
        # viewer are excluded so each fetch returns genuinely new work — they're
        # still held, just not re-sent.) Ordered owner-stable by date -> filename.
        candidates = (
            select(Chunk.id)
            .where(
                Chunk.discord_id == owner_id,
                Chunk.is_deleted == False,
                Chunk.validation_status == "pending",
                Chunk.id.notin_(my_skips),
                or_(
                    Chunk.validation_claimed_by.is_(None),
                    Chunk.validation_claimed_at < cutoff,
                ),
            )
            .order_by(Chunk.date.asc(), Chunk.filename.asc())
            .limit(limit)
            .scalar_subquery()
        )
        # One atomic write: stamp the chosen rows as ours. Commit immediately to
        # release SQLite's write lock BEFORE the (possibly slow, network-volume)
        # file-existence checks below, so other validators aren't blocked on disk.
        db.execute(
            update(Chunk)
            .where(Chunk.id.in_(candidates))
            .values(validation_claimed_by=viewer_id, validation_claimed_at=now)
            .execution_options(synchronize_session=False)
        )
        db.commit()

        # Exactly the rows this call just leased (unique by leaser + timestamp).
        rows = (
            db.query(Chunk)
            .filter(
                Chunk.validation_claimed_by == viewer_id,
                Chunk.validation_claimed_at == now,
            )
            .order_by(Chunk.date.asc(), Chunk.filename.asc())
            .all()
        )

        items, drop_ids = [], []
        for r in rows:
            if _resolve_filepath(r) is not None:
                items.append(_serialize(r))
            else:
                drop_ids.append(r.id)
        if drop_ids:
            (
                db.query(Chunk)
                .filter(Chunk.id.in_(drop_ids))
                .update(
                    {
                        Chunk.is_deleted: True,
                        Chunk.validation_claimed_by: None,
                        Chunk.validation_claimed_at: None,
                    },
                    synchronize_session=False,
                )
            )
            db.commit()

        # Pending count as THIS viewer sees it — excludes what they've skipped, so
        # the "N pending in queue" stat never promises work they can't be served.
        total = (
            db.query(Chunk)
            .filter(
                Chunk.discord_id == owner_id,
                Chunk.is_deleted == False,
                Chunk.validation_status == "pending",
                Chunk.id.notin_(my_skips),
            )
            .count()
        )
    return items, total


def get_submissions(viewer_id: str) -> list:
    """
    All decided chunks (verified + rejected) the viewer can access, most recent
    decision first — powers the "My Submissions" management page.
    """
    owners = get_accessible_owner_ids(viewer_id)
    with SessionLocal() as db:
        rows = (
            db.query(Chunk)
            .filter(
                Chunk.discord_id.in_(owners),
                Chunk.is_deleted == False,
                Chunk.validation_status.in_(["verified", "rejected", "issue"]),
            )
            .order_by(Chunk.validated_at.desc())
            .all()
        )
        return [_serialize(r) for r in rows if _resolve_filepath(r) is not None]


# ── Insights ──────────────────────────────────────────────────────────────────

def get_insights(owner_id: str) -> dict:
    """
    Per-owner validation insights for the submissions-page popup, scoped to a
    SINGLE owner (the viewer's own voices by default, or a user who granted the
    viewer access — the caller checks that access). All durations in SECONDS
    (the frontend formats minutes/hours):

      verified_count / verified_seconds
          That owner's chunks accepted by ANY validator (status 'verified') —
          i.e. dataset completion for this person's voices, not "who did it".

      remaining_count / remaining_seconds
          That owner's chunks still to do (status pending OR issue). Duration is
          the SUM of already-measured rows.

      remaining_unmeasured
          How many of those remaining chunks have no duration yet (backfill in
          progress). When > 0 the UI labels the time as "measuring…" since it's
          still climbing. 0 means the totals are exact.

    Computed entirely with the (discord_id, validation_status) index — no
    per-request file reads.
    """
    with SessionLocal() as db:
        v_count, v_secs = (
            db.query(
                func.count(Chunk.id),
                func.coalesce(func.sum(Chunk.duration), 0.0),
            )
            .filter(
                Chunk.discord_id == owner_id,
                Chunk.validation_status == "verified",
                Chunk.is_deleted == False,
            )
            .one()
        )
        r_count, r_secs, r_unmeasured = (
            db.query(
                func.count(Chunk.id),
                func.coalesce(func.sum(func.coalesce(Chunk.duration, 0.0)), 0.0),
                func.coalesce(func.sum(case((Chunk.duration.is_(None), 1), else_=0)), 0),
            )
            .filter(
                Chunk.discord_id == owner_id,
                Chunk.is_deleted == False,
                Chunk.validation_status.in_(REMAINING_STATUSES),
            )
            .one()
        )
    return {
        "owner_id": owner_id,
        "verified_count": int(v_count or 0),
        "verified_seconds": float(v_secs or 0.0),
        "remaining_count": int(r_count or 0),
        "remaining_seconds": float(r_secs or 0.0),
        "remaining_unmeasured": int(r_unmeasured or 0),
    }


# ── Admins (DB-backed, UI-managed) ────────────────────────────────────────────
# Source of truth for the admin gate is the `admins` table (not env/config), so
# the list is managed from the admin page with no redeploy. Bootstrap the first
# admin once with a single INSERT, then add the rest from the UI (see Admin in
# database.py for the SQL). Every admin route also enforces is_admin server-side.

def is_admin(discord_id: str) -> bool:
    """True if this Discord id has admin rights (dataset stats + admin mgmt)."""
    if not discord_id:
        return False
    with SessionLocal() as db:
        return db.get(Admin, discord_id) is not None


def list_admins() -> list:
    """All admins for the manage panel, oldest first.
    Each: {id, name, created_at, created_by}."""
    with SessionLocal() as db:
        rows = db.query(Admin).order_by(Admin.created_at.asc()).all()
        ids = [r.discord_id for r in rows]
        names = (
            {u.discord_id: u.username
             for u in db.query(User).filter(User.discord_id.in_(ids)).all()}
            if ids else {}
        )
        return [
            {"id": r.discord_id,
             "name": names.get(r.discord_id) or r.discord_id,
             "created_at": r.created_at.isoformat() if r.created_at else None,
             "created_by": r.created_by}
            for r in rows
        ]


def add_admin(actor_id: str, new_id: str) -> bool:
    """Grant admin to `new_id` (idempotent). False for an empty/non-numeric id
    (Discord ids are numeric snowflakes) so the endpoint can reject bad input."""
    new_id = (new_id or "").strip()
    if not new_id or not new_id.isdigit():
        return False
    with SessionLocal() as db:
        if not db.get(Admin, new_id):
            db.add(Admin(discord_id=new_id, created_by=actor_id))
            db.commit()
    return True


def remove_admin(target_id: str) -> str:
    """Revoke admin from `target_id`. Refuses to remove the LAST admin so the
    admin area can never lock everyone out. Returns 'ok' | 'last' | 'notfound'."""
    target_id = (target_id or "").strip()
    with SessionLocal() as db:
        row = db.get(Admin, target_id)
        if not row:
            return "notfound"
        if db.query(Admin).count() <= 1:
            return "last"
        db.delete(row)
        db.commit()
    return "ok"


# ── Dataset stats (admin) ─────────────────────────────────────────────────────

def get_dataset_stats() -> dict:
    """
    Whole-dataset + per-owner validation totals for the admin page. Same shape as
    get_insights() but across EVERY owner, computed in ONE grouped pass off the
    (discord_id, validation_status) index — no per-request file reads.

    'remaining' = pending + issue (REMAINING_STATUSES), matching Insights; rejected
    chunks count toward neither (out of the verifiable queue). Durations in SECONDS
    — the frontend formats minutes/hours. Owners whose chunks are all rejected/
    deleted are omitted from `users`.

    Returns:
      {"totals": {verified_count, verified_seconds, remaining_count,
                  remaining_seconds, remaining_unmeasured},
       "users":  [{owner_id, name, verified_count, verified_seconds,
                   remaining_count, remaining_seconds, remaining_unmeasured}, ...]}
      `users` sorted by verified_seconds desc, then remaining_seconds desc.
    """
    verified = Chunk.validation_status == "verified"
    remaining = Chunk.validation_status.in_(REMAINING_STATUSES)
    dur = func.coalesce(Chunk.duration, 0.0)
    with SessionLocal() as db:
        rows = (
            db.query(
                Chunk.discord_id,
                func.coalesce(func.sum(case((verified, 1), else_=0)), 0),
                func.coalesce(func.sum(case((verified, dur), else_=0.0)), 0.0),
                func.coalesce(func.sum(case((remaining, 1), else_=0)), 0),
                func.coalesce(func.sum(case((remaining, dur), else_=0.0)), 0.0),
                func.coalesce(
                    func.sum(case((and_(remaining, Chunk.duration.is_(None)), 1), else_=0)), 0),
            )
            .filter(Chunk.is_deleted == False)
            .group_by(Chunk.discord_id)
            .all()
        )
        owner_ids = [r[0] for r in rows]
        names = (
            {u.discord_id: u.username
             for u in db.query(User).filter(User.discord_id.in_(owner_ids)).all()}
            if owner_ids else {}
        )

    users, tot = [], {
        "verified_count": 0, "verified_seconds": 0.0,
        "remaining_count": 0, "remaining_seconds": 0.0, "remaining_unmeasured": 0,
    }
    for oid, vc, vs, rc, rs, ru in rows:
        if not (vc or rc):
            continue  # only rejected/deleted chunks — not part of the queue
        users.append({
            "owner_id": oid,
            "name": names.get(oid) or oid,
            "verified_count": int(vc or 0),
            "verified_seconds": float(vs or 0.0),
            "remaining_count": int(rc or 0),
            "remaining_seconds": float(rs or 0.0),
            "remaining_unmeasured": int(ru or 0),
        })
        tot["verified_count"] += int(vc or 0)
        tot["verified_seconds"] += float(vs or 0.0)
        tot["remaining_count"] += int(rc or 0)
        tot["remaining_seconds"] += float(rs or 0.0)
        tot["remaining_unmeasured"] += int(ru or 0)

    users.sort(key=lambda x: (x["verified_seconds"], x["remaining_seconds"]), reverse=True)
    return {"totals": tot, "users": users}


# ── Mutations ─────────────────────────────────────────────────────────────────

def _decide(viewer_id: str, owner_id: str, date: str, filename: str,
            new_status: str, text, labels=None) -> str:
    """
    Apply a validation decision. Returns one of:
      "ok"       — saved
      "denied"   — viewer can't access this owner
      "notfound" — chunk missing / deleted
      "conflict" — already decided by a DIFFERENT validator (lost lease race)

    `text` is None for reject (leave verified_transcription untouched), or the
    string to store for accept/issue (empty string allowed — an explicit empty
    save overrides any ASR default).
    """
    if not can_access(viewer_id, owner_id):
        return "denied"
    chunk_id = f"{owner_id}:{date}:{filename}"
    # Measure the duration BEFORE opening the write txn (header read off the DB
    # lock) so the Insights "verified" totals are exact without waiting on the
    # backfill, and without holding SQLite's write lock during file I/O.
    measured = measure_chunk_duration(owner_id, date, filename)
    with SessionLocal() as db:
        row = db.get(Chunk, chunk_id)
        if not row or row.is_deleted:
            return "notfound"
        # If this viewer SKIPPED this chunk, it's no longer theirs to decide — they
        # passed on it and it may already be someone else's. (Unreachable from the
        # UI, which drops skipped chunks from the buffer; this is the server guard.)
        if db.get(Skip, (viewer_id, chunk_id)):
            return "conflict"
        # Concurrency guard / last line of defense: if this chunk was already
        # decided by SOMEONE ELSE (e.g. our lease expired and they reclaimed and
        # decided it), don't silently clobber their work. Re-deciding our OWN
        # earlier decision (Back-navigation) is allowed.
        if (row.validation_status or "pending") != "pending" and row.validated_by not in (None, viewer_id):
            return "conflict"
        if text is not None:
            row.verified_transcription = text
        if labels is not None:
            for _k, _v in labels.items():
                setattr(row, _k, _v)
        if row.duration is None and measured is not None:
            row.duration = measured
        row.validation_status = new_status
        row.validated_at = datetime.now(timezone.utc)
        row.validated_by = viewer_id
        row.validation_claimed_by = None      # decision consumes the lease
        row.validation_claimed_at = None
        db.commit()
    return "ok"


def accept_chunk(viewer_id: str, owner_id: str, date: str, filename: str,
                 text: str, labels=None) -> str:
    """
    Mark a chunk verified and save the human transcription (empty text allowed).
    Requires at least ONE content label, and `normal` must be on its own — returns
    "nolabels" if the selection is invalid (the endpoint maps that to HTTP 400).
    """
    clean = _clean_labels(labels)
    if clean is None:
        return "nolabels"
    return _decide(viewer_id, owner_id, date, filename, "verified", text, labels=clean)


def reject_chunk(viewer_id: str, owner_id: str, date: str, filename: str) -> str:
    """Soft-reject a chunk: file stays on disk, status flips to rejected."""
    return _decide(viewer_id, owner_id, date, filename, "rejected", None)


def issue_chunk(viewer_id: str, owner_id: str, date: str, filename: str, text: str) -> str:
    """
    Flag a chunk as 'issue' — real speech that needs trimming/editing that can't
    be fixed by an edge-trim (e.g. noise in the MIDDLE). File stays on disk; any
    typed text/note is saved so the submissions view keeps the context. Empty
    text allowed. (Head/tail noise is handled in-place by trim_accept_chunk.)
    """
    return _decide(viewer_id, owner_id, date, filename, "issue", text)


def skip_chunk(viewer_id: str, owner_id: str, date: str, filename: str) -> str:
    """
    PASS on a chunk — "I can't judge this, let someone else try." NOT a decision:
    the chunk stays `pending` (no validated_by, no pay, still counts as remaining
    for others). We record the skip (so it's never re-served to this viewer and
    they can't re-decide it) and immediately RELEASE this viewer's lease so another
    validator can take it right away. Returns "ok" | "denied" | "notfound".
    Idempotent on the (viewer, chunk) PK.
    """
    if not can_access(viewer_id, owner_id):
        return "denied"
    chunk_id = f"{owner_id}:{date}:{filename}"
    with SessionLocal() as db:
        row = db.get(Chunk, chunk_id)
        if not row or row.is_deleted:
            return "notfound"
        if not db.get(Skip, (viewer_id, chunk_id)):
            db.add(Skip(viewer_id=viewer_id, chunk_id=chunk_id))
        # Release only OUR still-pending lease — never touch a chunk someone has
        # since decided (its lease was already consumed).
        if (row.validation_status or "pending") == "pending" and row.validation_claimed_by == viewer_id:
            row.validation_claimed_by = None
            row.validation_claimed_at = None
        db.commit()
    return "ok"


def trim_accept_chunk(viewer_id: str, owner_id: str, date: str, filename: str,
                      start: float, end: float, text: str, labels=None):
    """
    One-step "Trim & Accept": losslessly write a trimmed copy of a chunk as
    "{stem}_updated.wav", soft-delete the original, and register the new file as a
    VERIFIED chunk — carrying over the raw ASR transcription, the human text, the
    exact new duration, and derived_from (provenance / undo).

    Edge-trim only: `start`/`end` are the kept window in SECONDS, after the
    validator cut dead air / noise off the head and/or tail. (Middle problems are
    a reject/issue, not a trim.) Returns (result, new_chunk | None):
      "ok"       — trimmed + verified; new_chunk is the serialized replacement
      "nolabels" — no content label chosen (or `normal` mixed with others)
      "denied"   — viewer can't access this owner
      "notfound" — original missing/deleted, or the trim range was empty/unreadable
      "conflict" — original already decided by a DIFFERENT validator

    Like _decide()/claim_pending_window(), the slow WAV read+write happens with NO
    DB transaction open, so it never holds SQLite's single write lock during the
    network-volume file I/O: a short read txn validates + captures the source, the
    file work runs off the lock, then a short write txn flips is_deleted and
    upserts the new row.
    """
    if not can_access(viewer_id, owner_id):
        return "denied", None
    # Verifying requires >=1 label (normal exclusive) — fail BEFORE writing a file.
    clean = _clean_labels(labels)
    if clean is None:
        return "nolabels", None

    chunk_id = f"{owner_id}:{date}:{filename}"

    # 1) Validate the original and grab its path + ASR text, then drop the lock.
    with SessionLocal() as db:
        row = db.get(Chunk, chunk_id)
        if not row or row.is_deleted:
            return "notfound", None
        # Only an undecided chunk (or one we ourselves last touched) may be
        # trimmed — never clobber another validator's decision.
        if (row.validation_status or "pending") != "pending" and row.validated_by not in (None, viewer_id):
            return "conflict", None
        src_path = _resolve_filepath(row)      # heals a stale path if needed
        asr_text = row.transcription
    if not src_path:
        return "notfound", None

    # 2) Trim on disk, OFF the lock, into the canonical ID-folder location.
    stem = filename[:-4] if filename.lower().endswith(".wav") else filename
    new_filename = f"{stem}_updated.wav"
    new_id = f"{owner_id}:{date}:{new_filename}"
    new_path = os.path.join(RECORDINGS_PATH, owner_id, "chunks", date, new_filename)
    os.makedirs(os.path.dirname(new_path), exist_ok=True)
    new_duration = trim_wav(src_path, new_path, float(start), float(end))
    if new_duration is None:
        return "notfound", None                # empty/invalid range or unreadable

    # 3) Short write txn: soft-delete the original, upsert the verified new chunk.
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        orig = db.get(Chunk, chunk_id)
        # Re-check the decision race that could have closed while we trimmed.
        bad = (not orig or orig.is_deleted)
        conflict = (not bad
                    and (orig.validation_status or "pending") != "pending"
                    and orig.validated_by not in (None, viewer_id))
        if bad or conflict:
            try:
                os.remove(new_path)            # don't leave an orphan file behind
            except OSError:
                pass
            return ("notfound" if bad else "conflict"), None

        orig.is_deleted = True
        orig.validation_claimed_by = None
        orig.validation_claimed_at = None

        new_row = db.get(Chunk, new_id)        # supersede a prior _updated, if any
        if new_row is None:
            new_row = Chunk(id=new_id, discord_id=owner_id, date=date,
                            filename=new_filename, filepath=new_path)
            db.add(new_row)
        else:
            new_row.filepath = new_path
        new_row.is_deleted = False
        new_row.transcription = asr_text
        new_row.verified_transcription = text
        new_row.duration = new_duration
        new_row.derived_from = filename
        for _k, _v in clean.items():
            setattr(new_row, _k, _v)
        new_row.validation_status = "verified"
        new_row.validated_at = now
        new_row.validated_by = viewer_id
        new_row.validation_claimed_by = None
        new_row.validation_claimed_at = None
        db.commit()
        new_chunk = _serialize(new_row)
    return "ok", new_chunk


def resolve_chunk_file(viewer_id: str, owner_id: str, date: str, filename: str):
    """On-disk path for a chunk if the viewer may access it and it exists, else None."""
    if not can_access(viewer_id, owner_id):
        return None
    chunk_id = f"{owner_id}:{date}:{filename}"
    with SessionLocal() as db:
        row = db.get(Chunk, chunk_id)
        if not row or row.is_deleted:
            return None
        return _resolve_filepath(row)


# ── Wallet / earnings / withdrawals ───────────────────────────────────────────
# Earnings are NEVER stored — they're a pure function of the audio the user has
# validated (validated_by == them, any decision) times the global rate. The only
# persisted state is the `withdrawals` ledger and the user's CliQ alias. This
# keeps the number always-correct (re-decides, trims, etc. flow straight through)
# while withdrawn amounts stay LOCKED, so money already requested/paid can never
# be clawed back by a later edit — `available` just floors at 0.

def _validated_seconds(db, user_id: str) -> float:
    """Total audio seconds this user has validated (any decision counts)."""
    secs = (
        db.query(func.coalesce(func.sum(func.coalesce(Chunk.duration, 0.0)), 0.0))
        .filter(
            Chunk.validated_by == user_id,
            Chunk.is_deleted == False,
            Chunk.validation_status.in_(PAID_STATUSES),
        )
        .scalar()
    )
    return float(secs or 0.0)


def _withdrawn_usd(db, user_id: str) -> float:
    """Dollars locked by the user's pending + paid withdrawals (rejected frees up)."""
    amt = (
        db.query(func.coalesce(func.sum(Withdrawal.amount_usd), 0.0))
        .filter(
            Withdrawal.user_id == user_id,
            Withdrawal.status.in_(SPENT_WITHDRAWAL_STATUSES),
        )
        .scalar()
    )
    return float(amt or 0.0)


def _serialize_withdrawal(w) -> dict:
    return {
        "id": w.id,
        "amount_usd": round(float(w.amount_usd or 0.0), 2),
        "status": w.status or "pending",
        "cliq_alias": w.cliq_alias,
        "created_at": w.created_at.isoformat() if w.created_at else None,
        "decided_at": w.decided_at.isoformat() if w.decided_at else None,
        "note": w.note,
    }


def get_wallet(user_id: str) -> dict:
    """
    The validator's full wallet: live earnings, what's been withdrawn, what's
    available now, the CliQ alias on file, and their withdrawal history. All money
    in USD rounded to cents; durations in seconds (the frontend formats them).
    """
    with SessionLocal() as db:
        secs = _validated_seconds(db, user_id)
        withdrawn = _withdrawn_usd(db, user_id)
        paid = (
            db.query(func.coalesce(func.sum(Withdrawal.amount_usd), 0.0))
            .filter(Withdrawal.user_id == user_id, Withdrawal.status == "paid")
            .scalar()
        )
        pending = (
            db.query(Withdrawal)
            .filter(Withdrawal.user_id == user_id, Withdrawal.status == "pending")
            .first()
        )
        txns = (
            db.query(Withdrawal)
            .filter(Withdrawal.user_id == user_id)
            .order_by(Withdrawal.created_at.desc())
            .all()
        )
        profile = db.get(PayoutProfile, user_id)
        alias = profile.cliq_alias if profile else None

    earned = round(secs * PAY_RATE_PER_SEC, 2)
    available = round(max(0.0, earned - withdrawn), 2)
    return {
        "validated_seconds": secs,
        "earned_usd": earned,
        "withdrawn_usd": round(withdrawn, 2),
        "paid_usd": round(float(paid or 0.0), 2),
        "available_usd": available,
        "min_withdrawal_usd": round(MIN_WITHDRAWAL_USD, 2),
        "has_pending": pending is not None,
        "can_withdraw": available >= MIN_WITHDRAWAL_USD and pending is None,
        "cliq_alias": alias,
        "rate_per_hour": PAY_PER_HOUR,
        "transactions": [_serialize_withdrawal(w) for w in txns],
    }


def _clean_alias(alias):
    """Normalize a CliQ alias, or None if unusable. Permissive (handles are short
    alphanumerics, phone numbers, or IBANs) — just trim, cap length, no controls."""
    alias = (alias or "").strip()
    if not alias or len(alias) > 64:
        return None
    if any(ord(c) < 32 for c in alias):
        return None
    return alias


def set_cliq_alias(user_id: str, alias) -> str | None:
    """Set/update the user's CliQ alias. Returns the cleaned value, or None if
    the input was invalid (the endpoint maps that to HTTP 400)."""
    clean = _clean_alias(alias)
    if clean is None:
        return None
    with SessionLocal() as db:
        p = db.get(PayoutProfile, user_id)
        if p is None:
            db.add(PayoutProfile(discord_id=user_id, cliq_alias=clean))
        else:
            p.cliq_alias = clean
            p.updated_at = datetime.now(timezone.utc)
        db.commit()
    return clean


def request_withdrawal(user_id: str):
    """
    Create a pending withdrawal for the user's FULL available balance. Returns
    (result, wallet):
      "ok"          — created
      "no_alias"    — no CliQ alias on file (client should collect it, then retry)
      "below_min"   — available < MIN_WITHDRAWAL_USD
      "has_pending" — a pending withdrawal already exists (one at a time)
    The amount + alias are snapshotted onto the row at creation time.
    """
    result = "ok"
    with SessionLocal() as db:
        has_pending = db.query(Withdrawal).filter(
            Withdrawal.user_id == user_id, Withdrawal.status == "pending"
        ).first() is not None
        secs = _validated_seconds(db, user_id)
        withdrawn = _withdrawn_usd(db, user_id)
        earned = round(secs * PAY_RATE_PER_SEC, 2)
        available = round(max(0.0, earned - withdrawn), 2)
        profile = db.get(PayoutProfile, user_id)
        alias = profile.cliq_alias if profile else None

        # Order matters: a pending request and the minimum are hard blocks, so
        # report those before asking for an alias (no point collecting one if they
        # can't withdraw yet). The alias prompt is for an eligible first-timer.
        if has_pending:
            result = "has_pending"
        elif available < MIN_WITHDRAWAL_USD:
            result = "below_min"
        elif not alias:
            result = "no_alias"
        else:
            db.add(Withdrawal(
                user_id=user_id, amount_usd=available, seconds_snapshot=secs,
                cliq_alias=alias, status="pending",
            ))
            db.commit()
    return result, get_wallet(user_id)


# ── Admin: payout approval ────────────────────────────────────────────────────

def list_payouts(history_limit: int = 50) -> dict:
    """
    For the admin Payouts panel: every PENDING withdrawal (oldest first — act on
    these) plus recent decided ones (history). Each row carries the validator's
    display name and the alias to pay.
    """
    with SessionLocal() as db:
        pending = (
            db.query(Withdrawal).filter(Withdrawal.status == "pending")
            .order_by(Withdrawal.created_at.asc()).all()
        )
        history = (
            db.query(Withdrawal).filter(Withdrawal.status != "pending")
            .order_by(Withdrawal.decided_at.desc()).limit(history_limit).all()
        )
        ids = {w.user_id for w in pending} | {w.user_id for w in history}
        names = (
            {u.discord_id: u.username
             for u in db.query(User).filter(User.discord_id.in_(ids)).all()}
            if ids else {}
        )

    def _row(w):
        d = _serialize_withdrawal(w)
        d["user_id"] = w.user_id
        d["user_name"] = names.get(w.user_id) or w.user_id
        d["decided_by"] = w.decided_by
        return d

    return {
        "pending": [_row(w) for w in pending],
        "history": [_row(w) for w in history],
        "pending_total_usd": round(sum(float(w.amount_usd or 0.0) for w in pending), 2),
    }


def _decide_payout(admin_id: str, withdrawal_id: int, new_status: str, note=None) -> str:
    """Flip a pending withdrawal to paid/rejected. Returns 'ok' | 'notfound' |
    'notpending' (already decided — lets the admin UI skip a double action)."""
    with SessionLocal() as db:
        w = db.get(Withdrawal, withdrawal_id)
        if not w:
            return "notfound"
        if (w.status or "pending") != "pending":
            return "notpending"
        w.status = new_status
        w.decided_at = datetime.now(timezone.utc)
        w.decided_by = admin_id
        if note is not None:
            w.note = note
        db.commit()
    return "ok"


def approve_payout(admin_id: str, withdrawal_id: int) -> str:
    """Mark a pending withdrawal paid (after the admin has actually sent the money)."""
    return _decide_payout(admin_id, withdrawal_id, "paid")


def reject_payout(admin_id: str, withdrawal_id: int, note=None) -> str:
    """Reject a pending withdrawal — its amount returns to the user's available."""
    return _decide_payout(admin_id, withdrawal_id, "rejected", note=note)


# ── Admin: broadcast DMs ──────────────────────────────────────────────────────
# Persistence for the admin "message all users" feature. The Discord REST calls
# and the one-at-a-time background job live in discord_bot.py; this module only
# owns the durable record (the broadcasts table) and the recipient name lookups.

def list_all_users() -> list:
    """Every web-app user (logged in at least once), for the broadcast recipient
    picker. Each: {id, name}. Sorted by display name."""
    with SessionLocal() as db:
        users = db.query(User).order_by(func.lower(User.username).asc()).all()
        return [{"id": u.discord_id, "name": u.username or u.discord_id} for u in users]


def usernames_for(ids) -> dict:
    """Map {discord_id: username} for the given ids (known web-app users only).
    Raw pasted IDs with no user row simply won't appear — discord_bot resolves
    those names from Discord at send time."""
    ids = [i for i in ids if i]
    if not ids:
        return {}
    with SessionLocal() as db:
        return {u.discord_id: u.username
                for u in db.query(User).filter(User.discord_id.in_(ids)).all()}


def _serialize_broadcast(b) -> dict:
    sent = b.sent_count or 0
    failed = b.failed_count or 0
    return {
        "id": b.id,
        "status": b.status,
        "total": b.total or 0,
        "done": sent + failed,
        "sent": sent,
        "failed": failed,
        "results": json.loads(b.results) if b.results else [],
        "error": None,
        "message": b.message,
        "sent_by": b.sent_by,
        "created_at": b.created_at.isoformat() if b.created_at else None,
        "finished_at": b.finished_at.isoformat() if b.finished_at else None,
    }


def create_broadcast(admin_id: str, message: str, total: int) -> int:
    """Insert a `running` broadcast row and return its id (the job id)."""
    with SessionLocal() as db:
        b = Broadcast(sent_by=admin_id, message=message, total=total, status="running")
        db.add(b)
        db.commit()
        db.refresh(b)
        return b.id


def finalize_broadcast(broadcast_id: int, sent: int, failed: int, results) -> None:
    """Record the final per-recipient outcome and flip the row to `done`."""
    with SessionLocal() as db:
        b = db.get(Broadcast, broadcast_id)
        if not b:
            return
        b.sent_count = sent
        b.failed_count = failed
        b.status = "done"
        b.results = json.dumps(results, ensure_ascii=False)
        b.finished_at = datetime.now(timezone.utc)
        db.commit()


def get_broadcast(broadcast_id: int) -> dict | None:
    """Serialized broadcast row — the status endpoint's fallback once a job is no
    longer live in memory (finished, or after a restart)."""
    with SessionLocal() as db:
        b = db.get(Broadcast, broadcast_id)
        return _serialize_broadcast(b) if b else None


def list_broadcasts(limit: int = 20) -> list:
    """Recent broadcasts (newest first) for the admin history panel."""
    with SessionLocal() as db:
        rows = (db.query(Broadcast)
                .order_by(Broadcast.created_at.desc()).limit(limit).all())
        return [_serialize_broadcast(b) for b in rows]


# ── Validator leaderboard ─────────────────────────────────────────────────────
# A competition board ranking validators by the AUDIO THEY VALIDATED (validated_by
# + duration, all decisions) — the SAME basis the Wallet pays on, so climbing the
# board == earning more. This is per-VALIDATOR effort, distinct from the admin
# Dataset page (which is per voice-OWNER completion). Time windows reset in local
# (Jordan) time so "today" matches the real day, not UTC.

LEADERBOARD_WINDOWS = ("today", "week", "month", "all")
# Jordan is UTC+3 year-round (no DST since 2022); a whole-hour offset is exact.
LEADERBOARD_UTC_OFFSET_HOURS = float(os.environ.get("LEADERBOARD_UTC_OFFSET_HOURS", "3"))


def _window_start_utc(window: str):
    """Naive-UTC datetime for the start of `window`, computed in local time so the
    day/week/month resets at LOCAL midnight. None for all-time. Stored validated_at
    is naive UTC, so we return the same to compare directly."""
    if window not in ("today", "week", "month"):
        return None
    offset = timedelta(hours=LEADERBOARD_UTC_OFFSET_HOURS)
    now_local = datetime.now(timezone.utc).replace(tzinfo=None) + offset
    base = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    if window == "week":
        base = base - timedelta(days=base.weekday())   # Monday
    elif window == "month":
        base = base.replace(day=1)
    return base - offset                                # back to naive UTC


def has_validated(user_id: str) -> bool:
    """Leaderboard view-gate: has this user made at least one decision (lifetime)?
    Any decision counts (accept/reject/issue) — same basis as the score — so only
    active participants get to see the competition."""
    with SessionLocal() as db:
        return db.query(Chunk.id).filter(
            Chunk.validated_by == user_id,
            Chunk.validation_status.in_(PAID_STATUSES),
        ).first() is not None


def get_leaderboard(window: str, viewer_id: str) -> dict:
    """
    Ranked validators for `window` (today|week|month|all): each
    {rank, id, name, avatar, seconds, chunks}, sorted by validated audio time
    desc. Also returns the viewer's own row (`me`, even if outside the list view)
    and the group totals. Only validators with activity in the window appear.
    """
    if window not in LEADERBOARD_WINDOWS:
        window = "today"
    start = _window_start_utc(window)

    with SessionLocal() as db:
        q = db.query(
            Chunk.validated_by.label("uid"),
            func.coalesce(func.sum(Chunk.duration), 0.0).label("seconds"),
            func.count(Chunk.id).label("chunks"),
        ).filter(
            Chunk.validated_by.isnot(None),
            Chunk.validation_status.in_(PAID_STATUSES),
        )
        if start is not None:
            q = q.filter(Chunk.validated_at >= start)
        rows = q.group_by(Chunk.validated_by).all()

        ids = [r.uid for r in rows]
        users = (
            {u.discord_id: u for u in db.query(User).filter(User.discord_id.in_(ids)).all()}
            if ids else {}
        )

    # Rank by audio time, then chunk count as the tiebreaker.
    rows.sort(key=lambda r: (float(r.seconds or 0.0), int(r.chunks or 0)), reverse=True)
    entries = []
    for i, r in enumerate(rows, start=1):
        u = users.get(r.uid)
        entries.append({
            "rank": i,
            "id": r.uid,
            "name": (u.username if u else None) or r.uid,
            "avatar": (u.avatar_url if u else None),
            "seconds": round(float(r.seconds or 0.0), 1),
            "chunks": int(r.chunks or 0),
        })

    me = next((e for e in entries if e["id"] == viewer_id), None)
    return {
        "window": window,
        "entries": entries,
        "me": me,
        "participants": len(entries),
        "total_seconds": round(sum(e["seconds"] for e in entries), 1),
        "total_chunks": sum(e["chunks"] for e in entries),
    }
