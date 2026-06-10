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
from datetime import datetime, timezone, timedelta

from sqlalchemy import or_, update, select, func, case
from database import (
    SessionLocal, Chunk, AccessGrant, User, _resolve_filepath,
    measure_chunk_duration, trim_wav, RECORDINGS_PATH, VALIDATION_LEASE_MINUTES,
)

# Statuses that count as "still to validate" for the Insights remaining total.
# issue is included on purpose (flagged-but-not-finished work — see get_insights).
REMAINING_STATUSES = ["pending", "issue"]


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

    with SessionLocal() as db:
        # The next claimable window: pending, not deleted, and either unclaimed or
        # with an expired lease. (Rows already leased by this viewer are excluded
        # so each fetch returns genuinely new work — they're still held, just not
        # re-sent.) Ordered owner-stable by date -> filename.
        candidates = (
            select(Chunk.id)
            .where(
                Chunk.discord_id == owner_id,
                Chunk.is_deleted == False,
                Chunk.validation_status == "pending",
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

        total = (
            db.query(Chunk)
            .filter(
                Chunk.discord_id == owner_id,
                Chunk.is_deleted == False,
                Chunk.validation_status == "pending",
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


# ── Mutations ─────────────────────────────────────────────────────────────────

def _decide(viewer_id: str, owner_id: str, date: str, filename: str,
            new_status: str, text) -> str:
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
        # Concurrency guard / last line of defense: if this chunk was already
        # decided by SOMEONE ELSE (e.g. our lease expired and they reclaimed and
        # decided it), don't silently clobber their work. Re-deciding our OWN
        # earlier decision (Back-navigation) is allowed.
        if (row.validation_status or "pending") != "pending" and row.validated_by not in (None, viewer_id):
            return "conflict"
        if text is not None:
            row.verified_transcription = text
        if row.duration is None and measured is not None:
            row.duration = measured
        row.validation_status = new_status
        row.validated_at = datetime.now(timezone.utc)
        row.validated_by = viewer_id
        row.validation_claimed_by = None      # decision consumes the lease
        row.validation_claimed_at = None
        db.commit()
    return "ok"


def accept_chunk(viewer_id: str, owner_id: str, date: str, filename: str, text: str) -> str:
    """Mark a chunk verified and save the human transcription. Empty text allowed."""
    return _decide(viewer_id, owner_id, date, filename, "verified", text)


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


def trim_accept_chunk(viewer_id: str, owner_id: str, date: str, filename: str,
                      start: float, end: float, text: str):
    """
    One-step "Trim & Accept": losslessly write a trimmed copy of a chunk as
    "{stem}_updated.wav", soft-delete the original, and register the new file as a
    VERIFIED chunk — carrying over the raw ASR transcription, the human text, the
    exact new duration, and derived_from (provenance / undo).

    Edge-trim only: `start`/`end` are the kept window in SECONDS, after the
    validator cut dead air / noise off the head and/or tail. (Middle problems are
    a reject/issue, not a trim.) Returns (result, new_chunk | None):
      "ok"       — trimmed + verified; new_chunk is the serialized replacement
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
