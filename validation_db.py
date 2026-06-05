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
from datetime import datetime, timezone

from database import SessionLocal, Chunk, AccessGrant, _resolve_filepath


# ── Access grants (delegated validation) ──────────────────────────────────────

def grant_access(owner_id: str, delegate_id: str):
    """Allow `delegate_id` to validate `owner_id`'s chunks. Idempotent."""
    gid = f"{owner_id}:{delegate_id}"
    with SessionLocal() as db:
        if not db.get(AccessGrant, gid):
            db.add(AccessGrant(id=gid, owner_id=owner_id, delegate_id=delegate_id))
            db.commit()


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

def get_validation_state(viewer_id: str) -> list:
    """
    Ordered list for the sequential validate page: every `pending` + `verified`
    chunk the viewer can access (rejected/hard-deleted excluded), ordered
    owner -> date -> filename so the queue is stable and chronological. Only
    rows whose .wav still exists on disk are returned.
    """
    owners = get_accessible_owner_ids(viewer_id)
    with SessionLocal() as db:
        rows = (
            db.query(Chunk)
            .filter(
                Chunk.discord_id.in_(owners),
                Chunk.is_deleted == False,
                Chunk.validation_status.in_(["pending", "verified"]),
            )
            .order_by(Chunk.discord_id.asc(), Chunk.date.asc(), Chunk.filename.asc())
            .all()
        )
        return [_serialize(r) for r in rows if _resolve_filepath(r) is not None]


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


# ── Mutations ─────────────────────────────────────────────────────────────────

def accept_chunk(viewer_id: str, owner_id: str, date: str, filename: str, text: str) -> bool:
    """Mark a chunk verified and save the human transcription. Empty text allowed."""
    if not can_access(viewer_id, owner_id):
        return False
    chunk_id = f"{owner_id}:{date}:{filename}"
    with SessionLocal() as db:
        row = db.get(Chunk, chunk_id)
        if not row or row.is_deleted:
            return False
        row.verified_transcription = text
        row.validation_status = "verified"
        row.validated_at = datetime.now(timezone.utc)
        row.validated_by = viewer_id
        db.commit()
    return True


def reject_chunk(viewer_id: str, owner_id: str, date: str, filename: str) -> bool:
    """Soft-reject a chunk: file stays on disk, status flips to rejected."""
    if not can_access(viewer_id, owner_id):
        return False
    chunk_id = f"{owner_id}:{date}:{filename}"
    with SessionLocal() as db:
        row = db.get(Chunk, chunk_id)
        if not row or row.is_deleted:
            return False
        row.validation_status = "rejected"
        row.validated_at = datetime.now(timezone.utc)
        row.validated_by = viewer_id
        db.commit()
    return True


def issue_chunk(viewer_id: str, owner_id: str, date: str, filename: str, text: str) -> bool:
    """
    Flag a chunk as 'issue' — real speech that needs trimming/editing. File
    stays on disk; any typed text/note is saved (so a future trim tool and the
    submissions view keep the context). Empty text allowed.
    """
    if not can_access(viewer_id, owner_id):
        return False
    chunk_id = f"{owner_id}:{date}:{filename}"
    with SessionLocal() as db:
        row = db.get(Chunk, chunk_id)
        if not row or row.is_deleted:
            return False
        row.verified_transcription = text
        row.validation_status = "issue"
        row.validated_at = datetime.now(timezone.utc)
        row.validated_by = viewer_id
        db.commit()
    return True


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
