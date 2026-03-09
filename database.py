import os
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, Column, String, Boolean, DateTime, Text, ForeignKey, UniqueConstraint, Float
from sqlalchemy.orm import declarative_base, sessionmaker

DB_PATH = os.environ.get("DB_PATH", "/app/db/recordings.db")
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

RECORDINGS_PATH = os.environ.get("RECORDINGS_PATH", "/app/recordings")


class User(Base):
    __tablename__ = "users"

    discord_id = Column(String, primary_key=True)
    username = Column(String, nullable=False)
    avatar_url = Column(String)
    last_login = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class AudioFile(Base):
    __tablename__ = "audio_files"

    id = Column(String, primary_key=True)  # e.g. discord_id + filename
    discord_id = Column(String, ForeignKey("users.discord_id"), nullable=False)
    username = Column(String)
    filename = Column(String, nullable=False)
    filepath = Column(Text, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    is_deleted = Column(Boolean, default=False)


class Chunk(Base):
    """One row per processed VAD chunk file."""
    __tablename__ = "chunks"
    __table_args__ = (
        UniqueConstraint("discord_id", "date", "filename", name="uq_chunk"),
    )

    id = Column(String, primary_key=True)          # "{discord_id}:{date}:{filename}"
    discord_id = Column(String, ForeignKey("users.discord_id"), nullable=False)
    date = Column(String, nullable=False)           # "YYYY-MM-DD"
    filename = Column(String, nullable=False)       # "chunk_001.wav"
    filepath = Column(Text, nullable=False)         # absolute path on disk
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    is_deleted = Column(Boolean, default=False)
    transcription = Column(Text, nullable=True, default=None)   # whisper output
    transcribed_at = Column(DateTime, nullable=True, default=None)
    claimed_by = Column(String, nullable=True, default=None)    # machine_id holding this chunk
    claimed_at = Column(DateTime, nullable=True, default=None)  # when the claim was made


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    Base.metadata.create_all(engine)


def upsert_user(discord_id: str, username: str, avatar_url: str):
    with SessionLocal() as db:
        user = db.get(User, discord_id)
        if user:
            user.username = username
            user.avatar_url = avatar_url
            user.last_login = datetime.now(timezone.utc)
        else:
            user = User(
                discord_id=discord_id,
                username=username,
                avatar_url=avatar_url,
                last_login=datetime.now(timezone.utc)
            )
            db.add(user)
        db.commit()


def get_user(discord_id: str):
    with SessionLocal() as db:
        return db.get(User, discord_id)


def log_audio_file(discord_id: str, username: str, filepath: str):
    filename = os.path.basename(filepath)
    file_id = f"{discord_id}_{filename}"
    with SessionLocal() as db:
        existing = db.get(AudioFile, file_id)
        if not existing:
            audio = AudioFile(
                id=file_id,
                discord_id=discord_id,
                username=username,
                filename=filename,
                filepath=filepath,
                is_deleted=False
            )
            db.add(audio)
            db.commit()


def get_user_audio_files(discord_id: str):
    with SessionLocal() as db:
        return db.query(AudioFile).filter(
            AudioFile.discord_id == discord_id,
            AudioFile.is_deleted == False
        ).all()


# ── Chunk helpers ─────────────────────────────────────────────────────────────

def register_chunk(discord_id: str, date: str, filename: str, filepath: str):
    """Insert a chunk row if it doesn't already exist."""
    chunk_id = f"{discord_id}:{date}:{filename}"
    with SessionLocal() as db:
        if not db.get(Chunk, chunk_id):
            db.add(Chunk(
                id=chunk_id,
                discord_id=discord_id,
                date=date,
                filename=filename,
                filepath=filepath,
            ))
            db.commit()


def _heal_filepath(row_id: str, new_path: str):
    """
    Silently update a stale filepath in the DB so future lookups are instant.
    Called when a stored path no longer exists but the file was found at the
    new ID-only folder location (after the username_id -> id rename migration).
    """
    with SessionLocal() as db:
        row = db.get(Chunk, row_id)
        if row:
            row.filepath = new_path
            db.commit()


def _resolve_filepath(row) -> str | None:
    """
    Returns the real filepath for a chunk row, healing stale paths if needed.
    Returns None if the file genuinely doesn't exist anywhere.
    """
    # Fast path — stored path is still valid
    if os.path.exists(row.filepath):
        return row.filepath

    # Slow path — try the new ID-only folder structure
    new_path = os.path.join(
        RECORDINGS_PATH, row.discord_id, "chunks", row.date, row.filename
    )
    if os.path.exists(new_path):
        _heal_filepath(row.id, new_path)
        return new_path

    # File genuinely missing
    return None


def get_chunks_for_user(discord_id: str) -> dict:
    """
    Returns { "YYYY-MM-DD": [{"filename": "chunk_001.wav", "transcription": "..."}, ...] }
    ordered date-desc, filename-asc.
    Only includes rows where is_deleted=False AND the file still exists on disk.
    Heals stale filepaths transparently for rows created before the folder rename.
    """
    with SessionLocal() as db:
        rows = (
            db.query(Chunk)
            .filter(Chunk.discord_id == discord_id, Chunk.is_deleted == False)
            .order_by(Chunk.date.desc(), Chunk.filename.asc())
            .all()
        )
    result: dict = {}
    for row in rows:
        if _resolve_filepath(row) is not None:
            result.setdefault(row.date, []).append({
                "filename": row.filename,
                "transcription": row.transcription,
            })
    return result


CLAIM_TIMEOUT_MINUTES = 5


def release_stale_claims():
    """
    Reset claimed_by/claimed_at for any chunk that was claimed more than
    CLAIM_TIMEOUT_MINUTES ago but still has no transcription.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=CLAIM_TIMEOUT_MINUTES)
    with SessionLocal() as db:
        stale = (
            db.query(Chunk)
            .filter(
                Chunk.claimed_by != None,
                Chunk.claimed_at < cutoff,
                Chunk.transcription == None,
            )
            .all()
        )
        count = len(stale)
        for row in stale:
            row.claimed_by = None
            row.claimed_at = None
        if count:
            db.commit()
    return count


def claim_chunks(machine_id: str, batch_size: int = 20) -> list:
    """
    Atomically claims up to batch_size unclaimed, un-transcribed chunks for
    machine_id and returns them.
    """
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        rows = (
            db.query(Chunk)
            .filter(
                Chunk.is_deleted == False,
                Chunk.transcription == None,
                Chunk.claimed_by == None,
            )
            .order_by(Chunk.date.asc(), Chunk.filename.asc())
            .limit(batch_size)
            .all()
        )
        result = []
        needs_commit = False
        
        for row in rows:
            if _resolve_filepath(row) is not None:
                # File exists, claim it normally
                row.claimed_by = machine_id
                row.claimed_at = now
                result.append({
                    "discord_id": row.discord_id,
                    "date": row.date,
                    "filename": row.filename,
                })
                needs_commit = True
            else:
                # FILE IS MISSING: Mark it as deleted to prevent queue jams
                row.is_deleted = True
                needs_commit = True

        if needs_commit:
            db.commit()
            
    return result

def get_pending_chunks() -> list:
    """
    Returns all chunks that have no transcription yet (for the /pending endpoint).
    """
    with SessionLocal() as db:
        rows = (
            db.query(Chunk)
            .filter(Chunk.is_deleted == False, Chunk.transcription == None)
            .order_by(Chunk.date.asc(), Chunk.filename.asc())
            .all()
        )
    result = []
    for row in rows:
        if _resolve_filepath(row) is not None:
            result.append({
                "discord_id": row.discord_id,
                "date": row.date,
                "filename": row.filename,
                "claimed_by": row.claimed_by,
            })
    return result


def set_transcription(discord_id: str, date: str, filename: str, text: str) -> bool:
    """
    Save transcription text for a chunk and clear its claim.
    """
    chunk_id = f"{discord_id}:{date}:{filename}"
    with SessionLocal() as db:
        row = db.get(Chunk, chunk_id)
        if not row or row.discord_id != discord_id or row.is_deleted:
            return False
        row.transcription = text
        row.transcribed_at = datetime.now(timezone.utc)
        row.claimed_by = None
        row.claimed_at = None
        db.commit()
    return True


def delete_chunk(discord_id: str, date: str, filename: str) -> str | None:
    """
    Hard-delete: removes the file from disk and marks the DB row is_deleted=True.
    Heals stale filepath before attempting deletion if needed.
    Returns the filepath on success, None if not found.
    """
    chunk_id = f"{discord_id}:{date}:{filename}"
    with SessionLocal() as db:
        row = db.get(Chunk, chunk_id)
        if not row or row.discord_id != discord_id:
            return None

        filepath = _resolve_filepath(row)
        try:
            if filepath and os.path.exists(filepath):
                os.remove(filepath)
        except OSError:
            pass

        row.is_deleted = True
        db.commit()
    return filepath or row.filepath
