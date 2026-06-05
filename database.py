import os
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, Column, String, Boolean, DateTime, Text, ForeignKey, UniqueConstraint, Float, inspect, text
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

    # ── Validation / dataset-building fields ──────────────────────────────────
    # validation_status: "pending" (untouched) | "verified" (human-accepted) |
    # "rejected" (human soft-rejected — file kept on disk, hidden everywhere).
    validation_status = Column(String, default="pending")
    # Human-confirmed transcription. Kept SEPARATE from `transcription` (the raw
    # ASR/whisper output) so the dataset can compare machine vs human text.
    verified_transcription = Column(Text, nullable=True, default=None)
    validated_at = Column(DateTime, nullable=True, default=None)
    validated_by = Column(String, nullable=True, default=None)  # discord_id who acted


class AccessGrant(Base):
    """
    Lets `delegate_id` validate `owner_id`'s chunks. No UI yet — rows are
    created via grant_access() (or directly in the DB). The validation queue
    and audio endpoints consult this so a user can delegate their voices.
    """
    __tablename__ = "access_grants"
    __table_args__ = (
        UniqueConstraint("owner_id", "delegate_id", name="uq_grant"),
    )

    id = Column(String, primary_key=True)            # "{owner_id}:{delegate_id}"
    owner_id = Column(String, nullable=False)        # whose voices are shared
    delegate_id = Column(String, nullable=False)     # who is granted access
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# Columns that may be missing on an older live `chunks` table. Maps column name
# -> the type clause used in "ALTER TABLE chunks ADD COLUMN <name> <clause>".
_CHUNK_COLUMN_MIGRATIONS = {
    "validation_status":      "VARCHAR DEFAULT 'pending'",
    "verified_transcription": "TEXT",
    "validated_at":           "DATETIME",
    "validated_by":           "VARCHAR",
}


def _run_light_migrations():
    """
    Idempotent, additive-only schema healing for SQLite.

    create_all() creates *missing tables* but never alters an existing one, so
    columns added to a model after its table already exists on disk would be
    silently absent. This adds any such columns via ALTER TABLE ADD COLUMN.
    Safe to run on every startup: it only adds columns that don't yet exist and
    never drops, renames, or rewrites data. (When non-additive changes are
    needed later, graduate to a real migration tool like Alembic.)
    """
    insp = inspect(engine)
    if "chunks" not in insp.get_table_names():
        return
    existing = {col["name"] for col in insp.get_columns("chunks")}
    missing = {n: ddl for n, ddl in _CHUNK_COLUMN_MIGRATIONS.items() if n not in existing}
    if not missing:
        return
    with engine.begin() as conn:
        for name, ddl in missing.items():
            conn.execute(text(f"ALTER TABLE chunks ADD COLUMN {name} {ddl}"))


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    Base.metadata.create_all(engine)
    _run_light_migrations()


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
            .filter(
                Chunk.discord_id == discord_id,
                Chunk.is_deleted == False,
                # Hide both rejected and issue clips — dashboard shows only
                # pending (awaiting review) and verified (accepted) chunks.
                Chunk.validation_status.in_(["pending", "verified"]),
            )
            .order_by(Chunk.date.desc(), Chunk.filename.asc())
            .all()
        )
    result: dict = {}
    for row in rows:
        if _resolve_filepath(row) is not None:
            verified = row.validation_status == "verified"
            # Once a chunk is verified, the dashboard shows the human-confirmed
            # text (what the user saved), not the raw ASR output. Pending chunks
            # still show the ASR transcription.
            display = row.verified_transcription if verified else row.transcription
            result.setdefault(row.date, []).append({
                "filename": row.filename,
                "transcription": display,
                "verified": verified,
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
