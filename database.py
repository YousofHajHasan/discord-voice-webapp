import os
import wave
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, Column, String, Boolean, DateTime, Text, ForeignKey, UniqueConstraint, Float, inspect, text, event
from sqlalchemy.orm import declarative_base, sessionmaker

DB_PATH = os.environ.get("DB_PATH", "/app/db/recordings.db")
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})


@event.listens_for(engine, "connect")
def _sqlite_pragmas(dbapi_conn, _record):
    """
    Wait up to 5s for SQLite's single write lock instead of immediately raising
    "database is locked". The validator claim (claim_pending_window) is a
    conditional UPDATE; with multiple validators (or uvicorn workers) two claims
    may contend for the write lock, and this lets the second one briefly wait for
    the first to commit rather than error out.
    """
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA busy_timeout=5000")
    cur.close()


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
    # Audio length in seconds, filled lazily (header-only read) by the background
    # backfill and at decision time. NULL = not measured yet. Powers the
    # validation "Insights" totals via cheap SUM() aggregates (no per-request
    # file reads). See backfill_durations() and validation_db.get_insights().
    duration = Column(Float, nullable=True, default=None)
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
    # Provenance for a chunk produced by the in-app trim tool: the ORIGINAL
    # chunk's filename it was cut from (same owner+date). NULL for normally
    # recorded chunks. Enables auditing / undo of a "Trim & Accept" — the
    # original is soft-deleted and this new _updated.wav row points back at it.
    derived_from = Column(String, nullable=True, default=None)

    # Human-validation work-queue lease. SEPARATE from claimed_by/claimed_at above
    # (which the ASR transcription script uses) so the script and human validators
    # never contend for the same lock. A validator leases a window of pending
    # chunks when they fetch; the lease auto-expires after VALIDATION_LEASE_MINUTES
    # so chunks fetched-but-never-decided return to the shared pool. This is what
    # keeps two validators on the SAME owner from grabbing the same chunks — see
    # validation_db.claim_pending_window().
    validation_claimed_by = Column(String, nullable=True, default=None)   # discord_id leasing it
    validation_claimed_at = Column(DateTime, nullable=True, default=None)  # when, for lease expiry


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
    "validation_claimed_by":  "VARCHAR",
    "validation_claimed_at":  "DATETIME",
    "duration":               "FLOAT",
    "derived_from":           "VARCHAR",
}

# Indexes to (idempotently) ensure on the chunks table. The owner+status index
# makes the Insights SUM(duration) aggregates and the per-owner pending counts
# fast even on a multi-hundred-thousand-row table.
_CHUNK_INDEXES = {
    "ix_chunks_owner_status": "(discord_id, validation_status)",
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


def _ensure_indexes():
    """Create any missing chunks indexes (IF NOT EXISTS — safe every startup)."""
    if "chunks" not in inspect(engine).get_table_names():
        return
    with engine.begin() as conn:
        for name, cols in _CHUNK_INDEXES.items():
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS {name} ON chunks {cols}"))


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    Base.metadata.create_all(engine)
    _run_light_migrations()
    _ensure_indexes()


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


def bulk_register_chunks(rows) -> int:
    """
    Register many chunks efficiently: ONE query for existing ids, then a single
    bulk insert + commit for the new ones. `rows` is an iterable of
    (discord_id, date, filename, filepath). Returns the number inserted.

    Replaces calling register_chunk() per file (a SELECT + INSERT + COMMIT/fsync
    each). At thousands of chunks on a network-mounted volume, that per-file
    fsync storm made startup take minutes; this is one transaction.
    """
    rows = list(rows)
    if not rows:
        return 0
    with SessionLocal() as db:
        existing = {cid for (cid,) in db.query(Chunk.id).all()}
        new_objs = []
        for discord_id, date, filename, filepath in rows:
            cid = f"{discord_id}:{date}:{filename}"
            if cid in existing:
                continue
            existing.add(cid)  # also dedupe within this batch
            new_objs.append(Chunk(
                id=cid, discord_id=discord_id, date=date,
                filename=filename, filepath=filepath,
            ))
        if not new_objs:
            return 0
        db.bulk_save_objects(new_objs)
        db.commit()
    return len(new_objs)


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


# ── Audio duration (header-only) + lazy backfill ──────────────────────────────

DURATION_BACKFILL_BATCH = int(os.environ.get("DURATION_BACKFILL_BATCH", "500"))


def compute_wav_duration(path: str):
    """
    Seconds of audio in a .wav, read from the HEADER only (no full decode) — a
    few microseconds and a tiny read even on a network mount. Returns None if the
    file is unreadable/corrupt so callers can decide how to record that.
    """
    try:
        with wave.open(path, "rb") as w:
            rate = w.getframerate()
            if not rate:
                return None
            return w.getnframes() / float(rate)
    except Exception:
        return None


def _disk_path(discord_id: str, date: str, filename: str, stored_path: str | None):
    """
    Existing on-disk path for a chunk, resolved WITHOUT any DB access (no filepath
    healing, no nested session). Tries the stored path, then the canonical
    ID-folder location. None if the file isn't found. Safe to call off the DB lock.
    """
    if stored_path and os.path.exists(stored_path):
        return stored_path
    alt = os.path.join(RECORDINGS_PATH, discord_id, "chunks", date, filename)
    return alt if os.path.exists(alt) else None


def measure_chunk_duration(discord_id: str, date: str, filename: str, stored_path: str | None = None):
    """Header-only duration for a chunk, resolving its path without touching the
    DB. None if missing/unreadable. Safe to call before opening a write txn."""
    path = _disk_path(discord_id, date, filename, stored_path)
    return compute_wav_duration(path) if path else None


def trim_wav(src_path: str, dst_path: str, start_sec: float, end_sec: float):
    """
    Write a NEW wav at dst_path holding only [start_sec, end_sec] of src_path,
    losslessly — a raw PCM frame copy with the SAME channels / sample width /
    rate (no decode, no re-encode). Returns the new duration in seconds, or None
    if the source is unreadable or the range is empty after clamping.

    Does NO database access (mirrors compute_wav_duration) so it is safe to call
    with no transaction open — the network-volume read+write must never hold
    SQLite's single write lock. Writes to a temp file then atomically os.replace()s
    it into place, so a half-written .wav is never visible to a concurrent reader.
    """
    try:
        with wave.open(src_path, "rb") as w:
            rate = w.getframerate()
            nframes = w.getnframes()
            nchannels = w.getnchannels()
            sampwidth = w.getsampwidth()
            if not rate or not nframes:
                return None
            start_f = max(0, min(nframes, int(round(start_sec * rate))))
            end_f = max(0, min(nframes, int(round(end_sec * rate))))
            if end_f - start_f < 1:
                return None
            w.setpos(start_f)
            frames = w.readframes(end_f - start_f)
    except Exception:
        return None

    tmp = f"{dst_path}.{os.getpid()}.tmp"
    try:
        with wave.open(tmp, "wb") as out:
            out.setnchannels(nchannels)
            out.setsampwidth(sampwidth)
            out.setframerate(rate)
            out.writeframes(frames)
        os.replace(tmp, dst_path)          # atomic within the same directory
    except Exception:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass
        return None
    return (end_f - start_f) / float(rate)


def backfill_durations(batch_size: int = DURATION_BACKFILL_BATCH) -> int:
    """
    Fill Chunk.duration for up to batch_size rows that don't have it yet, reading
    each WAV header exactly once. Called every cycle from the background scan
    thread until it returns 0 (backlog drained); new chunks get measured the same
    way.

    CRITICAL for the ~400k-row production backfill: the slow, network-volume
    header reads happen with NO database transaction open. We (1) grab a batch of
    ids in a short read and RELEASE the connection, (2) stat + measure on disk
    holding no lock, then (3) write the results in one short transaction. This
    mirrors claim_pending_window() and means the backfill can never block
    validators or chunk registration on SQLite's single write lock, however slow
    the mounted volume is.

    Per row: file resolves -> store its duration; file present but corrupt ->
    store 0.0 (never retried, so the backlog always drains); file gone -> mark
    is_deleted (matches vanished-file handling elsewhere). Decided rows are
    measured first so the Insights "verified" totals become exact almost
    immediately. Returns the number of rows processed this batch.
    """
    # 1) Claim a batch of unmeasured chunks (short read), then drop the lock.
    with SessionLocal() as db:
        batch = (
            db.query(Chunk.id, Chunk.discord_id, Chunk.date, Chunk.filename, Chunk.filepath)
            .filter(Chunk.duration.is_(None), Chunk.is_deleted == False)
            # False (0) sorts first -> decided rows before pending ones.
            .order_by((Chunk.validation_status == "pending").asc())
            .limit(batch_size)
            .all()
        )
    if not batch:
        return 0

    # 2) Measure on disk with NO transaction open (header reads can be slow on a
    #    network mount; they must not hold SQLite's lock).
    durations = {}   # id -> seconds (0.0 if present but unreadable)
    healed = {}      # id -> corrected filepath (found at the ID-folder location)
    gone = []        # ids whose file is missing everywhere
    for cid, discord_id, date, filename, stored in batch:
        path = stored if (stored and os.path.exists(stored)) else None
        if path is None:
            alt = os.path.join(RECORDINGS_PATH, discord_id, "chunks", date, filename)
            if os.path.exists(alt):
                path, healed[cid] = alt, alt
        if path is None:
            gone.append(cid)
            continue
        dur = compute_wav_duration(path)
        durations[cid] = dur if dur is not None else 0.0

    # 3) Persist everything in one short write transaction.
    with SessionLocal() as db:
        if durations:
            db.bulk_update_mappings(Chunk, [{"id": cid, "duration": d} for cid, d in durations.items()])
        for cid, path in healed.items():
            db.query(Chunk).filter(Chunk.id == cid).update({Chunk.filepath: path}, synchronize_session=False)
        if gone:
            db.query(Chunk).filter(Chunk.id.in_(gone)).update({Chunk.is_deleted: True}, synchronize_session=False)
        db.commit()
    return len(batch)


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


# ── Human-validation leases ───────────────────────────────────────────────────

VALIDATION_LEASE_MINUTES = 15


def release_stale_validation_claims() -> int:
    """
    Free validator leases (validation_claimed_by/at) older than
    VALIDATION_LEASE_MINUTES that are still pending — so chunks a validator
    fetched but never decided (closed the tab, crashed, walked away) return to
    the shared pool for other validators. Mirrors release_stale_claims() for the
    ASR queue. Called from the background scan thread.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=VALIDATION_LEASE_MINUTES)
    with SessionLocal() as db:
        n = (
            db.query(Chunk)
            .filter(
                Chunk.validation_claimed_by != None,
                Chunk.validation_claimed_at < cutoff,
                Chunk.validation_status == "pending",
            )
            .update(
                {Chunk.validation_claimed_by: None, Chunk.validation_claimed_at: None},
                synchronize_session=False,
            )
        )
        if n:
            db.commit()
    return n


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
