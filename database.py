import os
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Boolean, DateTime, Text, ForeignKey, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker

DB_PATH = os.environ.get("DB_PATH", "/app/db/recordings.db")
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    discord_id = Column(String, primary_key=True)
    username = Column(String, nullable=False)
    avatar_url = Column(String)
    last_login = Column(DateTime, default=datetime.utcnow)


class AudioFile(Base):
    __tablename__ = "audio_files"

    id = Column(String, primary_key=True)  # e.g. discord_id + filename
    discord_id = Column(String, ForeignKey("users.discord_id"), nullable=False)
    username = Column(String)
    filename = Column(String, nullable=False)
    filepath = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
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
    created_at = Column(DateTime, default=datetime.utcnow)
    is_deleted = Column(Boolean, default=False)


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    Base.metadata.create_all(engine)


def upsert_user(discord_id: str, username: str, avatar_url: str):
    with SessionLocal() as db:
        user = db.get(User, discord_id)
        if user:
            user.username = username
            user.avatar_url = avatar_url
            user.last_login = datetime.utcnow()
        else:
            user = User(
                discord_id=discord_id,
                username=username,
                avatar_url=avatar_url,
                last_login=datetime.utcnow()
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


def get_chunks_for_user(discord_id: str) -> dict:
    """
    Returns { "YYYY-MM-DD": ["chunk_001.wav", ...] } ordered date-desc, filename-asc.
    Only includes rows where is_deleted=False AND the file still exists on disk.
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
        if os.path.exists(row.filepath):           # double-check file is on disk
            result.setdefault(row.date, []).append(row.filename)
    return result


def delete_chunk(discord_id: str, date: str, filename: str) -> str | None:
    """
    Hard-delete: removes the file from disk and marks the DB row is_deleted=True.
    Returns the filepath on success, None if the row wasn't found or doesn't belong
    to this user.
    """
    chunk_id = f"{discord_id}:{date}:{filename}"
    with SessionLocal() as db:
        row = db.get(Chunk, chunk_id)
        if not row or row.discord_id != discord_id:
            return None
        filepath = row.filepath
        # Delete file from disk
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
        except OSError:
            pass
        # Mark deleted in DB
        row.is_deleted = True
        db.commit()
    return filepath
