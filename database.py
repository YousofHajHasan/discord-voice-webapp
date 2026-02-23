import os
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Boolean, DateTime, Text, ForeignKey
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
    is_deleted = Column(Boolean, default=False)  # soft delete — ready for future use


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
