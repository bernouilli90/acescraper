from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import event, text
import os

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./acescraper.db")

engine = create_async_engine(DATABASE_URL, echo=False)


@event.listens_for(engine.sync_engine, "connect")
def _set_sqlite_pragma(dbapi_connection, _):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session


async def init_db():
    from app import models  # noqa: F401
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        for sql in [
            "ALTER TABLE sources ADD COLUMN test_status TEXT NOT NULL DEFAULT 'untested'",
            "ALTER TABLE sources ADD COLUMN test_last_run DATETIME",
            "ALTER TABLE sources ADD COLUMN fail_since DATETIME",
            "ALTER TABLE sources ADD COLUMN deleted BOOLEAN NOT NULL DEFAULT 0",
            "ALTER TABLE channels ADD COLUMN custom_logo TEXT",
            "ALTER TABLE channel_group ADD COLUMN position INTEGER DEFAULT 0",
        ]:
            try:
                await conn.execute(text(sql))
            except Exception:
                pass
