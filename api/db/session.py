"""
api/db/session.py — Database session management.

Two engines:
  - Async engine  → used by FastAPI (asyncpg driver)
  - Sync engine   → used by Airflow DAG tasks (psycopg2 driver)

Config is read from environment variables (see .env.example).
"""

from __future__ import annotations

import os
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker


def _build_dsn(driver: str) -> str:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "jobboard")
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "secret")
    return f"postgresql+{driver}://{user}:{password}@{host}:{port}/{db}"


# ---------------------------------------------------------------------------
# Async (FastAPI)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def get_async_engine():
    return create_async_engine(
        _build_dsn("asyncpg"),
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        echo=os.environ.get("DB_ECHO", "false").lower() == "true",
    )


AsyncSessionLocal = async_sessionmaker(
    bind=get_async_engine(),
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def get_async_session() -> AsyncSession:
    """FastAPI dependency — yields a session per request."""
    async with AsyncSessionLocal() as session:
        yield session


# ---------------------------------------------------------------------------
# Sync (Airflow DAG tasks)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def get_sync_engine():
    return create_engine(
        _build_dsn("psycopg2"),
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )


SyncSessionLocal = sessionmaker(
    bind=get_sync_engine(),
    autoflush=False,
    autocommit=False,
)


def get_sync_session() -> Session:
    """Context manager for DAG tasks."""
    return SyncSessionLocal()