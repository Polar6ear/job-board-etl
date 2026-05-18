"""
api/db/models.py — SQLAlchemy ORM models.

Tables:
  jobs          — normalized job listings
  job_sources   — source metadata (last run, health)
  pipeline_runs — audit log for every DAG run
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# jobs
# ---------------------------------------------------------------------------

class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Source tracking
    source: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    source_id: Mapped[str] = mapped_column(String(200), nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    # Core fields
    title: Mapped[str] = mapped_column(String(300), nullable=False)
    company: Mapped[str] = mapped_column(String(200), nullable=False)
    location: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    url: Mapped[str] = mapped_column(String(2048), nullable=False, default="")

    # Salary (USD)
    salary_min_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    salary_max_usd: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Classification
    contract_type: Mapped[str] = mapped_column(String(50), nullable=False, default="unknown")
    tags: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False, default=list)

    # Timestamps
    posted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    normalized_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )

    # Soft delete
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    __table_args__ = (
        # Prevent exact duplicate (source + source_id)
        UniqueConstraint("source", "source_id", name="uq_jobs_source_source_id"),
        # Fast full-text search via GIN on tags array
        Index("ix_jobs_tags", "tags", postgresql_using="gin"),
        # Composite for common API query patterns
        Index("ix_jobs_source_active", "source", "is_active"),
        Index("ix_jobs_company", "company"),
        Index("ix_jobs_posted_at", "posted_at"),
        Index("ix_jobs_contract_type", "contract_type"),
    )

    def __repr__(self) -> str:
        return f"<Job id={self.id} title={self.title!r} company={self.company!r}>"


# ---------------------------------------------------------------------------
# job_sources
# ---------------------------------------------------------------------------

class JobSource(Base):
    __tablename__ = "job_sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_run_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_run_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_extracted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    config: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    def __repr__(self) -> str:
        return f"<JobSource name={self.name!r} enabled={self.is_enabled}>"


# ---------------------------------------------------------------------------
# pipeline_runs — audit log
# ---------------------------------------------------------------------------

class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    dag_id: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    run_id: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)  # success/failed/running

    # Counts
    raw_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    normalized_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    deduped_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    upserted_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        UniqueConstraint("dag_id", "run_id", name="uq_pipeline_runs_dag_run"),
    )

    def __repr__(self) -> str:
        return f"<PipelineRun dag={self.dag_id!r} status={self.status!r}>"