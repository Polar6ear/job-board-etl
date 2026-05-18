"""
etl/loaders/postgres_loader.py — Load phase.

Reads the transformed JSONL file and upserts into PostgreSQL.

Strategy:
  INSERT INTO jobs (...) VALUES (...)
  ON CONFLICT (source, source_id) DO UPDATE SET ...

This means re-running the pipeline is safe (idempotent).
Only fields that actually changed get updated (content_hash comparison).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import structlog
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from api.db.models import Job, JobSource, PipelineRun
from api.db.session import get_sync_session

logger = structlog.get_logger(__name__)

BATCH_SIZE = 500   # rows per INSERT statement


class PostgresLoader:
    """
    Loads NormalizedJob dicts from a JSONL file into PostgreSQL.

    Usage (from DAG):
        loader = PostgresLoader()
        stats = loader.load_from_file(Path("data/transformed/2024-01-01.jsonl"))
    """

    def __init__(self) -> None:
        self.log = logger.bind(component="postgres_loader")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_from_file(
        self,
        jsonl_path: Path,
        dag_run_id: str = "",
    ) -> dict:
        """
        Read JSONL and upsert into jobs table.
        Returns stats dict with upserted/skipped/error counts.
        """
        if not jsonl_path.exists():
            raise FileNotFoundError(f"Transformed file not found: {jsonl_path}")

        self.log.info("load_started", file=str(jsonl_path))

        stats = {"upserted": 0, "skipped": 0, "errors": 0, "total": 0}
        run_record = None

        with get_sync_session() as session:
            run_record = self._start_pipeline_run(session, dag_run_id)

            try:
                for batch in self._read_batches(jsonl_path):
                    batch_stats = self._upsert_batch(session, batch)
                    stats["upserted"] += batch_stats["upserted"]
                    stats["skipped"] += batch_stats["skipped"]
                    stats["errors"] += batch_stats["errors"]
                    stats["total"] += len(batch)
                    session.commit()

                self._finish_pipeline_run(session, run_record, "success", stats)
                session.commit()

            except Exception as exc:
                session.rollback()
                if run_record:
                    self._finish_pipeline_run(
                        session, run_record, "failed", stats, error=str(exc)
                    )
                    session.commit()
                raise

        self.log.info("load_finished", **stats)
        return stats

    # ------------------------------------------------------------------
    # Batch reading
    # ------------------------------------------------------------------

    @staticmethod
    def _read_batches(
        jsonl_path: Path,
        batch_size: int = BATCH_SIZE,
    ) -> Generator[list[dict], None, None]:
        batch: list[dict] = []
        with jsonl_path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    batch.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

                if len(batch) >= batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    # ------------------------------------------------------------------
    # Upsert
    # ------------------------------------------------------------------

    def _upsert_batch(
        self,
        session: Session,
        batch: list[dict],
    ) -> dict:
        stats = {"upserted": 0, "skipped": 0, "errors": 0}
        rows = []

        for job_dict in batch:
            try:
                row = self._to_db_row(job_dict)
                rows.append(row)
            except Exception as exc:
                self.log.warning("row_prep_failed", error=str(exc))
                stats["errors"] += 1

        if not rows:
            return stats

        stmt = pg_insert(Job).values(rows)

        # ON CONFLICT: update all mutable fields, skip if content_hash unchanged
        update_cols = {
            "title": stmt.excluded.title,
            "company": stmt.excluded.company,
            "location": stmt.excluded.location,
            "description": stmt.excluded.description,
            "url": stmt.excluded.url,
            "salary_min_usd": stmt.excluded.salary_min_usd,
            "salary_max_usd": stmt.excluded.salary_max_usd,
            "contract_type": stmt.excluded.contract_type,
            "tags": stmt.excluded.tags,
            "posted_at": stmt.excluded.posted_at,
            "normalized_at": stmt.excluded.normalized_at,
            "content_hash": stmt.excluded.content_hash,
            "updated_at": datetime.now(timezone.utc),
            "is_active": True,
        }

        stmt = stmt.on_conflict_do_update(
            constraint="uq_jobs_source_source_id",
            set_=update_cols,
            # Only update if content changed (avoids unnecessary writes)
            where=Job.content_hash != stmt.excluded.content_hash,
        )

        result = session.execute(stmt)
        # rowcount = rows actually written (inserted or updated)
        stats["upserted"] = result.rowcount
        stats["skipped"] = len(rows) - result.rowcount

        self.log.debug(
            "batch_upserted",
            upserted=stats["upserted"],
            skipped=stats["skipped"],
        )
        return stats

    # ------------------------------------------------------------------
    # Row prep
    # ------------------------------------------------------------------

    @staticmethod
    def _to_db_row(job: dict) -> dict:
        """Convert NormalizedJob dict → flat dict matching Job columns."""
        def parse_dt(v):
            if not v:
                return None
            if isinstance(v, datetime):
                return v
            try:
                return datetime.fromisoformat(str(v))
            except ValueError:
                return None

        return {
            "source": job["source"],
            "source_id": job["source_id"],
            "content_hash": job.get("content_hash", ""),
            "title": job["title"],
            "company": job["company"],
            "location": job.get("location", ""),
            "description": job.get("description", ""),
            "url": job.get("url", ""),
            "salary_min_usd": job.get("salary_min_usd"),
            "salary_max_usd": job.get("salary_max_usd"),
            "contract_type": job.get("contract_type", "unknown"),
            "tags": job.get("tags") or [],
            "posted_at": parse_dt(job.get("posted_at")),
            "normalized_at": parse_dt(job.get("normalized_at"))
                             or datetime.now(timezone.utc),
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "is_active": True,
        }

    # ------------------------------------------------------------------
    # Pipeline run audit
    # ------------------------------------------------------------------

    @staticmethod
    def _start_pipeline_run(session: Session, run_id: str) -> PipelineRun:
        run = PipelineRun(
            dag_id="job_board_load",
            run_id=run_id or f"manual_{datetime.now(timezone.utc).isoformat()}",
            status="running",
            started_at=datetime.now(timezone.utc),
        )
        session.add(run)
        session.flush()
        return run

    @staticmethod
    def _finish_pipeline_run(
        session: Session,
        run: PipelineRun,
        status: str,
        stats: dict,
        error: str | None = None,
    ) -> None:
        run.status = status
        run.upserted_count = stats.get("upserted", 0)
        run.finished_at = datetime.now(timezone.utc)
        run.error_message = error
        session.add(run)

    # ------------------------------------------------------------------
    # Source stats update
    # ------------------------------------------------------------------

    def update_source_stats(
        self,
        session: Session,
        source_name: str,
        count: int,
    ) -> None:
        """Upsert into job_sources to track last run metadata."""
        stmt = pg_insert(JobSource).values(
            name=source_name,
            last_run_at=datetime.now(timezone.utc),
            last_run_count=count,
            total_extracted=count,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["name"],
            set_={
                "last_run_at": datetime.now(timezone.utc),
                "last_run_count": count,
                "total_extracted": JobSource.total_extracted + count,
            },
        )
        session.execute(stmt)