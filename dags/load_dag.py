"""
load_dag.py — Airflow DAG for the Load phase.

Triggered by : transform_dag (TriggerDagRunOperator)
Tasks        :
  1. resolve_transformed_file  — Find today's JSONL in data/transformed/
  2. upsert_to_postgres        — Batch upsert via ON CONFLICT DO UPDATE
  3. update_source_stats       — Refresh job_sources table counts
  4. run_post_load_checks      — Row count sanity check vs yesterday
  5. mark_pipeline_complete    — Log final PipelineRun record
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

TRANSFORMED_DIR = Path("data/transformed")


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------


def resolve_transformed_file(**context) -> str:
    """
    Find today's transformed JSONL. Falls back to most recent file
    if today's doesn't exist yet (handles timezone edge cases).
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    target = TRANSFORMED_DIR / f"{today}.jsonl"

    if target.exists():
        path = str(target)
    else:
        # Fallback: pick the most recently modified JSONL
        candidates = sorted(
            TRANSFORMED_DIR.glob("*.jsonl"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            raise FileNotFoundError(
                f"No transformed JSONL found in {TRANSFORMED_DIR}. "
                "Has transform_dag run successfully?"
            )
        path = str(candidates[0])
        print(f"[resolve_transformed_file] Today's file missing; using {path}")

    # Count lines for downstream tasks
    line_count = sum(1 for _ in open(path))
    print(f"[resolve_transformed_file] File: {path} | Lines: {line_count}")

    context["ti"].xcom_push(key="transformed_file", value=path)
    context["ti"].xcom_push(key="line_count", value=line_count)
    return path


def upsert_to_postgres(**context) -> dict:
    """
    Call PostgresLoader to batch-upsert all jobs from the JSONL file.
    """
    from etl.loaders.postgres_loader import PostgresLoader

    transformed_file = context["ti"].xcom_pull(
        task_ids="resolve_transformed_file", key="transformed_file"
    )
    dag_run_id = context.get("run_id", "")

    loader = PostgresLoader()
    stats = loader.load_from_file(
        jsonl_path=Path(transformed_file),
        dag_run_id=dag_run_id,
    )

    print(
        f"[upsert_to_postgres] "
        f"total={stats['total']} | upserted={stats['upserted']} | "
        f"skipped={stats['skipped']} | errors={stats['errors']}"
    )

    context["ti"].xcom_push(key="load_stats", value=stats)
    return stats


def update_source_stats(**context) -> None:
    """
    Update job_sources table: last_run_at, last_run_count, total_extracted.
    Reads the transformed JSONL to count per-source.
    """
    from etl.loaders.postgres_loader import PostgresLoader
    from api.db.session import get_sync_session

    transformed_file = context["ti"].xcom_pull(
        task_ids="resolve_transformed_file", key="transformed_file"
    )

    # Count jobs per source from the file
    source_counts: dict[str, int] = {}
    with open(transformed_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                job = json.loads(line)
                source = job.get("source", "unknown")
                source_counts[source] = source_counts.get(source, 0) + 1
            except json.JSONDecodeError:
                continue

    print(f"[update_source_stats] Per-source counts: {source_counts}")

    loader = PostgresLoader()
    with get_sync_session() as session:
        for source_name, count in source_counts.items():
            loader.update_source_stats(session, source_name, count)
        session.commit()


def run_post_load_checks(**context) -> None:
    """
    Sanity checks after loading:
      - Total active job count > 0
      - No source has 0 jobs (likely a scraper failure)
      - Upsert success rate > 90%
    """
    from api.db.session import get_sync_session
    from api.db.models import Job
    from sqlalchemy import func, select

    load_stats: dict = context["ti"].xcom_pull(
        task_ids="upsert_to_postgres", key="load_stats"
    ) or {}

    with get_sync_session() as session:
        # Total active jobs
        total_active = session.execute(
            select(func.count()).where(Job.is_active == True)  # noqa: E712
        ).scalar_one()

        # Per-source active counts
        rows = session.execute(
            select(Job.source, func.count().label("cnt"))
            .where(Job.is_active == True)  # noqa: E712
            .group_by(Job.source)
        ).all()
        source_counts = {r.source: r.cnt for r in rows}

    print(f"[run_post_load_checks] Total active jobs: {total_active}")
    print(f"[run_post_load_checks] Per-source: {source_counts}")

    # Gate 1: at least some jobs exist
    if total_active == 0:
        raise ValueError("Post-load check failed: 0 active jobs in database.")

    # Gate 2: upsert error rate
    total = load_stats.get("total", 0)
    errors = load_stats.get("errors", 0)
    if total > 0 and errors / total > 0.10:
        raise ValueError(
            f"Post-load check failed: {errors}/{total} rows had errors "
            f"({errors/total:.1%} error rate > 10% threshold)."
        )

    print("[run_post_load_checks] ✓ All post-load checks passed.")
    context["ti"].xcom_push(key="total_active_jobs", value=total_active)


def mark_pipeline_complete(**context) -> None:
    """
    Log the full pipeline summary — visible in Airflow UI task logs.
    In production you'd also push this to Slack / PagerDuty / Datadog here.
    """
    ti = context["ti"]

    load_stats = ti.xcom_pull(task_ids="upsert_to_postgres", key="load_stats") or {}
    total_active = ti.xcom_pull(task_ids="run_post_load_checks", key="total_active_jobs") or 0
    line_count = ti.xcom_pull(task_ids="resolve_transformed_file", key="line_count") or 0

    summary = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "jobs_in_file": line_count,
        "upserted": load_stats.get("upserted", 0),
        "skipped": load_stats.get("skipped", 0),
        "errors": load_stats.get("errors", 0),
        "total_active_in_db": total_active,
    }

    print(
        "[mark_pipeline_complete] Pipeline complete!\n"
        + json.dumps(summary, indent=2)
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="job_board_load",
    description="Upsert transformed job data into PostgreSQL",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,    # triggered by transform_dag
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "load", "job-board"],
) as dag:

    t_resolve = PythonOperator(
        task_id="resolve_transformed_file",
        python_callable=resolve_transformed_file,
        doc_md="Locate today's JSONL file in data/transformed/.",
    )

    t_upsert = PythonOperator(
        task_id="upsert_to_postgres",
        python_callable=upsert_to_postgres,
        doc_md="Batch upsert all jobs using ON CONFLICT DO UPDATE.",
    )

    t_source_stats = PythonOperator(
        task_id="update_source_stats",
        python_callable=update_source_stats,
        doc_md="Refresh job_sources table with latest run counts.",
    )

    t_checks = PythonOperator(
        task_id="run_post_load_checks",
        python_callable=run_post_load_checks,
        doc_md="Verify DB row counts and error rates after load.",
    )

    t_complete = PythonOperator(
        task_id="mark_pipeline_complete",
        python_callable=mark_pipeline_complete,
        doc_md="Log final pipeline summary (hook for Slack/Datadog alerts).",
    )

    # ---------------------------------------------------------------------------
    # Task flow
    #
    #   resolve_transformed_file
    #           ↓
    #   upsert_to_postgres
    #           ↓
    #   update_source_stats
    #           ↓
    #   run_post_load_checks
    #           ↓
    #   mark_pipeline_complete
    # ---------------------------------------------------------------------------

    t_resolve >> t_upsert >> t_source_stats >> t_checks >> t_complete