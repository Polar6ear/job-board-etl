"""
extract_dag.py — Airflow DAG for the Extract phase.

Schedule : Every 6 hours
Tasks    :
  1. check_sources_alive     — HTTP health-check each source
  2. extract_adzuna          — Pull from Adzuna API
  3. extract_remoteok        — Pull from RemoteOK API
  4. extract_linkedin        — Scrape LinkedIn public listings
  5. validate_raw_output     — Assert raw files exist and are non-empty
  6. trigger_transform_dag   — Fire the downstream Transform DAG

All source extractions run in parallel (tasks 2-4).
Task 5 waits for all three before validating.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

# ---------------------------------------------------------------------------
# DAG default args
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# ---------------------------------------------------------------------------
# Job queries to extract (can be driven by Airflow Variables in production)
# ---------------------------------------------------------------------------

EXTRACT_QUERIES = [
    {"query": "python developer", "location": "India"},
    {"query": "data engineer", "location": "Remote"},
    {"query": "backend engineer", "location": "India"},
    {"query": "machine learning engineer", "location": ""},
    {"query": "devops engineer", "location": "India"},
]

MAX_RESULTS_PER_QUERY = 50

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------


def check_sources_alive(**context) -> dict:
    """
    Simple HTTP HEAD/GET check on each source endpoint.
    Pushes source_status dict to XCom so downstream tasks can inspect it.
    """
    import httpx

    sources = {
        "adzuna": "https://api.adzuna.com/v1/api/jobs",
        "remoteok": "https://remoteok.com/api",
        "linkedin": "https://www.linkedin.com/jobs",
    }
    statuses = {}
    with httpx.Client(timeout=10.0) as client:
        for name, url in sources.items():
            try:
                resp = client.head(url)
                statuses[name] = {"alive": resp.status_code < 500, "code": resp.status_code}
            except Exception as exc:
                statuses[name] = {"alive": False, "error": str(exc)}

    context["ti"].xcom_push(key="source_status", value=statuses)
    print(f"[check_sources_alive] {json.dumps(statuses, indent=2)}")
    return statuses


def extract_adzuna(**context) -> str:
    """
    Extract jobs from Adzuna for all configured queries.
    Returns path to raw data directory via XCom.
    """
    from etl.extractors.adzuna import AdzunaExtractor

    all_jobs = []
    with AdzunaExtractor() as extractor:
        for q in EXTRACT_QUERIES:
            jobs = extractor.extract(
                query=q["query"],
                location=q["location"],
                max_results=MAX_RESULTS_PER_QUERY,
            )
            all_jobs.extend(jobs)

    print(f"[extract_adzuna] Total jobs extracted: {len(all_jobs)}")
    context["ti"].xcom_push(key="adzuna_count", value=len(all_jobs))
    return f"adzuna:{len(all_jobs)}"


def extract_remoteok(**context) -> str:
    """
    Extract jobs from RemoteOK for all configured queries.
    """
    from etl.extractors.remoteok import RemoteOKExtractor

    all_jobs = []
    with RemoteOKExtractor() as extractor:
        for q in EXTRACT_QUERIES:
            jobs = extractor.extract(
                query=q["query"],
                location=q["location"],
                max_results=MAX_RESULTS_PER_QUERY,
            )
            all_jobs.extend(jobs)

    print(f"[extract_remoteok] Total jobs extracted: {len(all_jobs)}")
    context["ti"].xcom_push(key="remoteok_count", value=len(all_jobs))
    return f"remoteok:{len(all_jobs)}"


def extract_linkedin(**context) -> str:
    """
    Scrape LinkedIn public listings for all configured queries.
    Keeps max_results low to avoid rate-limiting.
    """
    from etl.extractors.linkedin_scraper import LinkedInScraper

    all_jobs = []
    with LinkedInScraper() as extractor:
        for q in EXTRACT_QUERIES:
            jobs = extractor.extract(
                query=q["query"],
                location=q["location"],
                max_results=25,   # conservative for LinkedIn
            )
            all_jobs.extend(jobs)

    print(f"[extract_linkedin] Total jobs extracted: {len(all_jobs)}")
    context["ti"].xcom_push(key="linkedin_count", value=len(all_jobs))
    return f"linkedin:{len(all_jobs)}"


def validate_raw_output(**context) -> None:
    """
    Assert that raw data files were written today.
    Pulls XCom counts from each extractor and logs a summary.
    Raises ValueError if all sources returned 0 — likely a pipeline failure.
    """
    ti = context["ti"]

    counts = {
        "adzuna": ti.xcom_pull(task_ids="extract_adzuna", key="adzuna_count") or 0,
        "remoteok": ti.xcom_pull(task_ids="extract_remoteok", key="remoteok_count") or 0,
        "linkedin": ti.xcom_pull(task_ids="extract_linkedin", key="linkedin_count") or 0,
    }

    total = sum(counts.values())
    print(f"[validate_raw_output] Extraction summary: {json.dumps(counts)} | Total: {total}")

    if total == 0:
        raise ValueError(
            "ALL extractors returned 0 jobs. "
            "Check source connectivity and API credentials."
        )

    # Optionally verify raw files on disk
    raw_root = Path("data/raw")
    today = datetime.utcnow().strftime("%Y-%m-%d")
    for source in counts:
        source_dir = raw_root / source / today
        if not source_dir.exists():
            print(f"WARNING: Raw directory missing for {source} on {today}")

    context["ti"].xcom_push(key="total_extracted", value=total)
    print(f"[validate_raw_output] Validation passed. {total} jobs ready for transform.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="job_board_extract",
    description="Extract job listings from Adzuna, RemoteOK, and LinkedIn",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */6 * * *",   # every 6 hours
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "extract", "job-board"],
) as dag:

    # Task 1 — health check
    t_check = PythonOperator(
        task_id="check_sources_alive",
        python_callable=check_sources_alive,
        doc_md="HEAD-check each source to detect outages early.",
    )

    # Tasks 2-4 — parallel extraction
    t_adzuna = PythonOperator(
        task_id="extract_adzuna",
        python_callable=extract_adzuna,
        doc_md="Pull job listings from Adzuna REST API.",
    )

    t_remoteok = PythonOperator(
        task_id="extract_remoteok",
        python_callable=extract_remoteok,
        doc_md="Pull remote job listings from RemoteOK public API.",
    )

    t_linkedin = PythonOperator(
        task_id="extract_linkedin",
        python_callable=extract_linkedin,
        doc_md="Scrape LinkedIn public job listings.",
    )

    # Task 5 — validate
    t_validate = PythonOperator(
        task_id="validate_raw_output",
        python_callable=validate_raw_output,
        doc_md="Assert combined extraction count > 0 before triggering transform.",
        trigger_rule="all_done",   # run even if one source failed
    )

    # Task 6 — trigger downstream DAG
    t_trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="job_board_transform",
        wait_for_completion=False,
        doc_md="Fire the Transform DAG once extraction is validated.",
    )

    # ---------------------------------------------------------------------------
    # Task dependencies
    #
    #   check_sources_alive
    #        |
    #   ┌────┼────┐
    #   │    │    │
    # adzuna rk  li     (parallel)
    #   │    │    │
    #   └────┼────┘
    #        │
    #   validate_raw_output
    #        │
    #   trigger_transform_dag
    # ---------------------------------------------------------------------------

    t_check >> [t_adzuna, t_remoteok, t_linkedin]
    [t_adzuna, t_remoteok, t_linkedin] >> t_validate
    t_validate >> t_trigger_transform