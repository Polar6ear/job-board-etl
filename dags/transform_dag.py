from __future__ import annotations
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=20),
}

RAW_DATA_DIR = Path("/opt/airflow/data/raw")
TRANSFORMED_DIR = Path("/opt/airflow/data/transformed")
SOURCES = ["adzuna", "remoteok", "linkedin"]

def load_raw_files(**context):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_raw = []
    for source in SOURCES:
        source_dir = RAW_DATA_DIR / source / today
        if not source_dir.exists():
            print(f"No data for {source} on {today}")
            continue
        for json_file in sorted(source_dir.glob("*.json")):
            try:
                payload = json.loads(json_file.read_text())
                jobs = payload.get("jobs", [])
                all_raw.extend(jobs)
                print(f"{source} — {json_file.name}: {len(jobs)} jobs")
            except Exception as exc:
                print(f"ERROR reading {json_file}: {exc}")
    print(f"Total raw jobs loaded: {len(all_raw)}")
    context["ti"].xcom_push(key="raw_jobs", value=all_raw)
    context["ti"].xcom_push(key="raw_count", value=len(all_raw))
    return len(all_raw)

def normalize_jobs(**context):
    import sys
    sys.path.insert(0, "/opt/airflow")
    from etl.transformers.normalizer import Normalizer
    raw_jobs = context["ti"].xcom_pull(task_ids="load_raw_files", key="raw_jobs") or []
    if not raw_jobs:
        raise ValueError("No raw jobs found.")
    result = Normalizer().run(raw_jobs)
    normalized_dicts = [j.model_dump(mode="json") for j in result.valid]
    context["ti"].xcom_push(key="normalized_jobs", value=normalized_dicts)
    context["ti"].xcom_push(key="normalized_count", value=len(normalized_dicts))
    context["ti"].xcom_push(key="invalid_count", value=len(result.invalid))
    print(f"Valid: {len(normalized_dicts)} | Invalid: {len(result.invalid)}")
    return len(normalized_dicts)

def deduplicate_jobs(**context):
    import sys
    sys.path.insert(0, "/opt/airflow")
    from etl.transformers.deduplicator import Deduplicator
    from etl.transformers.schemas import NormalizedJob
    normalized_dicts = context["ti"].xcom_pull(task_ids="normalize_jobs", key="normalized_jobs") or []
    jobs = [NormalizedJob.model_validate(d) for d in normalized_dicts]
    unique_jobs, removed = Deduplicator().run(jobs)
    unique_dicts = [j.model_dump(mode="json") for j in unique_jobs]
    context["ti"].xcom_push(key="deduped_jobs", value=unique_dicts)
    context["ti"].xcom_push(key="deduped_count", value=len(unique_dicts))
    print(f"Unique: {len(unique_dicts)} | Removed: {len(removed)}")
    return len(unique_dicts)

def validate_transform(**context):
    ti = context["ti"]
    raw_count = ti.xcom_pull(task_ids="load_raw_files", key="raw_count") or 0
    invalid_count = ti.xcom_pull(task_ids="normalize_jobs", key="invalid_count") or 0
    deduped_jobs = ti.xcom_pull(task_ids="deduplicate_jobs", key="deduped_jobs") or []
    deduped_count = len(deduped_jobs)
    if deduped_count < 1:
        raise ValueError(f"Only {deduped_count} jobs after transform.")
    print(f"Validation passed | jobs={deduped_count}")

def persist_transformed(**context):
    deduped_jobs = context["ti"].xcom_pull(task_ids="deduplicate_jobs", key="deduped_jobs") or []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    TRANSFORMED_DIR.mkdir(parents=True, exist_ok=True)
    out_file = TRANSFORMED_DIR / f"{today}.jsonl"
    with out_file.open("w", encoding="utf-8") as f:
        for job in deduped_jobs:
            f.write(json.dumps(job, ensure_ascii=False, default=str) + "\n")
    print(f"Written {len(deduped_jobs)} jobs → {out_file}")
    context["ti"].xcom_push(key="transformed_file", value=str(out_file))
    return str(out_file)

with DAG(
    dag_id="job_board_transform",
    description="Normalize, deduplicate and validate extracted job data",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "transform", "job-board"],
) as dag:

    t_load_raw = PythonOperator(task_id="load_raw_files", python_callable=load_raw_files)
    t_normalize = PythonOperator(task_id="normalize_jobs", python_callable=normalize_jobs)
    t_dedup = PythonOperator(task_id="deduplicate_jobs", python_callable=deduplicate_jobs)
    t_validate = PythonOperator(task_id="validate_transform", python_callable=validate_transform)
    t_persist = PythonOperator(task_id="persist_transformed", python_callable=persist_transformed)
    t_trigger_load = TriggerDagRunOperator(
        task_id="trigger_load_dag",
        trigger_dag_id="job_board_load",
        wait_for_completion=False,
    )

    t_load_raw >> t_normalize >> t_dedup >> t_validate >> t_persist >> t_trigger_load