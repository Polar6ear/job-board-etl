"""
api/routers/jobs.py — All job-related API endpoints.

Endpoints:
  GET  /jobs/search          — full-text + filter search
  GET  /jobs/{job_id}        — single job detail
  GET  /jobs/sources         — source health stats
  GET  /jobs/runs            — recent pipeline run history
  POST /jobs/trigger          — manually trigger the extract DAG
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from api.db.models import Job, JobSource, PipelineRun
from api.db.session import get_async_session
from api.schemas.job import (
    ContractTypeFilter,
    JobListResponse,
    JobResponse,
    PipelineRunResponse,
    SortBy,
    SourceStatsResponse,
)

router = APIRouter(prefix="/jobs", tags=["jobs"])

# ---------------------------------------------------------------------------
# Dependency alias
# ---------------------------------------------------------------------------

DBSession = Annotated[AsyncSession, Depends(get_async_session)]


# ---------------------------------------------------------------------------
# GET /jobs/search
# ---------------------------------------------------------------------------

@router.get("/search", response_model=JobListResponse)
async def search_jobs(
    db: DBSession,
    q: Annotated[str, Query(description="Search query — title, company, tags")] = "",
    location: Annotated[str, Query(description="Filter by location (partial match)")] = "",
    contract_type: Annotated[ContractTypeFilter | None, Query()] = None,
    tags: Annotated[list[str], Query(description="Filter by tags (AND logic)")] = [],
    salary_min: Annotated[float | None, Query(ge=0)] = None,
    salary_max: Annotated[float | None, Query(ge=0)] = None,
    source: Annotated[str, Query(description="Filter by source: adzuna, remoteok, linkedin")] = "",
    sort_by: Annotated[SortBy, Query()] = SortBy.POSTED_AT,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> JobListResponse:
    """
    Search and filter job listings.

    - **q**: keyword search across title + company + description
    - **location**: partial match on location string
    - **contract_type**: full_time / part_time / contract / remote / internship
    - **tags**: one or more tags (e.g. python, aws) — ALL must match
    - **salary_min / salary_max**: USD salary range filter
    - **source**: adzuna | remoteok | linkedin
    - **sort_by**: posted_at | salary | relevance
    - **page / page_size**: pagination
    """
    stmt = select(Job).where(Job.is_active == True)  # noqa: E712

    # Full-text search (title + company + description)
    if q:
        q_lower = q.lower()
        stmt = stmt.where(
            or_(
                Job.title.ilike(f"%{q_lower}%"),
                Job.company.ilike(f"%{q_lower}%"),
                Job.description.ilike(f"%{q_lower}%"),
            )
        )

    # Location filter
    if location:
        stmt = stmt.where(Job.location.ilike(f"%{location}%"))

    # Contract type
    if contract_type:
        stmt = stmt.where(Job.contract_type == contract_type.value)

    # Tags — PostgreSQL array @> operator (contains all)
    for tag in tags:
        stmt = stmt.where(Job.tags.contains([tag.lower()]))

    # Salary filters
    if salary_min is not None:
        stmt = stmt.where(
            or_(Job.salary_max_usd >= salary_min, Job.salary_min_usd >= salary_min)
        )
    if salary_max is not None:
        stmt = stmt.where(
            or_(Job.salary_min_usd <= salary_max, Job.salary_min_usd.is_(None))
        )

    # Source filter
    if source:
        stmt = stmt.where(Job.source == source.lower())

    # Count total (before pagination)
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = (await db.execute(count_stmt)).scalar_one()

    # Sorting
    if sort_by == SortBy.POSTED_AT:
        stmt = stmt.order_by(Job.posted_at.desc().nullslast())
    elif sort_by == SortBy.SALARY:
        stmt = stmt.order_by(Job.salary_max_usd.desc().nullslast())
    else:
        # Relevance: rough score — jobs with salary + description rank higher
        stmt = stmt.order_by(
            Job.salary_min_usd.is_(None).asc(),
            Job.posted_at.desc().nullslast(),
        )

    # Pagination
    offset = (page - 1) * page_size
    stmt = stmt.offset(offset).limit(page_size)

    rows = (await db.execute(stmt)).scalars().all()

    return JobListResponse(
        total=total,
        page=page,
        page_size=page_size,
        results=[JobResponse.model_validate(r) for r in rows],
    )


# ---------------------------------------------------------------------------
# GET /jobs/sources
# ---------------------------------------------------------------------------

@router.get("/sources", response_model=list[SourceStatsResponse])
async def get_sources(db: DBSession) -> list[SourceStatsResponse]:
    """Return health and stats for each job source."""
    rows = (await db.execute(select(JobSource).order_by(JobSource.name))).scalars().all()
    return [SourceStatsResponse.model_validate(r) for r in rows]


# ---------------------------------------------------------------------------
# GET /jobs/runs
# ---------------------------------------------------------------------------

@router.get("/runs", response_model=list[PipelineRunResponse])
async def get_pipeline_runs(
    db: DBSession,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
) -> list[PipelineRunResponse]:
    """Return the most recent pipeline run records."""
    stmt = (
        select(PipelineRun)
        .order_by(PipelineRun.started_at.desc())
        .limit(limit)
    )
    rows = (await db.execute(stmt)).scalars().all()
    return [PipelineRunResponse.model_validate(r) for r in rows]


# ---------------------------------------------------------------------------
# GET /jobs/{job_id}
# ---------------------------------------------------------------------------

@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: int, db: DBSession) -> JobResponse:
    """Fetch a single job by its database ID."""
    job = await db.get(Job, job_id)
    if not job or not job.is_active:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found.",
        )
    return JobResponse.model_validate(job)


# ---------------------------------------------------------------------------
# POST /jobs/trigger
# ---------------------------------------------------------------------------

@router.post("/trigger", status_code=status.HTTP_202_ACCEPTED)
async def trigger_pipeline() -> dict:
    """
    Manually trigger the extract DAG via Airflow REST API.
    In production set AIRFLOW_API_URL + AIRFLOW_API_TOKEN in env.
    """
    import os
    import httpx

    airflow_url = os.environ.get("AIRFLOW_API_URL", "http://localhost:8080")
    token = os.environ.get("AIRFLOW_API_TOKEN", "")

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{airflow_url}/api/v1/dags/job_board_extract/dagRuns",
                json={"conf": {}, "note": "Triggered via FastAPI /jobs/trigger"},
                headers=headers,
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Could not reach Airflow: {exc}",
        )

    return {
        "message": "Pipeline triggered successfully.",
        "dag_run_id": data.get("dag_run_id"),
        "state": data.get("state"),
    }