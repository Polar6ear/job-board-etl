"""
api/schemas/job.py — Pydantic v2 response/request schemas for FastAPI.

Separate from etl/transformers/schemas.py intentionally:
  - ETL schemas deal with raw/normalized data during pipeline
  - API schemas deal with what the client sends and receives
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ContractTypeFilter(str, Enum):
    FULL_TIME = "full_time"
    PART_TIME = "part_time"
    CONTRACT = "contract"
    REMOTE = "remote"
    INTERNSHIP = "internship"
    UNKNOWN = "unknown"


class SortBy(str, Enum):
    POSTED_AT = "posted_at"
    SALARY = "salary"
    RELEVANCE = "relevance"


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------

class JobResponse(BaseModel):
    id: int
    source: str
    source_id: str
    title: str
    company: str
    location: str
    description: str
    url: str
    salary_min_usd: float | None
    salary_max_usd: float | None
    contract_type: str
    tags: list[str]
    posted_at: datetime | None
    normalized_at: datetime
    created_at: datetime

    model_config = {"from_attributes": True}


class JobListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    results: list[JobResponse]


class SourceStatsResponse(BaseModel):
    name: str
    is_enabled: bool
    last_run_at: datetime | None
    last_run_count: int
    total_extracted: int

    model_config = {"from_attributes": True}


class PipelineRunResponse(BaseModel):
    id: int
    dag_id: str
    run_id: str
    status: str
    raw_count: int
    normalized_count: int
    deduped_count: int
    upserted_count: int
    error_message: str | None
    started_at: datetime
    finished_at: datetime | None

    model_config = {"from_attributes": True}


class HealthResponse(BaseModel):
    status: str
    db: str
    total_jobs: int
    version: str = "1.0.0"