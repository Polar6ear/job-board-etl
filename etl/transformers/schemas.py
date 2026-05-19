from __future__ import annotations
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from pydantic import BaseModel, Field, field_validator, model_validator

class ContractType(str, Enum):
    FULL_TIME = "full_time"
    PART_TIME = "part_time"
    CONTRACT = "contract"
    REMOTE = "remote"
    INTERNSHIP = "internship"
    UNKNOWN = "unknown"

class JobSource(str, Enum):
    ADZUNA = "adzuna"
    REMOTEOK = "remoteok"
    LINKEDIN = "linkedin"
    UNKNOWN = "unknown"

class RawJobInput(BaseModel):
    model_config = {"extra": "allow"}
    source: str = ""
    source_id: str = ""
    title: str = ""
    company: str = ""
    location: str = ""
    description: str = ""
    url: str = ""
    salary_min: float | str | None = None
    salary_max: float | str | None = None
    contract_type: str | None = None
    category: str = ""
    posted_at: str | datetime | None = None
    raw: dict[str, Any] = Field(default_factory=dict)

class NormalizedJob(BaseModel):
    source: JobSource
    source_id: str
    title: str = Field(min_length=2, max_length=300)
    company: str = Field(min_length=1, max_length=200)
    location: str = Field(max_length=200)
    description: str = ""
    url: str = ""
    salary_min_usd: float | None = None
    salary_max_usd: float | None = None
    contract_type: ContractType = ContractType.UNKNOWN
    tags: list[str] = Field(default_factory=list)
    posted_at: datetime | None = None
    normalized_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    content_hash: str = ""

    @field_validator("title", "company", mode="before")
    @classmethod
    def strip_whitespace(cls, v):
        return str(v).strip() if v else ""

    @field_validator("tags", mode="before")
    @classmethod
    def parse_tags(cls, v):
        if isinstance(v, list): return [str(t).strip().lower() for t in v if t]
        if isinstance(v, str): return [t.strip().lower() for t in v.split(",") if t.strip()]
        return []

    @field_validator("salary_min_usd", "salary_max_usd", mode="before")
    @classmethod
    def parse_salary(cls, v):
        if v is None: return None
        try: return float(str(v).replace(",", "").replace("$", "").strip())
        except: return None

    @model_validator(mode="after")
    def salary_order(self):
        if self.salary_min_usd and self.salary_max_usd and self.salary_min_usd > self.salary_max_usd:
            self.salary_min_usd, self.salary_max_usd = self.salary_max_usd, self.salary_min_usd
        return self