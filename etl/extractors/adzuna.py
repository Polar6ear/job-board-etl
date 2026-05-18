"""
Adzuna extractor — uses the official Adzuna REST API.

Free tier: https://developer.adzuna.com/
Provides jobs from 20+ countries with salary data.
"""

from __future__ import annotations

import os
from typing import Any

import structlog

from etl.extractors.base import BaseExtractor, RawJob

logger = structlog.get_logger(__name__)

ADZUNA_BASE_URL = "https://api.adzuna.com/v1/api/jobs"
DEFAULT_COUNTRY = "in"   # India; change to "gb", "us", etc.
RESULTS_PER_PAGE = 50    # Adzuna max per page


class AdzunaExtractor(BaseExtractor):
    """
    Fetches jobs from Adzuna API.

    Required env vars (or pass via config dict):
        ADZUNA_APP_ID
        ADZUNA_API_KEY
    """

    source_name = "adzuna"

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        super().__init__(config)
        self.app_id = (
            self.config.get("app_id") or os.environ["ADZUNA_APP_ID"]
        )
        self.api_key = (
            self.config.get("api_key") or os.environ["ADZUNA_API_KEY"]
        )
        self.country = self.config.get("country", DEFAULT_COUNTRY)

    # ------------------------------------------------------------------
    # Core fetch
    # ------------------------------------------------------------------

    def fetch(
        self,
        query: str,
        location: str = "",
        max_results: int = 50,
    ) -> list[RawJob]:
        """
        Paginate through Adzuna results until max_results reached.
        Each page returns up to RESULTS_PER_PAGE jobs.
        """
        jobs: list[RawJob] = []
        page = 1
        pages_needed = -(-max_results // RESULTS_PER_PAGE)   # ceiling div

        while page <= pages_needed:
            self.log.info("adzuna_page_fetch", page=page, query=query)
            batch = self._fetch_page(
                query=query, location=location, page=page
            )
            if not batch:
                break

            jobs.extend(batch)
            if len(jobs) >= max_results:
                break
            page += 1

        return jobs[:max_results]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _fetch_page(
        self,
        query: str,
        location: str,
        page: int,
    ) -> list[RawJob]:
        url = f"{ADZUNA_BASE_URL}/{self.country}/search/{page}"

        params: dict[str, Any] = {
            "app_id": self.app_id,
            "app_key": self.api_key,
            "what": query,
            "results_per_page": RESULTS_PER_PAGE,
            "content-type": "application/json",
        }
        if location:
            params["where"] = location

        resp = self._get(url, params=params)
        data = resp.json()

        raw_jobs = data.get("results", [])
        return [self._normalize(j) for j in raw_jobs]

    def _normalize(self, raw: dict) -> RawJob:
        """
        Map Adzuna's field names to our internal RawJob schema.
        Downstream transformer will validate further.
        """
        return RawJob(
            source=self.source_name,
            source_id=str(raw.get("id", "")),
            title=raw.get("title", ""),
            company=raw.get("company", {}).get("display_name", ""),
            location=raw.get("location", {}).get("display_name", ""),
            description=raw.get("description", ""),
            url=raw.get("redirect_url", ""),
            salary_min=raw.get("salary_min"),
            salary_max=raw.get("salary_max"),
            contract_type=raw.get("contract_type"),
            category=raw.get("category", {}).get("label", ""),
            posted_at=raw.get("created"),
            raw=raw,   # keep original for debugging
        )