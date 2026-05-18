"""
Base extractor — all source-specific extractors inherit from this.
Provides: retry logic, structured logging, rate limiting, raw dump to disk.
"""

from __future__ import annotations

import abc
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = structlog.get_logger(__name__)

RAW_DATA_DIR = Path("data/raw")


class RawJob(dict):
    """
    Thin wrapper around dict so type-checkers stay happy.
    Each extractor returns a list[RawJob].
    """
    pass


class BaseExtractor(abc.ABC):
    """
    Abstract base for all job-board extractors.

    Subclasses must implement:
        - source_name: str  (e.g. "adzuna", "remoteok")
        - fetch(query, location, max_results) -> list[RawJob]

    Everything else (HTTP client, retries, persistence) lives here.
    """

    source_name: str = ""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or {}
        self._client: httpx.Client | None = None
        self.log = logger.bind(extractor=self.source_name)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(
        self,
        query: str,
        location: str = "",
        max_results: int = 50,
    ) -> list[RawJob]:
        """
        Entry point called by the DAG task.
        Delegates to fetch(), then persists raw output.
        """
        self.log.info("extraction_started", query=query, location=location)
        start = time.perf_counter()

        jobs = self.fetch(query=query, location=location, max_results=max_results)

        elapsed = round(time.perf_counter() - start, 2)
        self.log.info(
            "extraction_finished",
            count=len(jobs),
            elapsed_seconds=elapsed,
        )

        self._persist_raw(jobs, query=query, location=location)
        return jobs

    # ------------------------------------------------------------------
    # Abstract — implement per source
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def fetch(
        self,
        query: str,
        location: str,
        max_results: int,
    ) -> list[RawJob]:
        """Hit the external source and return normalised raw dicts."""
        ...

    # ------------------------------------------------------------------
    # HTTP helper (shared across all extractors)
    # ------------------------------------------------------------------

    def _get_client(self) -> httpx.Client:
        if self._client is None or self._client.is_closed:
            self._client = httpx.Client(
                timeout=httpx.Timeout(30.0),
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (compatible; JobBoardETL/1.0; "
                        "+https://github.com/your-org/job-board-etl)"
                    )
                },
            )
        return self._client

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _get(self, url: str, params: dict | None = None) -> httpx.Response:
        client = self._get_client()
        self.log.debug("http_get", url=url, params=params)
        resp = client.get(url, params=params)
        resp.raise_for_status()
        return resp

    # ------------------------------------------------------------------
    # Raw persistence
    # ------------------------------------------------------------------

    def _persist_raw(
        self,
        jobs: list[RawJob],
        query: str,
        location: str,
    ) -> Path:
        """Dump raw JSON to data/raw/<source>/<date>/<timestamp>.json"""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ts = datetime.now(timezone.utc).strftime("%H%M%S")

        out_dir = RAW_DATA_DIR / self.source_name / today
        out_dir.mkdir(parents=True, exist_ok=True)

        slug = query.replace(" ", "_")[:40]
        out_file = out_dir / f"{ts}_{slug}.json"

        payload = {
            "source": self.source_name,
            "query": query,
            "location": location,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "count": len(jobs),
            "jobs": jobs,
        }

        out_file.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        self.log.info("raw_persisted", path=str(out_file), count=len(jobs))
        return out_file

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def __enter__(self) -> "BaseExtractor":
        return self

    def __exit__(self, *args: Any) -> None:
        if self._client and not self._client.is_closed:
            self._client.close()