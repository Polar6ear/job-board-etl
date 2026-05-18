"""
RemoteOK extractor — uses the public RemoteOK JSON feed.

No API key required. Returns remote jobs worldwide.
Endpoint: https://remoteok.com/api  (returns JSON array)
"""

from __future__ import annotations

from typing import Any

import structlog

from etl.extractors.base import BaseExtractor, RawJob

logger = structlog.get_logger(__name__)

REMOTEOK_API_URL = "https://remoteok.com/api"


class RemoteOKExtractor(BaseExtractor):
    """
    Fetches remote jobs from RemoteOK's public API.
    No authentication needed — rate limit is ~1 req/min.

    The feed returns ALL jobs; we filter client-side by query keyword.
    """

    source_name = "remoteok"

    def fetch(
        self,
        query: str,
        location: str = "",   # RemoteOK is always remote, location ignored
        max_results: int = 50,
    ) -> list[RawJob]:
        self.log.info("remoteok_fetch_start", query=query)

        resp = self._get(
            REMOTEOK_API_URL,
            params={"tags": self._query_to_tags(query)},
        )

        # RemoteOK returns a JSON array; first element is metadata, skip it
        data = resp.json()
        if isinstance(data, list) and data:
            data = data[1:]   # drop the legal/metadata header object

        # Filter by query keywords against title + tags
        query_lower = query.lower()
        matched = [
            item for item in data
            if isinstance(item, dict) and self._matches(item, query_lower)
        ]

        self.log.info("remoteok_matched", total=len(data), matched=len(matched))

        return [self._normalize(j) for j in matched[:max_results]]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _query_to_tags(query: str) -> str:
        """Convert 'python developer' → 'python,developer' for API param."""
        return ",".join(word.lower() for word in query.split() if len(word) > 2)

    @staticmethod
    def _matches(job: dict, query_lower: str) -> bool:
        """Check if job title or tags contain any query keyword."""
        title = (job.get("position") or "").lower()
        tags = " ".join(job.get("tags") or []).lower()
        keywords = query_lower.split()
        return any(kw in title or kw in tags for kw in keywords)

    def _normalize(self, raw: dict) -> RawJob:
        """Map RemoteOK fields to our internal RawJob schema."""
        return RawJob(
            source=self.source_name,
            source_id=str(raw.get("id", "")),
            title=raw.get("position", ""),
            company=raw.get("company", ""),
            location="Remote",
            description=raw.get("description", ""),
            url=raw.get("url", ""),
            salary_min=self._parse_salary(raw.get("salary_min")),
            salary_max=self._parse_salary(raw.get("salary_max")),
            contract_type="remote",
            category=",".join(raw.get("tags") or []),
            posted_at=raw.get("date"),
            raw=raw,
        )

    @staticmethod
    def _parse_salary(value: Any) -> float | None:
        """RemoteOK sometimes sends salary as string or None."""
        if value is None:
            return None
        try:
            return float(str(value).replace(",", "").replace("$", ""))
        except (ValueError, TypeError):
            return None