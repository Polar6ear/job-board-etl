"""
LinkedIn public jobs scraper.

Scrapes LinkedIn's public job listings (no login required).
Uses BeautifulSoup + httpx. Respects robots.txt delay.

NOTE: LinkedIn rate-limits aggressively. Keep max_results low
      and add delay between requests in production.
"""

from __future__ import annotations

import time
from typing import Any

import structlog
from bs4 import BeautifulSoup

from etl.extractors.base import BaseExtractor, RawJob

logger = structlog.get_logger(__name__)

LINKEDIN_JOBS_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
REQUEST_DELAY_SECONDS = 2   # Be polite; don't hammer LinkedIn


class LinkedInScraper(BaseExtractor):
    """
    Scrapes LinkedIn public job listings.

    LinkedIn's guest API endpoint:
    /jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=...&location=...&start=0

    Each page returns 25 jobs as HTML fragments.
    """

    source_name = "linkedin"

    def fetch(
        self,
        query: str,
        location: str = "",
        max_results: int = 25,
    ) -> list[RawJob]:
        jobs: list[RawJob] = []
        start = 0
        page_size = 25

        while len(jobs) < max_results:
            self.log.info("linkedin_page_fetch", start=start, query=query)

            html = self._fetch_page_html(
                query=query, location=location, start=start
            )
            if not html:
                break

            batch = self._parse_page(html)
            if not batch:
                self.log.info("linkedin_no_more_results", start=start)
                break

            jobs.extend(batch)
            start += page_size
            time.sleep(REQUEST_DELAY_SECONDS)   # rate-limit guard

        return jobs[:max_results]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _fetch_page_html(
        self,
        query: str,
        location: str,
        start: int,
    ) -> str | None:
        params: dict[str, Any] = {
            "keywords": query,
            "location": location,
            "start": start,
            "sortBy": "DD",   # most recent first
        }
        try:
            resp = self._get(LINKEDIN_JOBS_URL, params=params)
            return resp.text
        except Exception as exc:
            self.log.warning("linkedin_fetch_failed", error=str(exc), start=start)
            return None

    def _parse_page(self, html: str) -> list[RawJob]:
        soup = BeautifulSoup(html, "lxml")
        cards = soup.find_all("div", class_="base-card")

        jobs: list[RawJob] = []
        for card in cards:
            job = self._parse_card(card)
            if job:
                jobs.append(job)
        return jobs

    def _parse_card(self, card: Any) -> RawJob | None:
        try:
            title_el = card.find("h3", class_="base-search-card__title")
            company_el = card.find("h4", class_="base-search-card__subtitle")
            location_el = card.find("span", class_="job-search-card__location")
            link_el = card.find("a", class_="base-card__full-link")
            time_el = card.find("time")

            # Extract job_id from the data-entity-urn attribute
            urn = card.get("data-entity-urn", "")
            job_id = urn.split(":")[-1] if urn else ""

            return RawJob(
                source=self.source_name,
                source_id=job_id,
                title=title_el.get_text(strip=True) if title_el else "",
                company=company_el.get_text(strip=True) if company_el else "",
                location=location_el.get_text(strip=True) if location_el else "",
                description="",   # requires detail page fetch (Step 3 enhancement)
                url=link_el["href"].split("?")[0] if link_el else "",
                salary_min=None,
                salary_max=None,
                contract_type=None,
                category="",
                posted_at=time_el.get("datetime") if time_el else None,
                raw={
                    "title": title_el.get_text(strip=True) if title_el else "",
                    "company": company_el.get_text(strip=True) if company_el else "",
                    "job_id": job_id,
                },
            )
        except Exception as exc:
            self.log.warning("linkedin_card_parse_error", error=str(exc))
            return None