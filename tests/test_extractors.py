"""
tests/test_extractors.py — Unit tests for all extractors.

Uses httpx mock so no real HTTP calls are made.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from etl.extractors.adzuna import AdzunaExtractor
from etl.extractors.remoteok import RemoteOKExtractor
from etl.extractors.base import RawJob


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

ADZUNA_SAMPLE = {
    "results": [
        {
            "id": "123",
            "title": "Python Developer",
            "company": {"display_name": "TechCorp"},
            "location": {"display_name": "Bangalore, India"},
            "description": "We need a Python developer with FastAPI experience.",
            "redirect_url": "https://adzuna.com/job/123",
            "salary_min": 800000,
            "salary_max": 1200000,
            "contract_type": "permanent",
            "category": {"label": "IT Jobs"},
            "created": "2024-01-01T00:00:00Z",
        }
    ]
}

REMOTEOK_SAMPLE = [
    {"legal": "ignore this"},  # first item is metadata
    {
        "id": "456",
        "position": "Remote Backend Engineer",
        "company": "StartupXYZ",
        "description": "Looking for a Python and Django developer.",
        "url": "https://remoteok.com/job/456",
        "tags": ["python", "django", "remote"],
        "salary_min": "80000",
        "salary_max": "120000",
        "date": "2024-01-02T00:00:00Z",
    },
]


# ---------------------------------------------------------------------------
# Adzuna tests
# ---------------------------------------------------------------------------

class TestAdzunaExtractor:

    def test_normalize_maps_fields_correctly(self):
        extractor = AdzunaExtractor(
            config={"app_id": "test_id", "app_key": "test_key"}
        )
        raw = ADZUNA_SAMPLE["results"][0]
        job = extractor._normalize(raw)

        assert job["source"] == "adzuna"
        assert job["source_id"] == "123"
        assert job["title"] == "Python Developer"
        assert job["company"] == "TechCorp"
        assert job["location"] == "Bangalore, India"
        assert job["salary_min"] == 800000
        assert job["salary_max"] == 1200000

    def test_fetch_paginates_correctly(self):
        extractor = AdzunaExtractor(
            config={"app_id": "test_id", "app_key": "test_key"}
        )
        mock_response = MagicMock()
        mock_response.json.return_value = ADZUNA_SAMPLE

        with patch.object(extractor, "_get", return_value=mock_response):
            jobs = extractor.fetch(query="python", location="India", max_results=1)

        assert len(jobs) == 1
        assert jobs[0]["title"] == "Python Developer"

    def test_fetch_returns_empty_on_no_results(self):
        extractor = AdzunaExtractor(
            config={"app_id": "test_id", "app_key": "test_key"}
        )
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": []}

        with patch.object(extractor, "_get", return_value=mock_response):
            jobs = extractor.fetch(query="cobol", max_results=10)

        assert jobs == []


# ---------------------------------------------------------------------------
# RemoteOK tests
# ---------------------------------------------------------------------------

class TestRemoteOKExtractor:

    def test_normalize_maps_fields_correctly(self):
        extractor = RemoteOKExtractor()
        raw = REMOTEOK_SAMPLE[1]
        job = extractor._normalize(raw)

        assert job["source"] == "remoteok"
        assert job["source_id"] == "456"
        assert job["title"] == "Remote Backend Engineer"
        assert job["company"] == "StartupXYZ"
        assert job["location"] == "Remote"
        assert job["salary_min"] == 80000.0
        assert job["salary_max"] == 120000.0

    def test_skips_metadata_first_item(self):
        extractor = RemoteOKExtractor()
        mock_response = MagicMock()
        mock_response.json.return_value = REMOTEOK_SAMPLE

        with patch.object(extractor, "_get", return_value=mock_response):
            jobs = extractor.fetch(query="python", max_results=10)

        # Only 1 real job in sample (first is metadata)
        assert len(jobs) == 1

    def test_keyword_filter_works(self):
        extractor = RemoteOKExtractor()
        mock_response = MagicMock()
        mock_response.json.return_value = REMOTEOK_SAMPLE

        with patch.object(extractor, "_get", return_value=mock_response):
            # "java" not in sample job tags/title
            jobs = extractor.fetch(query="java", max_results=10)

        assert jobs == []

    def test_parse_salary_handles_string(self):
        assert RemoteOKExtractor._parse_salary("$120,000") == 120000.0
        assert RemoteOKExtractor._parse_salary(None) is None
        assert RemoteOKExtractor._parse_salary("invalid") is None