"""
tests/test_api.py — Integration tests for FastAPI endpoints.

Uses FastAPI TestClient with a mocked DB session.
No real DB connection needed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.db.models import Job
from api.db.session import get_async_session


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_db_job(**kwargs) -> MagicMock:
    job = MagicMock(spec=Job)
    job.id = kwargs.get("id", 1)
    job.source = kwargs.get("source", "adzuna")
    job.source_id = kwargs.get("source_id", "abc123")
    job.title = kwargs.get("title", "Python Developer")
    job.company = kwargs.get("company", "TechCorp")
    job.location = kwargs.get("location", "Bangalore, India")
    job.description = kwargs.get("description", "Python and AWS role.")
    job.url = kwargs.get("url", "https://example.com/job/1")
    job.salary_min_usd = kwargs.get("salary_min_usd", 9600.0)
    job.salary_max_usd = kwargs.get("salary_max_usd", 14400.0)
    job.contract_type = kwargs.get("contract_type", "full_time")
    job.tags = kwargs.get("tags", ["python", "aws"])
    job.posted_at = kwargs.get("posted_at", datetime(2024, 1, 1, tzinfo=timezone.utc))
    job.normalized_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    job.created_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    job.is_active = True
    return job


# ---------------------------------------------------------------------------
# Client fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return TestClient(app)


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------

class TestHealth:

    def test_health_returns_200(self, client):
        with patch("api.db.session.AsyncSessionLocal") as mock_session_cls:
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=False)
            mock_session.execute = AsyncMock(
                return_value=MagicMock(scalar_one=MagicMock(return_value=42))
            )
            mock_session_cls.return_value = mock_session

            resp = client.get("/health")

        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data
        assert "db" in data
        assert "total_jobs" in data


# ---------------------------------------------------------------------------
# /jobs/search
# ---------------------------------------------------------------------------

class TestJobSearch:

    def _mock_session(self, jobs: list, total: int = None):
        """Return a mock async session that yields given jobs."""
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        total = total if total is not None else len(jobs)

        # Two execute calls: count query + data query
        mock_session.execute = AsyncMock(side_effect=[
            MagicMock(scalar_one=MagicMock(return_value=total)),
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=jobs)))),
        ])
        return mock_session

    def test_search_returns_200(self, client):
        jobs = [make_db_job()]
        mock_session = self._mock_session(jobs)

        with patch("api.routers.jobs.get_async_session") as mock_dep:
            mock_dep.return_value = mock_session
            app.dependency_overrides[get_async_session] = lambda: mock_session
            resp = client.get("/jobs/search?q=python")
            app.dependency_overrides.clear()

        assert resp.status_code == 200
        data = resp.json()
        assert "results" in data
        assert "total" in data
        assert data["page"] == 1

    def test_search_pagination_defaults(self, client):
        mock_session = self._mock_session([])
        app.dependency_overrides[get_async_session] = lambda: mock_session
        resp = client.get("/jobs/search")
        app.dependency_overrides.clear()

        assert resp.status_code == 200
        data = resp.json()
        assert data["page"] == 1
        assert data["page_size"] == 20

    def test_search_invalid_page_size(self, client):
        resp = client.get("/jobs/search?page_size=999")
        assert resp.status_code == 422   # Pydantic validation error

    def test_search_negative_salary(self, client):
        resp = client.get("/jobs/search?salary_min=-1")
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# /jobs/{job_id}
# ---------------------------------------------------------------------------

class TestGetJob:

    def test_get_existing_job(self, client):
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.get = AsyncMock(return_value=make_db_job(id=1))

        app.dependency_overrides[get_async_session] = lambda: mock_session
        resp = client.get("/jobs/1")
        app.dependency_overrides.clear()

        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == 1
        assert data["title"] == "Python Developer"

    def test_get_nonexistent_job_returns_404(self, client):
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.get = AsyncMock(return_value=None)

        app.dependency_overrides[get_async_session] = lambda: mock_session
        resp = client.get("/jobs/99999")
        app.dependency_overrides.clear()

        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()