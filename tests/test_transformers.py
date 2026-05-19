"""
tests/test_transformers.py — Unit tests for Normalizer and Deduplicator.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from etl.transformers.normalizer import Normalizer
from etl.transformers.deduplicator import Deduplicator
from etl.transformers.schemas import ContractType, JobSource, NormalizedJob


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_raw_job(**kwargs) -> dict:
    base = {
        "source": "adzuna",
        "source_id": "123",
        "title": "Python Developer",
        "company": "TechCorp",
        "location": "Bangalore, India",
        "description": "We need a Python and AWS developer.",
        "url": "https://example.com/job/123",
        "salary_min": 800000,
        "salary_max": 1200000,
        "contract_type": "permanent",
        "category": "IT Jobs",
        "posted_at": "2024-01-01T00:00:00Z",
        "raw": {},
    }
    base.update(kwargs)
    return base


def make_normalized_job(**kwargs) -> NormalizedJob:
    base = dict(
        source=JobSource.ADZUNA,
        source_id="abc123",
        title="Python Developer",
        company="TechCorp",
        location="Bangalore",
        description="Python and AWS role",
        url="https://example.com/job/1",
        salary_min_usd=9600.0,
        salary_max_usd=14400.0,
        contract_type=ContractType.FULL_TIME,
        tags=["python", "aws"],
        posted_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        normalized_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        content_hash="abc123def456",
    )
    base.update(kwargs)
    return NormalizedJob(**base)


# ---------------------------------------------------------------------------
# Normalizer tests
# ---------------------------------------------------------------------------

class TestNormalizer:

    def setup_method(self):
        self.normalizer = Normalizer()

    def test_valid_job_is_normalized(self):
        result = self.normalizer.run([make_raw_job()])
        assert len(result.valid) == 1
        assert len(result.invalid) == 0
        job = result.valid[0]
        assert job.title == "Python Developer"
        assert job.company == "TechCorp"
        assert job.source == JobSource.ADZUNA

    def test_html_stripped_from_description(self):
        raw = make_raw_job(description="<p>We need a <b>Python</b> developer.</p>")
        result = self.normalizer.run([raw])
        assert "<p>" not in result.valid[0].description
        assert "<b>" not in result.valid[0].description
        assert "Python" in result.valid[0].description

    def test_empty_title_is_skipped(self):
        result = self.normalizer.run([make_raw_job(title="")])
        assert len(result.valid) == 0

    def test_empty_company_is_skipped(self):
        result = self.normalizer.run([make_raw_job(company="")])
        assert len(result.valid) == 0

    def test_contract_type_inferred_from_title(self):
        result = self.normalizer.run([make_raw_job(
            title="Remote Python Developer",
            contract_type=None,
        )])
        assert result.valid[0].contract_type == ContractType.REMOTE

    def test_internship_inferred_from_title(self):
        result = self.normalizer.run([make_raw_job(
            title="Software Engineering Intern",
            contract_type=None,
        )])
        assert result.valid[0].contract_type == ContractType.INTERNSHIP

    def test_inr_salary_converted_to_usd(self):
        result = self.normalizer.run([make_raw_job(
            salary_min=800000,
            salary_max=1200000,
            location="Bangalore, India",
        )])
        job = result.valid[0]
        # 800000 INR * 0.012 = 9600 USD
        assert job.salary_min_usd == pytest.approx(9600.0)
        assert job.salary_max_usd == pytest.approx(14400.0)

    def test_tech_tags_extracted_from_description(self):
        result = self.normalizer.run([make_raw_job(
            description="We need Python, AWS, and Docker experience."
        )])
        tags = result.valid[0].tags
        assert "python" in tags
        assert "aws" in tags
        assert "docker" in tags

    def test_content_hash_is_consistent(self):
        raw = make_raw_job()
        result1 = self.normalizer.run([raw])
        result2 = self.normalizer.run([raw])
        assert result1.valid[0].content_hash == result2.valid[0].content_hash

    def test_multiple_jobs_processed(self):
        raws = [make_raw_job(source_id=str(i), title=f"Job {i}") for i in range(5)]
        result = self.normalizer.run(raws)
        assert len(result.valid) == 5

    def test_invalid_source_becomes_unknown(self):
        result = self.normalizer.run([make_raw_job(source="unknown_source")])
        assert result.valid[0].source == JobSource.UNKNOWN


# ---------------------------------------------------------------------------
# Deduplicator tests
# ---------------------------------------------------------------------------

class TestDeduplicator:

    def setup_method(self):
        self.dedup = Deduplicator()

    def test_exact_duplicate_removed(self):
        job1 = make_normalized_job(content_hash="samehash")
        job2 = make_normalized_job(content_hash="samehash")
        unique, removed = self.dedup.run([job1, job2])
        assert len(unique) == 1
        assert len(removed) == 1

    def test_unique_jobs_kept(self):
        job1 = make_normalized_job(content_hash="hash1", source_id="1")
        job2 = make_normalized_job(
            content_hash="hash2",
            source_id="2",
            title="Data Engineer",
            company="OtherCorp",
        )
        unique, removed = self.dedup.run([job1, job2])
        assert len(unique) == 2
        assert len(removed) == 0

    def test_fuzzy_duplicate_same_company_removed(self):
        job1 = make_normalized_job(
            content_hash="hash1",
            source_id="1",
            title="Senior Python Developer",
        )
        job2 = make_normalized_job(
            content_hash="hash2",
            source_id="2",
            title="Senior Python Developer",   # identical title, same company
        )
        unique, removed = self.dedup.run([job1, job2])
        assert len(unique) == 1
        assert len(removed) == 1

    def test_similar_title_different_company_kept(self):
        job1 = make_normalized_job(
            content_hash="hash1",
            source_id="1",
            title="Python Developer",
            company="CompanyA",
        )
        job2 = make_normalized_job(
            content_hash="hash2",
            source_id="2",
            title="Python Developer",
            company="CompanyB",   # different company — not a dupe
        )
        unique, removed = self.dedup.run([job1, job2])
        assert len(unique) == 2

    def test_richer_record_kept_on_exact_dupe(self):
        poor = make_normalized_job(
            content_hash="samehash",
            description="",
            salary_min_usd=None,
        )
        rich = make_normalized_job(
            content_hash="samehash",
            description="A very detailed job description with lots of info.",
            salary_min_usd=10000.0,
        )
        unique, removed = self.dedup.run([poor, rich])
        assert unique[0].description == rich.description

    def test_empty_list(self):
        unique, removed = self.dedup.run([])
        assert unique == []
        assert removed == []