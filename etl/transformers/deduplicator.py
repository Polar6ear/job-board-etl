from __future__ import annotations
import re
from difflib import SequenceMatcher
import structlog
from etl.transformers.schemas import NormalizedJob

logger = structlog.get_logger(__name__)
FUZZY_TITLE_THRESHOLD = 0.85

def _similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

def _slug(text):
    return re.sub(r"[^a-z0-9 ]", "", text.lower()).strip()

class Deduplicator:
    def __init__(self, fuzzy_threshold=FUZZY_TITLE_THRESHOLD):
        self.fuzzy_threshold = fuzzy_threshold
        self.log = logger.bind(component="deduplicator")

    def run(self, jobs):
        after_exact, exact_dupes = self._exact_pass(jobs)
        after_fuzzy, fuzzy_dupes = self._fuzzy_pass(after_exact)
        removed = exact_dupes + fuzzy_dupes
        self.log.info("dedup_complete", input=len(jobs), output=len(after_fuzzy),
            exact_removed=len(exact_dupes), fuzzy_removed=len(fuzzy_dupes))
        return after_fuzzy, removed

    def _exact_pass(self, jobs):
        seen, dupes = {}, []
        for job in jobs:
            key = job.content_hash
            if key in seen:
                if self._score(job) > self._score(seen[key]):
                    dupes.append(seen[key])
                    seen[key] = job
                else:
                    dupes.append(job)
            else:
                seen[key] = job
        return list(seen.values()), dupes

    def _fuzzy_pass(self, jobs):
        by_company = {}
        for job in jobs:
            key = _slug(job.company)[:30]
            by_company.setdefault(key, []).append(job)
        unique, dupes = [], []
        for group in by_company.values():
            u, d = self._fuzzy_within_group(group)
            unique.extend(u)
            dupes.extend(d)
        return unique, dupes

    def _fuzzy_within_group(self, jobs):
        kept, dupes = [], []
        for job in jobs:
            is_dupe = False
            slug = _slug(job.title)
            for existing in kept:
                if _similarity(slug, _slug(existing.title)) >= self.fuzzy_threshold:
                    if self._score(job) > self._score(existing):
                        kept.remove(existing)
                        dupes.append(existing)
                        kept.append(job)
                    else:
                        dupes.append(job)
                    is_dupe = True
                    break
            if not is_dupe:
                kept.append(job)
        return kept, dupes

    @staticmethod
    def _score(job):
        score = 0
        if job.description and len(job.description) > 100: score += 2
        if job.salary_min_usd: score += 2
        if job.salary_max_usd: score += 1
        if job.posted_at: score += 1
        if job.tags: score += 1
        return score