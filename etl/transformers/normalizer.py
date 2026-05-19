from __future__ import annotations
import hashlib, re
from datetime import datetime, timezone
from typing import Any
import structlog
from etl.transformers.schemas import ContractType, JobSource, NormalizedJob, RawJobInput

logger = structlog.get_logger(__name__)
_FX_TO_USD = {"inr": 0.012, "gbp": 1.27, "eur": 1.08, "usd": 1.0}
_CONTRACT_HINTS = [
    (ContractType.INTERNSHIP, ["intern", "internship", "trainee"]),
    (ContractType.PART_TIME, ["part-time", "part time", "parttime"]),
    (ContractType.REMOTE, ["remote", "work from home", "wfh", "distributed"]),
    (ContractType.CONTRACT, ["contract", "freelance", "consultant", "temporary"]),
    (ContractType.FULL_TIME, ["full-time", "full time", "permanent"]),
]
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_SPACE_RE = re.compile(r"\s{2,}")

class Normalizer:
    def __init__(self):
        self.log = logger.bind(component="normalizer")

    def run(self, raw_jobs):
        valid, invalid = [], []
        for raw in raw_jobs:
            try:
                job = self._normalize_one(raw)
                if job:
                    valid.append(job)
            except Exception as exc:
                invalid.append({"raw": raw, "error": str(exc)})
        self.log.info("normalization_complete", valid=len(valid), invalid=len(invalid))
        return NormalizationResult(valid=valid, invalid=invalid)

    def _normalize_one(self, raw):
        inp = RawJobInput.model_validate(raw)
        if not inp.title.strip() or not inp.company.strip():
            return None
        title = self._clean_text(inp.title)
        company = self._clean_text(inp.company)
        description = self._clean_text(inp.description)
        location = self._clean_text(inp.location)
        contract_type = self._infer_contract_type(inp.contract_type, title, description)
        source = self._parse_source(inp.source)
        posted_at = self._parse_datetime(inp.posted_at)
        sal_min, sal_max = self._normalize_salary(inp.salary_min, inp.salary_max, location)
        tags = self._extract_tags(inp.category, description)
        content_hash = self._hash(source.value, inp.source_id, title, company)
        return NormalizedJob(source=source, source_id=inp.source_id or content_hash[:12],
            title=title, company=company, location=location, description=description[:5000],
            url=inp.url, salary_min_usd=sal_min, salary_max_usd=sal_max,
            contract_type=contract_type, tags=tags, posted_at=posted_at, content_hash=content_hash)

    @staticmethod
    def _clean_text(text):
        if not text: return ""
        s = _HTML_TAG_RE.sub(" ", str(text)).replace("\xa0", " ")
        return _MULTI_SPACE_RE.sub(" ", s).strip()

    @staticmethod
    def _parse_source(source):
        try: return JobSource(source.lower())
        except ValueError: return JobSource.UNKNOWN

    @staticmethod
    def _parse_datetime(value):
        if value is None: return None
        if isinstance(value, datetime):
            return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
        if isinstance(value, str):
            for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try: return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
                except ValueError: continue
        return None

    @staticmethod
    def _infer_contract_type(raw_type, title, description):
        haystack = f"{raw_type or ''} {title} {description}".lower()
        for ct, keywords in _CONTRACT_HINTS:
            if any(kw in haystack for kw in keywords): return ct
        return ContractType.UNKNOWN

    @staticmethod
    def _normalize_salary(sal_min, sal_max, location):
        def to_float(v):
            if v is None: return None
            try: return float(str(v).replace(",", "").replace("$", ""))
            except: return None
        loc = location.lower()
        if any(x in loc for x in ["india", "bengaluru", "mumbai"]): currency = "inr"
        elif any(x in loc for x in ["uk", "london", "united kingdom"]): currency = "gbp"
        elif any(x in loc for x in ["europe", "germany", "france"]): currency = "eur"
        else: currency = "usd"
        rate = _FX_TO_USD.get(currency, 1.0)
        s_min, s_max = to_float(sal_min), to_float(sal_max)
        if currency == "inr" and s_min and s_min <= 100:
            s_min = s_min * 100_000
            s_max = s_max * 100_000 if s_max else None
        return (round(s_min * rate, 2) if s_min else None, round(s_max * rate, 2) if s_max else None)

    @staticmethod
    def _extract_tags(category, description):
        tech_keywords = {"python","javascript","typescript","java","go","rust","react","node",
            "django","fastapi","flask","aws","gcp","azure","docker","kubernetes","terraform",
            "postgresql","mysql","mongodb","redis","kafka","machine learning","devops","graphql"}
        tags = set()
        for part in re.split(r"[,\s]+", category.lower()):
            if part: tags.add(part)
        for kw in tech_keywords:
            if kw in description.lower(): tags.add(kw)
        return sorted(tags)[:20]

    @staticmethod
    def _hash(*parts):
        content = "|".join(str(p).lower().strip() for p in parts)
        return hashlib.sha256(content.encode()).hexdigest()

class NormalizationResult:
    def __init__(self, valid, invalid):
        self.valid = valid
        self.invalid = invalid