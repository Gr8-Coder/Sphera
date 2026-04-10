from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class CompanySeed:
    name: str
    headquarters: Optional[str] = None
    cfo: Optional[str] = None
    email: Optional[str] = None
    turnover: Optional[str] = None
    ar: Optional[str] = None
    dealer: Optional[str] = None
    dso: Optional[str] = None
    tech: Optional[str] = None
    contact: Optional[str] = None
    ar_secondary: Optional[str] = None
    status: Optional[str] = None


@dataclass(frozen=True)
class RawNewsItem:
    company_name: str
    title: str
    snippet: str
    source_name: Optional[str]
    source_url: str
    published_at: Optional[datetime]
    raw_summary: Optional[str] = None


@dataclass(frozen=True)
class ClassifiedNewsItem:
    company_name: str
    title: str
    snippet: str
    source_name: Optional[str]
    source_url: str
    published_at: Optional[datetime]
    raw_summary: Optional[str]
    theme_slug: str
    theme_label: str
    signal: str
    signal_reason: str
    confidence: float


@dataclass(frozen=True)
class RefreshSummary:
    run_id: int
    trigger_source: str
    scope: str
    company_count: int
    articles_scanned: int
    matched_articles: int
    new_articles: int
    error_count: int
    status: str
    started_at: datetime
    completed_at: datetime
