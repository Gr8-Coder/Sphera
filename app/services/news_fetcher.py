from __future__ import annotations

import html
import re
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import List, Optional

import feedparser
import httpx

from app.config import Settings
from app.schemas import RawNewsItem


class GoogleNewsFetcher:
    TRACKING_TERMS = (
        "distributor",
        "distribution",
        "dealer",
        '"working capital"',
        "receivables",
        '"channel partner"',
        "rural",
        '"tier 2"',
        '"tier 3"',
        '"secondary distribution"',
        '"secondary sales"',
    )

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch_for_company(self, company_name: str) -> List[RawNewsItem]:
        query = self._build_query(company_name)
        params = {
            "q": query,
            "hl": self.settings.rss_language,
            "gl": self.settings.rss_country,
            "ceid": self.settings.rss_edition,
        }
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0 Safari/537.36"
            )
        }
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            response = await client.get(
                "https://news.google.com/rss/search", params=params, headers=headers
            )
            response.raise_for_status()

        parsed_feed = feedparser.parse(response.text)
        items = []
        for entry in parsed_feed.entries[: self.settings.rss_max_results]:
            title = self._clean_text(entry.get("title", ""))
            summary = self._clean_summary(entry.get("summary", ""))
            source_name = self._extract_source_name(entry, title)
            if source_name and title.endswith(f" - {source_name}"):
                title = title[: -(len(source_name) + 3)].strip()

            source_url = entry.get("link", "")
            if not title or not source_url:
                continue

            items.append(
                RawNewsItem(
                    company_name=company_name,
                    title=title,
                    snippet=summary or title,
                    source_name=source_name,
                    source_url=source_url,
                    published_at=self._parse_published_at(
                        entry.get("published") or entry.get("updated")
                    ),
                    raw_summary=summary or None,
                )
            )
        return items

    def _build_query(self, company_name: str) -> str:
        joined_terms = " OR ".join(self.TRACKING_TERMS)
        return f'"{company_name}" ({joined_terms}) when:{self.settings.lookback_days}d'

    @staticmethod
    def _clean_summary(value: str) -> str:
        without_tags = re.sub(r"<[^>]+>", " ", value or "")
        return GoogleNewsFetcher._clean_text(without_tags)

    @staticmethod
    def _clean_text(value: str) -> str:
        value = html.unescape(value or "")
        value = re.sub(r"\s+", " ", value)
        return value.strip()

    @staticmethod
    def _extract_source_name(entry, title: str) -> Optional[str]:
        source = entry.get("source")
        if source and source.get("title"):
            return GoogleNewsFetcher._clean_text(source.get("title"))
        if " - " in title:
            source_candidate = title.rsplit(" - ", 1)[-1].strip()
            if 1 <= len(source_candidate) <= 48:
                return source_candidate
        return None

    @staticmethod
    def _parse_published_at(raw_value: Optional[str]):
        if not raw_value:
            return None
        parsed = parsedate_to_datetime(raw_value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

