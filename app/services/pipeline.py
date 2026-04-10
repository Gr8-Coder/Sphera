from __future__ import annotations

import asyncio
from typing import Iterable, Optional, Sequence

from app.config import Settings
from app.database import (
    create_refresh_run,
    finish_refresh_run,
    get_company_lookup,
    list_company_names,
    mark_company_refreshed,
    upsert_companies,
    insert_news_items,
)
from app.schemas import RefreshSummary
from app.services.classifier import CredServClassifier, build_item_hash
from app.services.company_loader import load_companies_from_excel
from app.services.news_fetcher import GoogleNewsFetcher
from app.database import utc_now


class NewsPipeline:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.fetcher = GoogleNewsFetcher(settings)
        self.classifier = CredServClassifier()

    def bootstrap_companies(self) -> int:
        companies = load_companies_from_excel(self.settings.company_sheet_path)
        return upsert_companies(self.settings.database_path, companies)

    async def refresh_news(
        self,
        *,
        company_names: Optional[Sequence[str]] = None,
        trigger_source: str = "manual",
        limit: Optional[int] = None,
    ) -> RefreshSummary:
        selected_company_names = list(company_names or list_company_names(self.settings.database_path))
        if limit is not None:
            selected_company_names = selected_company_names[:limit]

        scope = "full-watchlist"
        if company_names:
            scope = "single-company" if len(selected_company_names) == 1 else "batch"

        run_id = create_refresh_run(
            self.settings.database_path,
            trigger_source=trigger_source,
            scope=scope,
            company_count=len(selected_company_names),
        )
        started_at = utc_now()
        semaphore = asyncio.Semaphore(self.settings.refresh_concurrency)
        company_lookup = get_company_lookup(self.settings.database_path)

        async def process_company(company_name: str) -> dict:
            async with semaphore:
                try:
                    raw_items = await self.fetcher.fetch_for_company(company_name)
                    classified_items = [
                        classified
                        for raw_item in raw_items
                        if (classified := self.classifier.classify(raw_item)) is not None
                    ]
                    inserted = 0
                    company_id = company_lookup.get(company_name)
                    if company_id and classified_items:
                        inserted = insert_news_items(
                            self.settings.database_path,
                            company_id,
                            classified_items,
                            [build_item_hash(item) for item in classified_items],
                        )
                    mark_company_refreshed(self.settings.database_path, company_name)
                    return {
                        "company_name": company_name,
                        "articles_scanned": len(raw_items),
                        "matched_articles": len(classified_items),
                        "new_articles": inserted,
                        "error": None,
                    }
                except Exception as exc:  # pragma: no cover
                    return {
                        "company_name": company_name,
                        "articles_scanned": 0,
                        "matched_articles": 0,
                        "new_articles": 0,
                        "error": str(exc),
                    }

        results = await asyncio.gather(
            *(process_company(company_name) for company_name in selected_company_names)
        )
        articles_scanned = sum(result["articles_scanned"] for result in results)
        matched_articles = sum(result["matched_articles"] for result in results)
        new_articles = sum(result["new_articles"] for result in results)
        error_count = sum(1 for result in results if result["error"])
        status = "completed_with_errors" if error_count else "completed"

        finish_refresh_run(
            self.settings.database_path,
            run_id,
            articles_scanned=articles_scanned,
            matched_articles=matched_articles,
            new_articles=new_articles,
            error_count=error_count,
            status=status,
        )

        return RefreshSummary(
            run_id=run_id,
            trigger_source=trigger_source,
            scope=scope,
            company_count=len(selected_company_names),
            articles_scanned=articles_scanned,
            matched_articles=matched_articles,
            new_articles=new_articles,
            error_count=error_count,
            status=status,
            started_at=started_at,
            completed_at=utc_now(),
        )
