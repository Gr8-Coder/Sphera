from __future__ import annotations

import argparse
import asyncio

from app.config import get_settings
from app.database import init_db
from app.services.pipeline import NewsPipeline


async def _run(args) -> None:
    settings = get_settings()
    settings.ensure_directories()
    init_db(settings.database_path)
    pipeline = NewsPipeline(settings)
    ingested = pipeline.bootstrap_companies()
    print(f"Ingested {ingested} companies from {settings.company_sheet_path}")

    if args.command == "refresh":
        company_names = [args.company] if args.company else None
        summary = await pipeline.refresh_news(
            company_names=company_names,
            trigger_source="cli",
            limit=args.limit,
        )
        print(
            "Refresh finished:",
            {
                "company_count": summary.company_count,
                "articles_scanned": summary.articles_scanned,
                "matched_articles": summary.matched_articles,
                "new_articles": summary.new_articles,
                "error_count": summary.error_count,
                "status": summary.status,
            },
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sphera task runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="Load the Excel sheet into SQLite")
    ingest_parser.set_defaults(command="ingest")

    refresh_parser = subparsers.add_parser("refresh", help="Fetch and classify news")
    refresh_parser.add_argument("--company", help="Refresh only a single company")
    refresh_parser.add_argument("--limit", type=int, help="Refresh only the first N companies")
    refresh_parser.set_defaults(command="refresh")

    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
