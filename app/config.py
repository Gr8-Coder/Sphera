from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_COMPANY_SHEET = BASE_DIR / "data" / "companies.csv"


def _parse_refresh_hours(raw_value: str) -> Tuple[int, ...]:
    hours = []
    for chunk in raw_value.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        hour = int(chunk)
        if hour < 0 or hour > 23:
            raise ValueError("Refresh hours must be between 0 and 23.")
        hours.append(hour)
    if not hours:
        raise ValueError("At least one refresh hour must be configured.")
    return tuple(dict.fromkeys(hours))


def _parse_bool(raw_value: str, default: bool) -> bool:
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    company_sheet_path: Path
    database_path: Path
    timezone: str
    refresh_hours: Tuple[int, ...]
    lookback_days: int
    rss_max_results: int
    refresh_concurrency: int
    start_scheduler: bool
    run_refresh_on_start: bool
    live_refresh_enabled: bool
    live_refresh_interval_seconds: int
    live_refresh_batch_size: int
    live_stream_ping_seconds: int
    templates_dir: Path
    static_dir: Path

    @property
    def rss_language(self) -> str:
        return "en-IN"

    @property
    def rss_country(self) -> str:
        return "IN"

    @property
    def rss_edition(self) -> str:
        return f"{self.rss_country}:{self.rss_language.split('-')[0]}"

    def ensure_directories(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)


def get_settings() -> Settings:
    company_sheet_path = Path(
        os.getenv("SPHERA_COMPANY_SHEET", str(DEFAULT_COMPANY_SHEET))
    ).expanduser()
    database_path = Path(
        os.getenv("SPHERA_DB_PATH", str(BASE_DIR / "data" / "sphera.sqlite3"))
    ).expanduser()

    return Settings(
        company_sheet_path=company_sheet_path,
        database_path=database_path,
        timezone=os.getenv("SPHERA_TIMEZONE", "Asia/Kolkata"),
        refresh_hours=_parse_refresh_hours(
            os.getenv("SPHERA_REFRESH_HOURS", "8,11,14,17,20")
        ),
        lookback_days=int(os.getenv("SPHERA_LOOKBACK_DAYS", "7")),
        rss_max_results=int(os.getenv("SPHERA_RSS_MAX_RESULTS", "10")),
        refresh_concurrency=int(os.getenv("SPHERA_REFRESH_CONCURRENCY", "8")),
        start_scheduler=_parse_bool(os.getenv("SPHERA_START_SCHEDULER"), True),
        run_refresh_on_start=_parse_bool(
            os.getenv("SPHERA_RUN_REFRESH_ON_START"), False
        ),
        live_refresh_enabled=_parse_bool(
            os.getenv("SPHERA_LIVE_REFRESH_ENABLED"), True
        ),
        live_refresh_interval_seconds=int(
            os.getenv("SPHERA_LIVE_REFRESH_INTERVAL_SECONDS", "300")
        ),
        live_refresh_batch_size=int(
            os.getenv("SPHERA_LIVE_REFRESH_BATCH_SIZE", "25")
        ),
        live_stream_ping_seconds=int(
            os.getenv("SPHERA_LIVE_STREAM_PING_SECONDS", "15")
        ),
        templates_dir=BASE_DIR / "app" / "templates",
        static_dir=BASE_DIR / "app" / "static",
    )
