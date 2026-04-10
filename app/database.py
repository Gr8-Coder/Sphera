from __future__ import annotations

import sqlite3
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from app.schemas import ClassifiedNewsItem, CompanySeed


RETRYABLE_SQLITE_ERRORS = ("database is locked", "database schema is locked")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _connect(database_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database_path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    return connection


def _run_with_retry(callback):
    delay_seconds = 0.2
    for attempt in range(5):
        try:
            return callback()
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if attempt == 4 or not any(
                retryable in message for retryable in RETRYABLE_SQLITE_ERRORS
            ):
                raise
            time.sleep(delay_seconds)
            delay_seconds *= 2


def init_db(database_path: Path) -> None:
    def _init() -> None:
        with closing(_connect(database_path)) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    headquarters TEXT,
                    cfo TEXT,
                    email TEXT,
                    turnover TEXT,
                    ar TEXT,
                    dealer TEXT,
                    dso TEXT,
                    tech TEXT,
                    contact TEXT,
                    ar_secondary TEXT,
                    status TEXT,
                    last_refreshed_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS news_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
                    theme_slug TEXT NOT NULL,
                    theme_label TEXT NOT NULL,
                    signal TEXT NOT NULL,
                    signal_reason TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    title TEXT NOT NULL,
                    snippet TEXT NOT NULL,
                    source_name TEXT,
                    source_url TEXT NOT NULL,
                    published_at TEXT,
                    raw_summary TEXT,
                    item_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS refresh_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trigger_source TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    company_count INTEGER NOT NULL,
                    articles_scanned INTEGER NOT NULL DEFAULT 0,
                    matched_articles INTEGER NOT NULL DEFAULT 0,
                    new_articles INTEGER NOT NULL DEFAULT 0,
                    error_count INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_news_company_id
                    ON news_items(company_id);
                CREATE INDEX IF NOT EXISTS idx_news_theme_slug
                    ON news_items(theme_slug);
                CREATE INDEX IF NOT EXISTS idx_news_signal
                    ON news_items(signal);
                CREATE INDEX IF NOT EXISTS idx_news_published_at
                    ON news_items(published_at DESC);
                """
            )
            connection.commit()

    _run_with_retry(_init)


def upsert_companies(database_path: Path, companies: Iterable[CompanySeed]) -> int:
    company_rows = list(companies)
    timestamp = to_iso(utc_now())

    def _write() -> int:
        with closing(_connect(database_path)) as connection:
            connection.executemany(
                """
                INSERT INTO companies (
                    name, headquarters, cfo, email, turnover, ar, dealer, dso, tech,
                    contact, ar_secondary, status, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    headquarters = excluded.headquarters,
                    cfo = excluded.cfo,
                    email = excluded.email,
                    turnover = excluded.turnover,
                    ar = excluded.ar,
                    dealer = excluded.dealer,
                    dso = excluded.dso,
                    tech = excluded.tech,
                    contact = excluded.contact,
                    ar_secondary = excluded.ar_secondary,
                    status = excluded.status,
                    updated_at = excluded.updated_at
                """,
                [
                    (
                        company.name,
                        company.headquarters,
                        company.cfo,
                        company.email,
                        company.turnover,
                        company.ar,
                        company.dealer,
                        company.dso,
                        company.tech,
                        company.contact,
                        company.ar_secondary,
                        company.status,
                        timestamp,
                        timestamp,
                    )
                    for company in company_rows
                ],
            )
            connection.commit()
            return len(company_rows)

    return _run_with_retry(_write)


def get_company_lookup(database_path: Path) -> dict:
    with closing(_connect(database_path)) as connection:
        rows = connection.execute("SELECT id, name FROM companies ORDER BY name").fetchall()
    return {row["name"]: row["id"] for row in rows}


def list_company_names(database_path: Path) -> List[str]:
    with closing(_connect(database_path)) as connection:
        rows = connection.execute("SELECT name FROM companies ORDER BY name").fetchall()
    return [row["name"] for row in rows]


def mark_company_refreshed(database_path: Path, company_name: str) -> None:
    timestamp = to_iso(utc_now())

    def _write() -> None:
        with closing(_connect(database_path)) as connection:
            connection.execute(
                """
                UPDATE companies
                SET last_refreshed_at = ?, updated_at = ?
                WHERE name = ?
                """,
                (timestamp, timestamp, company_name),
            )
            connection.commit()

    _run_with_retry(_write)


def insert_news_items(
    database_path: Path,
    company_id: int,
    items: Iterable[ClassifiedNewsItem],
    item_hashes: Iterable[str],
) -> int:
    item_rows = list(zip(items, item_hashes))
    created_at = to_iso(utc_now())

    def _write() -> int:
        with closing(_connect(database_path)) as connection:
            cursor = connection.executemany(
                """
                INSERT INTO news_items (
                    company_id, theme_slug, theme_label, signal, signal_reason,
                    confidence, title, snippet, source_name, source_url,
                    published_at, raw_summary, item_hash, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_hash) DO NOTHING
                """,
                [
                    (
                        company_id,
                        item.theme_slug,
                        item.theme_label,
                        item.signal,
                        item.signal_reason,
                        item.confidence,
                        item.title,
                        item.snippet,
                        item.source_name,
                        item.source_url,
                        to_iso(item.published_at),
                        item.raw_summary,
                        item_hash,
                        created_at,
                    )
                    for item, item_hash in item_rows
                ],
            )
            connection.commit()
            return cursor.rowcount if cursor.rowcount != -1 else 0

    return _run_with_retry(_write)


def create_refresh_run(
    database_path: Path, trigger_source: str, scope: str, company_count: int
) -> int:
    started_at = to_iso(utc_now())

    def _write() -> int:
        with closing(_connect(database_path)) as connection:
            cursor = connection.execute(
                """
                INSERT INTO refresh_runs (
                    trigger_source, scope, company_count, status, started_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (trigger_source, scope, company_count, "running", started_at),
            )
            connection.commit()
            return int(cursor.lastrowid)

    return _run_with_retry(_write)


def finish_refresh_run(
    database_path: Path,
    run_id: int,
    *,
    articles_scanned: int,
    matched_articles: int,
    new_articles: int,
    error_count: int,
    status: str,
) -> None:
    completed_at = to_iso(utc_now())

    def _write() -> None:
        with closing(_connect(database_path)) as connection:
            connection.execute(
                """
                UPDATE refresh_runs
                SET articles_scanned = ?,
                    matched_articles = ?,
                    new_articles = ?,
                    error_count = ?,
                    status = ?,
                    completed_at = ?
                WHERE id = ?
                """,
                (
                    articles_scanned,
                    matched_articles,
                    new_articles,
                    error_count,
                    status,
                    completed_at,
                    run_id,
                ),
            )
            connection.commit()

    _run_with_retry(_write)


def list_news(
    database_path: Path,
    *,
    company_name: Optional[str] = None,
    theme_slug: Optional[str] = None,
    signal: Optional[str] = None,
    limit: int = 100,
):
    query = """
        SELECT
            news_items.id,
            companies.name AS company_name,
            news_items.theme_slug,
            news_items.theme_label,
            news_items.signal,
            news_items.signal_reason,
            news_items.confidence,
            news_items.title,
            news_items.snippet,
            news_items.source_name,
            news_items.source_url,
            news_items.published_at,
            news_items.created_at
        FROM news_items
        JOIN companies ON companies.id = news_items.company_id
        WHERE 1 = 1
    """
    params = []
    if company_name:
        query += " AND companies.name = ?"
        params.append(company_name)
    if theme_slug:
        query += " AND news_items.theme_slug = ?"
        params.append(theme_slug)
    if signal:
        query += " AND news_items.signal = ?"
        params.append(signal)
    query += """
        ORDER BY
            CASE WHEN news_items.published_at IS NULL THEN 1 ELSE 0 END,
            news_items.published_at DESC,
            news_items.created_at DESC
        LIMIT ?
    """
    params.append(limit)

    with closing(_connect(database_path)) as connection:
        return connection.execute(query, params).fetchall()


def list_companies(database_path: Path):
    query = """
        SELECT
            companies.id,
            companies.name,
            companies.headquarters,
            companies.status,
            companies.last_refreshed_at,
            COUNT(news_items.id) AS snippet_count,
            MAX(COALESCE(news_items.published_at, news_items.created_at)) AS latest_news_at
        FROM companies
        LEFT JOIN news_items ON news_items.company_id = companies.id
        GROUP BY companies.id
        ORDER BY
            CASE
                WHEN MAX(COALESCE(news_items.published_at, news_items.created_at)) IS NULL
                THEN 1 ELSE 0
            END,
            MAX(COALESCE(news_items.published_at, news_items.created_at)) DESC,
            companies.name ASC
    """
    with closing(_connect(database_path)) as connection:
        return connection.execute(query).fetchall()


def get_dashboard_metrics(database_path: Path) -> dict:
    with closing(_connect(database_path)) as connection:
        company_count = connection.execute(
            "SELECT COUNT(*) AS count FROM companies"
        ).fetchone()["count"]
        news_count = connection.execute(
            "SELECT COUNT(*) AS count FROM news_items"
        ).fetchone()["count"]
        last_run = connection.execute(
            """
            SELECT *
            FROM refresh_runs
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        positive_count = connection.execute(
            "SELECT COUNT(*) AS count FROM news_items WHERE signal = 'positive'"
        ).fetchone()["count"]
        negative_count = connection.execute(
            "SELECT COUNT(*) AS count FROM news_items WHERE signal = 'negative'"
        ).fetchone()["count"]

    return {
        "company_count": company_count,
        "news_count": news_count,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "last_run": dict(last_run) if last_run else None,
    }

