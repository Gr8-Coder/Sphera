from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from typing import Optional, Sequence
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import Settings, get_settings
from app.database import (
    get_dashboard_metrics,
    init_db,
    list_companies,
    list_company_names,
    list_news,
)
from app.schemas import RefreshSummary
from app.services.classifier import TRACKED_THEMES
from app.services.live_updates import LiveUpdateBroker, format_sse, next_company_batch
from app.services.pipeline import NewsPipeline


settings = get_settings()
templates = Jinja2Templates(directory=str(settings.templates_dir))


def _localize_timestamp(raw_value: Optional[str], settings: Settings) -> Optional[datetime]:
    if not raw_value:
        return None
    parsed = datetime.fromisoformat(raw_value)
    return parsed.astimezone(ZoneInfo(settings.timezone))


def _format_timestamp(raw_value: Optional[str], settings: Settings) -> str:
    parsed = _localize_timestamp(raw_value, settings)
    if not parsed:
        return "Not available"
    return parsed.strftime("%d %b %Y, %I:%M %p")


def _serialize_company(row, settings: Settings) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "headquarters": row["headquarters"],
        "status": row["status"],
        "snippet_count": row["snippet_count"],
        "last_refreshed_at": row["last_refreshed_at"],
        "last_refreshed_at_display": _format_timestamp(
            row["last_refreshed_at"], settings
        ),
        "latest_news_at": row["latest_news_at"],
        "latest_news_at_display": _format_timestamp(row["latest_news_at"], settings),
    }


def _serialize_news(row, settings: Settings) -> dict:
    published_at = row["published_at"] or row["created_at"]
    return {
        "id": row["id"],
        "company_name": row["company_name"],
        "theme_slug": row["theme_slug"],
        "theme_label": row["theme_label"],
        "signal": row["signal"],
        "signal_reason": row["signal_reason"],
        "confidence": row["confidence"],
        "title": row["title"],
        "snippet": row["snippet"],
        "source_name": row["source_name"] or "Source unavailable",
        "source_url": row["source_url"],
        "published_at": published_at,
        "published_at_display": _format_timestamp(published_at, settings),
    }


def _serialize_refresh_summary(summary: Optional[RefreshSummary]) -> Optional[dict]:
    if not summary:
        return None
    return {
        "run_id": summary.run_id,
        "trigger_source": summary.trigger_source,
        "scope": summary.scope,
        "company_count": summary.company_count,
        "articles_scanned": summary.articles_scanned,
        "matched_articles": summary.matched_articles,
        "new_articles": summary.new_articles,
        "error_count": summary.error_count,
        "status": summary.status,
        "started_at": summary.started_at.isoformat(),
        "completed_at": summary.completed_at.isoformat(),
    }


def _serialize_last_run(metrics: dict, settings: Settings) -> Optional[dict]:
    last_run = metrics.get("last_run")
    if not last_run:
        return None
    return {
        **last_run,
        "started_at_display": _format_timestamp(last_run.get("started_at"), settings),
        "completed_at_display": _format_timestamp(
            last_run.get("completed_at"), settings
        ),
    }


def _status_payload(app: FastAPI) -> dict:
    refresh_task = getattr(app.state, "refresh_task", None)
    running = bool(refresh_task and not refresh_task.done())
    latest_summary = getattr(app.state, "latest_refresh_summary", None)
    return {
        "running": running,
        "latest_refresh_summary": _serialize_refresh_summary(latest_summary),
        "refresh_hours": settings.refresh_hours,
        "timezone": settings.timezone,
        "live_refresh_enabled": settings.live_refresh_enabled,
        "live_refresh_interval_seconds": settings.live_refresh_interval_seconds,
        "live_refresh_batch_size": settings.live_refresh_batch_size,
    }


def _build_snapshot_payload(
    app: FastAPI,
    *,
    company: Optional[str] = None,
    theme: Optional[str] = None,
    signal: Optional[str] = None,
    limit: int = 80,
) -> dict:
    metrics = get_dashboard_metrics(settings.database_path)
    company_rows = list_companies(settings.database_path)
    news_rows = list_news(
        settings.database_path,
        company_name=company,
        theme_slug=theme,
        signal=signal,
        limit=limit,
    )
    return {
        "metrics": {
            **metrics,
            "last_run": _serialize_last_run(metrics, settings),
        },
        "status_payload": _status_payload(app),
        "companies": [_serialize_company(row, settings) for row in company_rows],
        "news_items": [_serialize_news(row, settings) for row in news_rows],
        "selected_company": company,
        "selected_theme": theme,
        "selected_signal": signal,
        "limit": limit,
        "generated_at": datetime.now(ZoneInfo(settings.timezone)).isoformat(),
    }


async def _publish_refresh_event(
    app: FastAPI,
    event_name: str,
    *,
    trigger_source: str,
    company_names: Optional[Sequence[str]],
    summary: Optional[RefreshSummary] = None,
) -> None:
    preview = list(company_names or [])[:5]
    payload = {
        "trigger_source": trigger_source,
        "company_count": len(company_names or []),
        "company_names_preview": preview,
        "summary": _serialize_refresh_summary(summary),
    }
    await app.state.live_updates.publish(event_name, payload)


async def _queue_refresh(
    app: FastAPI,
    *,
    company_names: Optional[Sequence[str]] = None,
    trigger_source: str,
    limit: Optional[int] = None,
) -> bool:
    refresh_task = getattr(app.state, "refresh_task", None)
    if refresh_task and not refresh_task.done():
        return False

    company_names_list = list(company_names) if company_names else None

    async def _runner() -> None:
        try:
            await _publish_refresh_event(
                app,
                "refresh-started",
                trigger_source=trigger_source,
                company_names=company_names_list,
            )
            summary = await app.state.pipeline.refresh_news(
                company_names=company_names_list,
                trigger_source=trigger_source,
                limit=limit,
            )
            app.state.latest_refresh_summary = summary
            await _publish_refresh_event(
                app,
                "refresh-completed",
                trigger_source=trigger_source,
                company_names=company_names_list,
                summary=summary,
            )
        finally:
            app.state.refresh_task = None

    app.state.refresh_task = asyncio.create_task(_runner())
    return True


async def _scheduled_refresh_job(app: FastAPI) -> None:
    await _queue_refresh(app, company_names=None, trigger_source="scheduled")


async def _live_refresh_job(app: FastAPI) -> None:
    company_names = list_company_names(settings.database_path)
    batch, next_index = next_company_batch(
        company_names,
        getattr(app.state, "live_refresh_cursor", 0),
        settings.live_refresh_batch_size,
    )
    if not batch:
        return

    queued = await _queue_refresh(
        app,
        company_names=batch,
        trigger_source="live-cycle",
    )
    if queued:
        app.state.live_refresh_cursor = next_index


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings.ensure_directories()
    if not settings.company_sheet_path.exists():
        raise FileNotFoundError(
            f"Company sheet not found at {settings.company_sheet_path}. "
            "Update SPHERA_COMPANY_SHEET before starting the app."
        )

    init_db(settings.database_path)
    pipeline = NewsPipeline(settings)
    ingested_count = pipeline.bootstrap_companies()

    app.state.pipeline = pipeline
    app.state.ingested_count = ingested_count
    app.state.refresh_task = None
    app.state.latest_refresh_summary = None
    app.state.live_refresh_cursor = 0
    app.state.live_updates = LiveUpdateBroker()

    scheduler = None
    if settings.start_scheduler:
        scheduler = AsyncIOScheduler(timezone=settings.timezone)
        scheduler.add_job(
            _scheduled_refresh_job,
            trigger=CronTrigger(
                hour=",".join(str(hour) for hour in settings.refresh_hours),
                minute=0,
                timezone=settings.timezone,
            ),
            kwargs={"app": app},
            id="sphera-refresh-job",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        if settings.live_refresh_enabled:
            scheduler.add_job(
                _live_refresh_job,
                trigger=IntervalTrigger(
                    seconds=settings.live_refresh_interval_seconds,
                    timezone=settings.timezone,
                ),
                kwargs={"app": app},
                id="sphera-live-refresh-job",
                max_instances=1,
                coalesce=True,
                replace_existing=True,
            )
        scheduler.start()
    app.state.scheduler = scheduler

    if settings.run_refresh_on_start:
        await _queue_refresh(app, company_names=None, trigger_source="startup")

    yield

    refresh_task = getattr(app.state, "refresh_task", None)
    if refresh_task and not refresh_task.done():
        refresh_task.cancel()
        with suppress(asyncio.CancelledError):
            await refresh_task

    if scheduler:
        scheduler.shutdown(wait=False)


app = FastAPI(
    title="Sphera CredServ Signals",
    version="0.2.0",
    lifespan=lifespan,
)
app.mount("/static", StaticFiles(directory=str(settings.static_dir)), name="static")


@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    company: Optional[str] = Query(default=None),
    theme: Optional[str] = Query(default=None),
    signal: Optional[str] = Query(default=None),
    limit: int = Query(default=80, ge=1, le=250),
):
    snapshot = _build_snapshot_payload(
        app,
        company=company,
        theme=theme,
        signal=signal,
        limit=limit,
    )
    context = {
        "request": request,
        **snapshot,
        "theme_options": [{"slug": theme.slug, "label": theme.label} for theme in TRACKED_THEMES],
        "refresh_schedule": ", ".join(f"{hour:02d}:00" for hour in settings.refresh_hours),
        "timezone": settings.timezone,
        "company_sheet_path": str(settings.company_sheet_path),
        "database_path": str(settings.database_path),
        "live_refresh_interval_minutes": max(
            1, settings.live_refresh_interval_seconds // 60
        ),
    }
    return templates.TemplateResponse("index.html", context)


@app.get("/api/snapshot")
async def api_snapshot(
    company: Optional[str] = Query(default=None),
    theme: Optional[str] = Query(default=None),
    signal: Optional[str] = Query(default=None),
    limit: int = Query(default=80, ge=1, le=250),
):
    return _build_snapshot_payload(
        app,
        company=company,
        theme=theme,
        signal=signal,
        limit=limit,
    )


@app.get("/api/news")
async def api_news(
    company: Optional[str] = Query(default=None),
    theme: Optional[str] = Query(default=None),
    signal: Optional[str] = Query(default=None),
    limit: int = Query(default=80, ge=1, le=250),
):
    news_rows = list_news(
        settings.database_path,
        company_name=company,
        theme_slug=theme,
        signal=signal,
        limit=limit,
    )
    return {"items": [_serialize_news(row, settings) for row in news_rows]}


@app.get("/api/companies")
async def api_companies():
    return {"items": [_serialize_company(row, settings) for row in list_companies(settings.database_path)]}


@app.get("/api/status")
async def api_status():
    return _status_payload(app)


@app.get("/api/stream")
async def api_stream(request: Request):
    queue = await app.state.live_updates.subscribe()

    async def _event_stream():
        try:
            connected_payload = json.dumps(
                {
                    "status": "connected",
                    "timezone": settings.timezone,
                    "live_refresh_enabled": settings.live_refresh_enabled,
                    "live_refresh_interval_seconds": settings.live_refresh_interval_seconds,
                }
            )
            yield format_sse("connected", connected_payload)

            while True:
                if await request.is_disconnected():
                    break
                try:
                    event_name, payload = await asyncio.wait_for(
                        queue.get(),
                        timeout=settings.live_stream_ping_seconds,
                    )
                    yield format_sse(event_name, payload)
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        finally:
            await app.state.live_updates.unsubscribe(queue)

    return StreamingResponse(
        _event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/refresh")
async def api_refresh(
    company: Optional[str] = Query(default=None),
    limit: Optional[int] = Query(default=None, ge=1),
):
    company_names = {row["name"] for row in list_companies(settings.database_path)}
    if company and company not in company_names:
        raise HTTPException(status_code=404, detail=f"Company '{company}' is not tracked.")

    queued = await _queue_refresh(
        app,
        company_names=[company] if company else None,
        trigger_source="manual",
        limit=limit,
    )
    if not queued:
        return JSONResponse(
            status_code=409,
            content={"detail": "A refresh is already running. Please wait for it to finish."},
        )
    return {"status": "queued", "company": company, "limit": limit}


@app.get("/api/health")
async def api_health():
    metrics = get_dashboard_metrics(settings.database_path)
    return {
        "status": "ok",
        "company_count": metrics["company_count"],
        "news_count": metrics["news_count"],
        "database_path": str(settings.database_path),
        "company_sheet_path": str(settings.company_sheet_path),
        "live_refresh_enabled": settings.live_refresh_enabled,
        "live_refresh_interval_seconds": settings.live_refresh_interval_seconds,
    }
