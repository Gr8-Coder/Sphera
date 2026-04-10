# Sphera CredServ Signals

A lightweight news-monitoring tool for tracking CredServ-relevant signals across the companies listed in the bundled watchlist at `/Users/sajaltyagi/Documents/Sphera/data/companies.csv`.

The app does four things:

1. Ingests the company watchlist from the Excel file into SQLite.
2. Fetches Google News RSS snippets for each company.
3. Classifies snippets into these five focus areas:
   - Distributor expansion
   - Dealer financing gaps
   - Working capital commentary
   - Channel scale-up
   - Rural/secondary distribution growth
4. Labels each snippet as `positive` or `negative` for CredServ propensity.

It now also supports a live portal experience:

1. A rotating background refresh loop checks a small batch of companies every few minutes.
2. The browser listens on a live event stream.
3. New snippets, counts, and status updates appear in the portal without a page reload.

`Positive` here means the news increases the likelihood that a CredServ financing or channel-support use case is relevant. `Negative` means the signal points to lower or reducing need.

## Stack

- FastAPI for the web app and JSON API
- SQLite for local persistence
- APScheduler for 5-times-per-day refresh scheduling
- Pandas + OpenPyXL for Excel ingestion
- Google News RSS for the MVP news source

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000).

The default refresh schedule is `08:00`, `11:00`, `14:00`, `17:00`, and `20:00` in `Asia/Kolkata`. You can change that with `SPHERA_REFRESH_HOURS`.

The live portal loop is enabled by default and checks the next batch of companies every 5 minutes.

## Commands

Load the watchlist:

```bash
python -m app.tasks ingest
```

Refresh all tracked companies:

```bash
python -m app.tasks refresh
```

Refresh a single company:

```bash
python -m app.tasks refresh --company "3M India"
```

Dry-run a smaller batch during testing:

```bash
python -m app.tasks refresh --limit 25
```

## Environment variables

- `SPHERA_COMPANY_SHEET`: absolute path to the Excel source file
- Default source in this repo: `data/companies.csv`
- `SPHERA_DB_PATH`: where the SQLite database should live
- `SPHERA_TIMEZONE`: scheduler timezone
- `SPHERA_REFRESH_HOURS`: comma-separated hours for the 5 daily refreshes
- `SPHERA_LOOKBACK_DAYS`: Google News RSS lookback window
- `SPHERA_RSS_MAX_RESULTS`: maximum RSS items to inspect per company per refresh
- `SPHERA_REFRESH_CONCURRENCY`: concurrent company refreshes
- `SPHERA_START_SCHEDULER`: enable or disable APScheduler inside the web app
- `SPHERA_RUN_REFRESH_ON_START`: optionally trigger a refresh on app startup
- `SPHERA_LIVE_REFRESH_ENABLED`: enable the rotating live-refresh loop
- `SPHERA_LIVE_REFRESH_INTERVAL_SECONDS`: cadence for the live background refresh
- `SPHERA_LIVE_REFRESH_BATCH_SIZE`: number of companies checked in each live batch
- `SPHERA_LIVE_STREAM_PING_SECONDS`: heartbeat interval for the server-sent event stream

## Notes

- Google News RSS is a practical MVP source, but for a larger production watchlist you may eventually want a commercial news API with stronger coverage and batch querying.
- The classifier is rule-based so it is easy to inspect and tune. If you want, the next step can be replacing or augmenting it with an LLM-based classifier for better precision.
- The portal is "live" by polling Google News in the background and streaming updates into the browser. It is near-real-time, not a direct publisher push feed.
- The repo includes a `Dockerfile` and `render.yaml`, so it is ready for container-based deployment on services like Render.
