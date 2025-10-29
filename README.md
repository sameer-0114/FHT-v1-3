# FH-Terminal v1.3 (Realtime Order Flow)

A FastAPI + Lightweight Charts web app that renders real-time price bars, a DOM (order book) overlay behind candles, a footprint heatmap with POC and imbalances, and crosshair-driven info.

## Features
- Realtime bars via WebSocket, initial snapshots via REST
- DOM (Level 2) overlay rendered behind candles
- Footprint columns from PostgreSQL (price_levels), with crisp text and POC highlighting
- Imbalances (same-level/diagonal) in yellow/orange with strength shading
- Mobile-friendly UI with theme toggle

## Structure
- `server_db.py` — FastAPI app serving API, WebSockets, and static web UI
- `db_async.py` — Async PostgreSQL data access (bars, footprint)
- `XMR_L2.py` — Binance L2 live order book reader used for DOM stream
- `web/index.html` — Frontend (Lightweight Charts + custom DOM/footprint canvases)
- `sql/notify_triggers.sql` — Optional LISTEN/NOTIFY trigger helpers
- `requirements.txt` — Python dependencies
- `.env.example` — Environment variable template
- `runtime.txt` — Pin Python runtime version (for platforms that use it)

## Requirements
- Python 3.11 (managed via `.python-version`)
- PostgreSQL with the `timeframe_data` table containing OHLCV, `price_levels` (JSON), and `imbalances` (JSON)

## Quickstart (Local)
1. Copy `.env.example` to `.env` and fill DB connection values.
2. Install dependencies:
   ```sh
   python -m pip install -r requirements.txt
   ```
3. Run the server:
   ```sh
   python -m uvicorn server_db:app --host 127.0.0.1 --port 8000
   ```
4. Open the app at http://127.0.0.1:8000/ (root serves `web/index.html`).

## Environment Variables
- `PG_DSN` — Full Postgres URI (preferred) e.g. `postgres://doadmin:***@host:25060/defaultdb?sslmode=require`
- or the parts (used if `PG_DSN` is not set): `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`, `PGSSLMODE`
- `APP_SYMBOL` — Symbol filter (default `xmrusdt`)
- `APP_TABLE` — Table name (default `timeframe_data`)

## Deploying to DigitalOcean App Platform (summary)
- Push this repo to GitHub.
- Create a new App in DO, link the repo, choose Python.
- Set the run command: `uvicorn server_db:app --host 0.0.0.0 --port $PORT` (App Platform injects `$PORT`).
- Add your Managed PostgreSQL as a resource and expose env vars (or set `PG_DSN`).
- Scale to 1+ instance, deploy.

Notes:
- App Platform prefers `.python-version` over `runtime.txt`. This repo includes `.python-version` with `3.11` so the latest secure patch is used.

See comments in `.env.example` for a Managed PG DSN template.

## Notes
- The UI expects `/api/bars`, `/ws/bars`, `/api/footprint`, and `/ws/dom` to be available from `server_db.py`.
- If you don’t need the older desktop/Qt GUI files, omit them from the repo.
