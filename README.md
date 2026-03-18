# CEF News Feed (V1)

Local-first RSS aggregation platform for closed-end funds.

## Stack

- Backend: FastAPI + SQLAlchemy + APScheduler
- Frontend: Next.js (App Router)
- DB: PostgreSQL
- Sources: Yahoo Finance, PRNewswire, GlobeNewswire, Business Wire

## Features

- Polls configured feeds every minute (configurable)
- Normalizes RSS entries into a unified article model
- URL + title-window dedupe
- Tiered ticker extraction: context, exchange, paren, table cell, validated token, and token match types with confidence scoring
- Source page fallback extraction (fetches article HTML from BW/PRN/GNW when RSS metadata is insufficient)
- trafilatura-based article body extraction with regex fallback
- Per-symbol validation keywords (auto-generated from sponsor brand + fund name, with CSV override)
- Automated maintenance: deduplication, false-positive purge, source remap, stale mapping revalidation
- Push notifications (Web Push / VAPID)
- Filterable API (`ticker`, `provider`, `source`, `q`, `from`, `to`) with cursor pagination
- Feed UI with provider/ticker/search filters and load-more pagination

## Project Layout

- `backend/` FastAPI service and ingestion engine
- `frontend/` Next.js UI
- `data/cef_tickers.csv` ticker universe (edit this for your full 350 list)
- `docker-compose.yml` local multi-service runtime

## Recommended Fast Dev Loop (Docker DB + Local App)

This is the primary workflow for local development:

- Keep PostgreSQL in Docker.
- Run backend and frontend locally with hot reload.
- Avoid container rebuilds/restarts for every app edit.

### 1) Copy environment template

```bash
cp .env.example .env
```

### 2) Start DB only

```powershell
.\dev-db.ps1 -Wait
```

### 3) Start backend locally

```powershell
.\dev-backend.ps1
```

### 4) Start frontend locally

```powershell
.\dev-frontend.ps1
```

### 5) Open app

- UI: http://127.0.0.1:3005
- API docs: http://127.0.0.1:8001/docs

Script notes:

- `dev-db.ps1` starts only the `db` service (`docker compose up -d db`).
- `dev-backend.ps1` uses `backend/.venv`, sets local env defaults (`localhost:5433` database, local ticker CSV), installs deps if needed, and creates the target DB if absent.
- `dev-frontend.ps1` uses `frontend/node_modules` and sets `NEXT_PUBLIC_API_BASE` to `http://127.0.0.1:8001`.
- You can override ports/host with script params (for example `.\dev-frontend.ps1 -Port 3010`).

## Secondary Path: Full Docker App Startup

1. Copy environment template:

```bash
cp .env.example .env
```

2. Start the core stack:

```bash
docker compose up --build
```

This starts only `db`, `backend`, and `frontend`. `NEXT_PUBLIC_API_BASE` is a build-time input for the frontend image. The provided `.env.example` already sets the local Docker value (`http://localhost:8000`).

3. Open:

- UI: http://localhost:3005
- API docs: http://localhost:8000/docs

Default frontend host port is `3005` (`FRONTEND_PORT` in `.env`).

## Monitoring UI (Prometheus + Grafana)

This repo now includes a lightweight monitoring stack (no alerting):

- Prometheus: scrape + query metrics
- Grafana: dashboard UI
- Grafana alerting is disabled in `docker-compose.yml` (`GF_UNIFIED_ALERTING_ENABLED=false`)

### 1) Run backend with metrics exposed

If you run backend locally, bind to `0.0.0.0` so Dockerized Prometheus can scrape it:

```powershell
.\dev-backend.ps1 -BindHost 0.0.0.0 -Port 8001
```

Confirm metrics endpoint:

- http://127.0.0.1:8001/metrics

### 2) Start monitoring services

```powershell
docker compose --profile monitoring up --build
```

### 3) Open dashboards

- Prometheus: http://127.0.0.1:9090
- Grafana: http://127.0.0.1:3006
  - default login: `admin` / `admin`

Grafana auto-loads dashboard:

- `CEF / CEF Ingestion Overview`

### 4) What you can see

- ingestion cycle duration (`p50` / `p95`)
- ingestion outcomes (`success`, `skipped_*`, `failed`)
- last cycle summary (duration, inserted, failed feeds, timestamp)
- API route latency (`http_request_duration_seconds`)
- latest individual feed runs table (`source`, `feed_url`, start/end, status, seen/inserted, error)

### Notes

- Default `docker compose up --build` does not start monitoring services.
- Default scrape targets are `host.docker.internal:8001` and `host.docker.internal:8000`.
- If backend is unavailable at those ports, Grafana panels will be empty until a target responds.
- The table panel uses a provisioned Postgres datasource (`db:5432`, `cef/cef`, `cef_news`).

## Manual Local Dev (Without Helper Scripts)

### Backend

```powershell
cd backend
python -m venv .venv
. .venv/Scripts/activate  # Windows PowerShell
pip install -r requirements.txt -r requirements-dev.txt
uvicorn app.main:app --reload --host 127.0.0.1 --port 8001
```

### Frontend

```powershell
cd frontend
npm install
npm run dev
```

Set `NEXT_PUBLIC_API_BASE` if backend is on a non-default host/port.

## Troubleshooting

### Next.js lock error (`Unable to acquire lock ... .next/dev/lock`)

If `dev-frontend.ps1` fails with a lock error, clear the stale lock file:

```powershell
Remove-Item -Force .\frontend\.next\dev\lock
.\dev-frontend.ps1
```

If needed, clear the whole Next dev cache:

```powershell
cmd /c rmdir /s /q frontend\.next\dev
.\dev-frontend.ps1
```

### Port conflicts with other projects

If you already use frontend `3000` or backend `800`/`8000` in another repo, run this project on custom ports:

```powershell
.\dev-backend.ps1 -Port 8001
.\dev-frontend.ps1 -Port 3005 -ApiBase http://127.0.0.1:8001
```

If another project is using PostgreSQL `5432`, this repo intentionally uses `5433` for local DB access.

For frontend on `3000` instead:

```powershell
.\dev-frontend.ps1 -Port 3000 -ApiBase http://127.0.0.1:8001
```

## API Endpoints

### Public

- `GET /health`
- `GET /api/v1/tickers` — list active tickers (`?q=` prefix search)
- `GET /api/v1/news` — list articles (filters: `ticker`, `provider`, `source`, `q`, `from`, `to`)
- `GET /api/v1/news/ids` — cursor-paginated article IDs (`limit`, `cursor`)
- `GET /api/v1/news/{id}` — single article detail

### Push Notifications

- `GET /api/v1/push/vapid-key` — VAPID public key (or `enabled: false`)
- `PUT /api/v1/push/subscription` — create/update push subscription
- `DELETE /api/v1/push/subscription` — remove push subscription

### Admin (require `X-API-Key` header)

- `POST /api/v1/admin/ingest/run-once` — trigger one ingestion cycle
- `GET /api/v1/admin/ingest/status` — recent ingestion run history
- `POST /api/v1/admin/tickers/reload` — reload CSV + auto-remap unmapped articles
- `POST /api/v1/admin/remap/{source_code}` — re-extract tickers for a source (`businesswire`, `prnewswire`, `globenewswire`)
- `POST /api/v1/admin/revalidate` — revalidate stale ArticleTicker rows against current extraction logic
- `POST /api/v1/admin/purge/false-positives` — remove token-only false-positive mappings (`?dry_run=true` default)
- `POST /api/v1/admin/dedupe/businesswire-url-variants` — merge BW URL variant duplicates
- `POST /api/v1/admin/dedupe/title` — merge title-based duplicates
- `POST /api/v1/news/alerts/sent` — mark articles as alerted (internal)

`GET /api/v1/news` defaults to mapped CEF-linked articles only.
Mapped means linked to at least one active ticker.
Use `include_unmapped=true` to include stories with no active ticker mapping.
Use `include_unmapped_from_provider=Business%20Wire` to include only Business Wire stories with no active ticker mapping while keeping active-ticker-mapped stories from all providers.
`GET /api/v1/news/ids` supports cursor pagination with `limit` and `cursor`.
`GET /api/v1/tickers?q=` does case-insensitive prefix matching on the ticker symbol.

Admin endpoints require the `X-API-Key` header and `ADMIN_API_KEY` to be configured.

Push endpoints require Web Push configuration in `.env`:

- `VAPID_PUBLIC_KEY`
- `VAPID_PRIVATE_KEY`
- `VAPID_CONTACT_EMAIL`

Generate keys with:

```bash
python -m app.vapid_keygen
```

After editing `data/cef_tickers.csv` while the backend is running, call:

```bash
curl -X POST "http://localhost:8000/api/v1/admin/tickers/reload" \
  -H "X-API-Key: ${ADMIN_API_KEY}"
```

This reloads symbols and remaps recent unmapped page-fetch sources, including `businesswire`, so stories move out of `GENERAL` when a ticker match is found.

Ingestion reliability controls are configurable via env vars:

- `INGESTION_STALE_RUN_TIMEOUT_SECONDS` marks stale `running` jobs as failed.
- `FEED_FETCH_MAX_ATTEMPTS` / `FEED_FETCH_BACKOFF_SECONDS` / `FEED_FETCH_BACKOFF_JITTER_SECONDS` control feed retry behavior.
- `RAW_FEED_RETENTION_DAYS` / `RAW_FEED_PRUNE_BATCH_SIZE` / `RAW_FEED_PRUNE_MAX_BATCHES` bound raw feed table growth.

## Ticker Universe

Edit `data/cef_tickers.csv` with your full list:

```csv
ticker,fund_name,sponsor,active,validation_keywords
GOF,Guggenheim Strategic Opportunities Fund,Guggenheim,true,
FFA,First Trust Enhanced Equity Income Fund,First Trust,true,first trust
DNP,Duff & Phelps Utility and Corporate Bond Trust,Duff & Phelps,true,"duff,phelps"
...
```

The `validation_keywords` column is optional. When set, it overrides auto-generated keywords for that symbol. Comma-separated values are treated as independent keywords (any match validates). Leave blank to use the default logic (sponsor brand + fund name words, with common fund-industry stopwords removed).

## Notes

- V1 includes the public Business Wire home RSS feed.
- If a feed fails intermittently, status is tracked in `ingestion_runs`.
- Default polling interval is 60 seconds.
