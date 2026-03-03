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
- Ticker mapping from context, exchange patterns, and token match
- Filterable API (`ticker`, `provider`, `source`, `q`, `from`, `to`) with cursor pagination
- Feed UI with provider/ticker/search filters and load-more pagination

## Project Layout

- `backend/` FastAPI service and ingestion engine
- `frontend/` Next.js UI
- `data/cef_tickers.csv` ticker universe (edit this for your full 350 list)
- `docker-compose.yml` local multi-service runtime

## Quick Start (Docker)

1. Copy environment template:

```bash
cp .env.example .env
```

2. Start stack:

```bash
docker compose up --build
```

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
docker compose up -d prometheus grafana
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

### Notes

- Default scrape targets are `host.docker.internal:8001` and `host.docker.internal:8000`.
- If backend is unavailable at those ports, Grafana panels will be empty until a target responds.

## Recommended Fast Dev Loop (Docker DB + Local App)

This is the fastest workflow when you are making lots of code changes:

- Keep only PostgreSQL in Docker.
- Run backend and frontend locally with hot reload.
- Avoid container rebuilds/restarts for every app edit.

### 1) Start DB only

```powershell
.\dev-db.ps1 -Wait
```

### 2) Start backend locally

```powershell
.\dev-backend.ps1
```

### 3) Start frontend locally

```powershell
.\dev-frontend.ps1
```

### 4) Open app

- UI: http://127.0.0.1:3005
- API docs: http://127.0.0.1:8001/docs

Script notes:

- `dev-db.ps1` starts only the `db` service (`docker compose up -d db`).
- `dev-backend.ps1` sets local env defaults (`localhost:5433` database, local ticker CSV).
- `dev-frontend.ps1` sets `NEXT_PUBLIC_API_BASE` to `http://127.0.0.1:8001`.
- `dev-backend.ps1` auto-creates/fixes `.venv`, installs deps if `uvicorn` is missing, and creates the target DB if absent.
- You can override ports/host with script params (for example `.\dev-frontend.ps1 -Port 3010`).

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

- `GET /health`
- `GET /api/v1/tickers`
- `GET /api/v1/news`
- `GET /api/v1/news/{id}`
- `POST /api/v1/admin/ingest/run-once`
- `GET /api/v1/admin/ingest/status`
- `POST /api/v1/admin/tickers/reload`
- `POST /api/v1/admin/remap/businesswire`

`GET /api/v1/news` defaults to mapped CEF-linked articles only.
Use `include_unmapped=true` to include all unmapped stories.
Use `include_unmapped_from_provider=Business%20Wire` to include only Business Wire unmapped stories while keeping mapped stories from all providers.
`GET /api/v1/news/ids` supports cursor pagination with `limit` and `cursor`.

Admin endpoints require the `X-API-Key` header and `ADMIN_API_KEY` to be configured.

After editing `data/cef_tickers.csv` while the backend is running, call:

```bash
curl -X POST "http://localhost:8000/api/v1/admin/tickers/reload" \
  -H "X-API-Key: ${ADMIN_API_KEY}"
```

This reloads symbols and remaps recent unmapped Business Wire items so they move out of `GENERAL` when a ticker match is found.

Ingestion reliability controls are configurable via env vars:

- `INGESTION_STALE_RUN_TIMEOUT_SECONDS` marks stale `running` jobs as failed.
- `FEED_FETCH_MAX_ATTEMPTS` / `FEED_FETCH_BACKOFF_SECONDS` / `FEED_FETCH_BACKOFF_JITTER_SECONDS` control feed retry behavior.
- `RAW_FEED_RETENTION_DAYS` / `RAW_FEED_PRUNE_BATCH_SIZE` / `RAW_FEED_PRUNE_MAX_BATCHES` bound raw feed table growth.

## Ticker Universe

Edit `data/cef_tickers.csv` with your full list:

```csv
ticker,fund_name,sponsor,active
GOF,Guggenheim Strategic Opportunities Fund,Guggenheim,true
...
```

## Notes

- V1 includes the public Business Wire home RSS feed.
- If a feed fails intermittently, status is tracked in `ingestion_runs`.
- Default polling interval is 60 seconds.
