# CEF News Feed (V1)

Local-first RSS aggregation platform for closed-end funds.

## Stack

- Backend: FastAPI + SQLAlchemy + Alembic
- Frontend: Next.js (App Router)
- DB: PostgreSQL
- HTTP: httpx (HTTP/2, connection pooling)
- Sources: Yahoo Finance, PRNewswire, GlobeNewswire, Business Wire

## Features

- Polls configured feeds continuously with a configurable per-cycle cooldown (`INGEST_COOLDOWN_SECONDS`)
- Normalizes RSS entries into a unified article model
- URL + title-window dedupe
- Tiered ticker extraction: context, exchange, paren, table cell, validated token, and token match types with confidence scoring
- Source page fallback extraction (fetches article HTML from BW/PRN/GNW when RSS metadata is insufficient)
- trafilatura-based article body extraction with regex fallback
- Per-symbol validation keywords (auto-generated from sponsor brand + fund name, with CSV override)
- Automated maintenance: deduplication, false-positive purge, source remap, stale mapping revalidation
- Real-time SSE delivery via PostgreSQL LISTEN/NOTIFY (auto-fallback to polling for SQLite)
- Adaptive polling: 30s without SSE, 120s when SSE connected
- Push notifications (Web Push / VAPID)
- Filterable API (`ticker`, `provider`, `source`, `q`, `from`, `to`) with cursor pagination
- Feed UI with provider/ticker/search filters and load-more pagination
- Docker: multi-stage builds, non-root containers, `.dockerignore`
- Schema migrations via Alembic with legacy database bootstrapping

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

The repo root `.env` is the single canonical env file for:

- local backend settings
- local frontend settings
- Docker Compose

There is no separate `backend/.env` runtime file anymore.

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
- `dev-backend.ps1` uses `backend/.venv`, reads config from the repo root `.env`, overrides the DB/ticker/CORS values needed for local dev, installs deps if key packages are missing (`uvicorn`, `alembic`, `httpx`), and creates the target DB if absent.
- `dev-backend.ps1` runs `python migrate.py` before starting the API so local schema stays aligned with the app.
- If a local database already has the pre-Alembic schema, the migration bootstrap stamps it at the baseline revision and then applies future migrations normally.
- `dev-frontend.ps1` uses `frontend/node_modules` and sets `NEXT_PUBLIC_API_BASE` to `http://127.0.0.1:8001`.
- If `ADMIN_API_KEY` is blank in `.env`, `dev-backend.ps1` generates one automatically for that local run.
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

This starts only `db`, `backend`, and `frontend`. `NEXT_PUBLIC_API_BASE` is a build-time input for the frontend image. The provided `.env.example` already sets the local Docker value (`http://127.0.0.1:8001`).

The published compose ports are bound to `127.0.0.1` by default so they are only reachable from the local machine. That keeps the stack usable for development without exposing Postgres, the API, the UI, or monitoring services on the network interface.

Before using this stack for anything beyond a single local machine, override these values in `.env`:

- `POSTGRES_PASSWORD`
- `GRAFANA_PASSWORD`
- `ADMIN_API_KEY` if you use admin endpoints
- any `*_PORT` values if you need different host ports

If you intentionally want network exposure, remove the `127.0.0.1:` prefix from the relevant published ports and place the stack behind your own firewall or reverse proxy.

If the backend is behind a reverse proxy and you need forwarded client IPs for SSE connection limiting, set `BEHIND_PROXY=true` and set `TRUSTED_PROXY_IPS` to the proxy IPs or CIDRs that are allowed to supply `X-Forwarded-For` / `X-Real-IP`. Do not enable forwarded headers without a trusted proxy allowlist.

3. Open:

- UI: http://127.0.0.1:3005
- API docs: http://127.0.0.1:8001/docs

Default frontend host port is `3005` (`FRONTEND_PORT` in `.env`).

## Monitoring UI (Prometheus + Grafana)

This repo now includes a lightweight monitoring stack:

- Prometheus: scrape, query metrics, and evaluate local alert rules
- Grafana: dashboard UI
- Grafana alerting is disabled in `docker-compose.yml` (`GF_UNIFIED_ALERTING_ENABLED=false`)
- Alert notifications are not routed anywhere yet because Alertmanager is not configured

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
  - login uses `GRAFANA_USER` / `GRAFANA_PASSWORD` from `.env` when set
  - the example file ships with a placeholder password, so replace it before sharing or deploying the stack

Grafana auto-loads dashboard:

- `CEF / CEF Ingestion Overview`

### 4) What you can see

- ingestion cycle duration (`p50` / `p95`)
- ingestion outcomes (`success`, `skipped_*`, `failed`)
- last cycle summary (duration, inserted, failed feeds, timestamp)
- API route latency (`http_request_duration_seconds`)
- latest individual feed runs table (`source`, `feed_url`, start/end, status, seen/inserted, error)
- alert states in Prometheus for stalled ingestion, repeated ingestion failures, zero recent inserts, and high API 5xx rate

### Notes

- Default `docker compose up --build` does not start monitoring services.
- Default scrape target is `host.docker.internal:8001`.
- If backend is unavailable at those ports, Grafana panels will be empty until a target responds.
- Prometheus alert rules are loaded from `monitoring/prometheus/alerts.yml` and can be inspected in the Prometheus UI under `Alerts`.
- The table panel uses a provisioned Postgres datasource (`db:5432`, `cef/cef`, `cef_news`).

## Manual Local Dev (Without Helper Scripts)

### Backend

```powershell
cd backend
python -m venv .venv
. .venv/Scripts/activate  # Windows PowerShell
pip install -r requirements.txt -r requirements-dev.txt
python migrate.py
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
- `GET /api/v1/events/news` — SSE stream for real-time article notifications

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
For local `.\dev-backend.ps1` runs, leaving `ADMIN_API_KEY` blank is fine because the script generates one automatically.
For Docker/manual runs, set a stable value in `.env` if you need admin endpoints.

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
curl -X POST "http://localhost:8001/api/v1/admin/tickers/reload" \
  -H "X-API-Key: ${ADMIN_API_KEY}"
```

This reloads symbols and remaps recent unmapped page-fetch sources, including `businesswire`, so stories move out of `GENERAL` when a ticker match is found.

Ingestion reliability controls are configurable via env vars:

- `INGEST_COOLDOWN_SECONDS` waits this many seconds after each ingestion cycle before the next one starts.
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

## Architecture Notes

- V1 includes the public Business Wire home RSS feed.
- If a feed fails intermittently, status is tracked in `ingestion_runs`.
- Ingestion uses a background loop with `INGEST_COOLDOWN_SECONDS` sleep after each completed cycle.
- SSE uses PostgreSQL `LISTEN/NOTIFY` — each per-feed commit fires `pg_notify('new_articles', count)`. The `SSEBroadcaster` runs a dedicated LISTEN thread and fans out to connected clients via `asyncio.Queue`. When the broadcaster is unavailable (SQLite or Postgres down), the frontend falls back to 30s polling.
- The backend uses a shared `httpx.Client` for HTTP/2 and connection pooling across feed fetches and source page lookups.
- Docker images use multi-stage builds (separate builder/runtime stages) and run as non-root `appuser`.
- Schema migrations run via `python migrate.py` (or automatically during app startup). Legacy pre-Alembic databases are detected and stamped at the baseline revision so future migrations apply normally.
