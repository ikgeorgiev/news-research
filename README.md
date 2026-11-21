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

## Running the tests

With the backend requirements and development requirements installed, run
`python -m pytest -q tests` from `backend/` and `python -m pytest -q tools/tests`
from the repository root. Recorded feeds and publisher fixtures live in
`backend/tests/fixtures/`. From `frontend/`, run `npm ci`, `npm test`, and
`npm run build`.

For the PostgreSQL integration tests, use a disposable PostgreSQL 16 database
named `cef_study_disposable` owned by `cef_test` on `127.0.0.1`. Set both
`DATABASE_URL` and `CEF_DISPOSABLE_TEST_URL` to its SQLAlchemy
`postgresql+psycopg://` URL. From `backend/`, run `python migrate.py`, then run
the PostgreSQL test selection in `.github/workflows/ci.yml`.

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
- `dev-backend.ps1` uses `backend/.venv`, resolves DB credentials/name and admin settings from the repo root `.env`, applies its local `-DbHost` / `-DbPort` parameters, overrides the ticker/CORS values needed for local dev, synchronizes exact dependency pins when the environment is missing or stale, and creates the target DB if absent.
- `dev-backend.ps1` runs `python migrate.py` before starting the API so local schema stays aligned with the app.
- If a local database already has the pre-Alembic schema, the migration bootstrap stamps it at the baseline revision and then applies future migrations normally.
- `dev-frontend.ps1` synchronizes missing or stale `frontend/node_modules` with `npm ci` and sets `NEXT_PUBLIC_API_BASE` to `http://127.0.0.1:8001`.
- `clean-local.ps1` removes generated caches and build output while preserving installed dependencies. Pass `-Dependencies` for a full local dependency reset.
- If `ADMIN_API_KEY` is blank in `.env`, `dev-backend.ps1` generates an undisclosed temporary key so admin routes stay protected. Set an explicit value before starting when you need to call those routes.
- You can override ports/host with script params (for example `.\dev-frontend.ps1 -Port 3010`).

## Secondary Path: Full Docker App Startup

1. Copy environment template:

```bash
cp .env.example .env
```

2. Start the core stack:

```bash
mkdir -p data/article-bodies
docker compose up --build
```

The backend stores its SQLite files and reader archives in the checkout's `data/` directory. The frontend mounts only `data/article-bodies/` read-only. Both containers use UID 1000 so they can share existing private archive files. On Linux, give UID 1000 write access to the state directory and ownership of private archives. Run one ingestion backend against this data at a time.

When upgrading from an older Compose backend with container-local state, stop that backend and copy its `/app/data` into a separate backup before recreating it. Reconcile the saved SQLite files and article bodies with the checkout before starting the upgraded stack.

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

With the backend already running through `dev-backend.ps1`, start only monitoring:

```powershell
docker compose --profile monitoring up -d --no-deps prometheus grafana
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
- new article commits and receipt-to-commit timing by source, with pending-enrichment replays separated from fresh feed responses
- article processing, database commit and server notification-call stages; page lookup is included within processing, so these stages must not all be added together
- actual source-page request durations and HTTP outcomes, including 403 responses; cache hits do not count as network requests
- alert states in Prometheus for stalled ingestion, repeated ingestion failures, zero recent inserts, and high API 5xx rate

### Notes

- Default `docker compose up --build` does not start monitoring services.
- Default scrape target is `host.docker.internal:8001`.
- If backend is unavailable at those ports, Grafana panels will be empty until a target responds.
- Prometheus alert rules are loaded from `monitoring/prometheus/alerts.yml` and can be inspected in the Prometheus UI under `Alerts`.
- The table panel uses a provisioned Postgres datasource (`db:5432`) with the configured `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB`.
- These metrics measure application work, not original publication-to-discovery or browser rendering. Source first sightings remain in the bounded research probe. A notification call is not proof of UI delivery.
- For capped page lookups with at least two slots, due retries leave one slot for unresolved current-feed items. The total request cap stays the same; a 304 with no current-feed items can use the full cap for retries. A one-slot configuration still gives due retries priority.
- New article metrics start when this backend version starts; there is no historical backfill. Quantiles are bucket estimates and need sample counts; no observations means no estimate. Prometheus counters reset on process restart and a restart between scrapes can lose unsampled work.

## Manual Local Dev (Without Helper Scripts)

### Backend

```powershell
cd backend
python -m venv .venv
. .venv/Scripts/activate  # Windows PowerShell
pip install -r requirements.txt -r requirements-dev.txt
python migrate.py
uvicorn app.main:app --reload --host 127.0.0.1 --port 8001 --timeout-graceful-shutdown 5
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
For local `.\dev-backend.ps1` runs, leaving `ADMIN_API_KEY` blank is fine when you do not need admin endpoints; the script generates an undisclosed temporary key.
Set an explicit value in `.env` before starting when you need to call admin endpoints.
For Docker/manual runs, set a stable value in `.env` if you need admin endpoints.

Push endpoints require Web Push configuration in `.env`:

- `VAPID_PUBLIC_KEY`
- `VAPID_PRIVATE_KEY`
- `VAPID_CONTACT_EMAIL`

For SSRF protection, subscriptions are accepted only for the standard Google,
Mozilla, Apple, and Windows browser push-service domains.

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
- SSE uses PostgreSQL `LISTEN/NOTIFY` — each newly inserted article is committed with its ticker mappings and raw record, then fires `pg_notify('new_articles', 1)`. The `SSEBroadcaster` runs a dedicated LISTEN thread and fans out to connected clients via `asyncio.Queue`. When the broadcaster is unavailable (SQLite or Postgres down), the frontend falls back to 30s polling.
- The backend uses a shared `httpx.Client` for HTTP/2 and connection pooling across feed fetches and source page lookups.
- Parallel ingestion runs each serialized source in one worker job, so other workers do not wait on its source lock. Optional Yahoo concurrency remains configurable. Entry transactions release deduplication locks before processing the next article, avoiding deadlocks between feeds that list the same articles in different orders. For sources with page fallback, entries with sufficient ticker evidence in the feed are processed first, preserving order within each group and retaining the normal validation and deduplication checks. Validated articles become available before page lookups for other entries finish. A later database failure retains earlier commits and reports their insert count; HTTP cache validators advance only after processing finishes, allowing the remaining entries to be retried.
- Each newly committed article emits an `article_latency` JSON log with its URL, UTC feed-receipt and commit timestamps, request time (including retries), time waiting before processing, processing time, page-lookup time, commit time and notification-call time. Page lookup is included in processing time, so do not add it again. Feed receipt is the completed HTTP response, not original publication or first upstream availability; notification-call timing does not measure SSE transport or browser rendering. Duplicate entries and failed commits do not emit a new-article timing record.
- Each actual enrichment HTTP request emits `source_page_latency`, including requests for entries that never become articles. It records the source, page URL, completion time, duration, configured timeout, HTTP status or request-error type, and whether a nonempty HTML response was received. Cache hits do not emit a network timing record; a nonempty response does not establish valid content or ticker accuracy.
- Successful article-body extraction is reused for identical HTML, with at most 128 results and 16 MiB of digest/body payloads retained. Changed HTML is reparsed; failed extraction remains retryable. Ticker matching still runs against the current fund universe and validation keywords on every call.
- Page fallback also handles exchange-qualified tickers embedded in a table's fund-name cell. Fund names in the same row remain validation evidence when readable-body extraction omits the table; existing confidence thresholds and stricter common-word/acronym rules still apply.
- Accepted wire articles with validated feed tickers now queue a separate full-page coverage check when no page lookup was needed during ingestion. The headline, initial mappings, and queue entry commit together. A dedicated worker checks up to three jobs, then waits five seconds, without occupying a feed worker or retaining database locks during HTTP requests. It uses current ticker validation after the fetch and adds missing mappings without removing existing ones. Short RSS replays preserve this coverage even after raw-feed cleanup.
- This coverage queue requires migration `20261027_0003`. It keeps one job per article independently of raw-feed retention; completed jobs prevent duplicate work and are removed with the article. Only recognized wire URLs on accepted articles published within the past two days are eligible; existing attached records are not bulk backfilled. Failed lookups back off from one minute to one hour, with at most 12 attempts or one day of queued work. Two-minute leases recover interrupted workers and reject stale results. Disabled sources are skipped. `POSTCOMMIT_ENRICHMENT_ENABLED=false` stops enqueueing and the background worker; `SCHEDULER_ENABLED=false` also prevents the worker from starting.
- An added mapping commits before emitting a zero-count `new_articles` refresh event. Failed notification delivery remains queued without repeating the page fetch. `article_enrichment_latency` separates queue age, page lookup (which can hit cache), validation, commit, and notification-call durations; these do not measure live browser delivery. The worker reuses the configured ingestion page timeout, capped at ten seconds per HTTP timeout phase. Expired work is logged and retained for inspection; this is one coverage check per article, not continuous monitoring of later publisher edits.
- Unavailable article pages and lookups deferred by the page budget retain retry state in the saved raw item. Up to three due items per feed are reconsidered even on HTTP 304 or after they leave the feed, using current ticker validation and normal deduplication. Failed lookups back off from one minute to one hour; successfully parsed pages with no validated fund are not queued. Pending data follows the existing raw-item retention policy.
- Business Wire's strict finance feed now permits three uncached page lookups per poll; remaining work is saved for later. The general-news feed retains its existing behavior so already-persisted general articles do not lose ticker enrichment. Feed-validated headlines still go first, followed by due retries before fresh page-dependent work. No additional service or database migration is required. Older raw rows without retry metadata are classified when their feed returns them again; they are not bulk reclassified. `article_latency.discovery_kind` distinguishes pending enrichment from current feed items; for a retry, `feed_received_at` measures the triggering poll and `enrichment_queued_at` records when pending work was saved, not original publisher availability.
- Docker images use multi-stage builds (separate builder/runtime stages) and run as non-root `appuser`.
- Schema migrations run via `python migrate.py` (or automatically during app startup). Legacy pre-Alembic databases are detected and stamped at the baseline revision so future migrations apply normally.

## Latency research

Run a bounded, read-only comparison from `backend/` without starting the app or touching its database:

```powershell
.\.venv\Scripts\python.exe -m scripts.news_latency_probe --duration-minutes 30 --interval-seconds 30
```

The probe compares the published GlobeNewswire RSS and sitemap routes, PR Newswire financial RSS, and Business Wire category RSS. The larger Business Wire all-news feed is opt-in with `--routes bw-all-rss`. Select routes with `--routes`; use `--help` for names. Each run creates a new directory under `.tmp/` with `state.json` and append-only `observations.jsonl`. Existing output directories are rejected to preserve evidence.

Additional opt-in routes include `prn-mutual-funds-rss`, `prn-dividends-rss`, and the public issuer collection `tortoise-tyg-json`. The issuer collection uses stable post IDs and `date_gmt` publication metadata; edits do not become new arrivals. TYG taxonomy membership is a discovery filter, not ticker validation. RSS summaries and issuer post content are retained for later validation. Cross-provider titles form candidate matches requiring review, not automatic deduplication. Runs are bounded to at most 13 hours.

It records request times, cache headers, first sightings, and the last successful response that did not contain each new release. Initial snapshots are excluded from matched-arrival comparisons. Different source filters and rolling windows mean counts alone do not measure completeness. Publisher timestamps and cache lifetimes do not prove publication-to-delivery delay.

For a bounded local field study, set both `RESEARCH_TIMING_LOG_PATH` and `RESEARCH_TIMING_LOG_UNTIL` (an ISO timestamp with timezone) before starting the backend. This optionally copies existing `article_latency`, `article_enrichment_latency`, and `source_page_latency` diagnostics into JSON lines with the emitting process ID. Collection stops at the deadline and rotates at 8 MiB with three backups; it adds no network observer and does not measure the browser. Both settings are unset by default, and console logging is preserved.

The probe uses conditional requests, backs off on failures and rate limits, and stops a route after HTTP 401, 403, 404, or a response exceeding 4 MiB. It does not fetch article pages or send application alerts. The frontend coalesces closely spaced automatic updates into one trailing refresh. If a feed request is still running, automatic events queue one follow-up request so slow responses can finish. Manual refreshes and filter changes replace the current request immediately. Feed refresh requests time out after 15 seconds so a stalled response cannot block queued updates indefinitely.
