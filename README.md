# CEF News Feed (V1)

Local-first RSS aggregation platform for closed-end funds.

## Stack

- Backend: FastAPI + SQLAlchemy + APScheduler
- Frontend: Next.js (App Router)
- DB: PostgreSQL
- Sources: Yahoo Finance, PRNewswire, GlobeNewswire

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

## Local Dev Without Docker

### Backend

```bash
cd backend
python -m venv .venv
. .venv/Scripts/activate  # Windows PowerShell
pip install -r requirements.txt -r requirements-dev.txt
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

### Frontend

```bash
cd frontend
npm install
npm run dev
```

Set `NEXT_PUBLIC_API_BASE` if backend is on a non-default host/port.

## API Endpoints

- `GET /health`
- `GET /api/v1/tickers`
- `GET /api/v1/news`
- `GET /api/v1/news/{id}`
- `POST /api/v1/admin/ingest/run-once`
- `GET /api/v1/admin/ingest/status`

`GET /api/v1/news` defaults to mapped CEF-linked articles only.
Use `include_unmapped=true` to include broad wire stories that are not mapped to your ticker universe.

## Ticker Universe

Edit `data/cef_tickers.csv` with your full list:

```csv
ticker,fund_name,sponsor,active
GOF,Guggenheim Strategic Opportunities Fund,Guggenheim,true
...
```

## Notes

- V1 intentionally excludes account-protected Business Wire feeds.
- If a feed fails intermittently, status is tracked in `ingestion_runs`.
- Default polling interval is 60 seconds.
