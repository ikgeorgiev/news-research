#!/bin/sh
set -eu

echo "Running Alembic migrations..."
python migrate.py

echo "Starting uvicorn..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
