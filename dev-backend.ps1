[CmdletBinding()]
param(
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 8001,
    [string]$DbHost = "localhost",
    [int]$DbPort = 5433,
    [switch]$DisableScheduler
)

$ErrorActionPreference = "Stop"

# Script location is the repo root.
$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$backendDir = Join-Path $repoRoot "backend"
$tickersCsv = Join-Path $repoRoot "data\\cef_tickers.csv"

# Local dev defaults so backend can run outside Docker without extra setup.
$env:DATABASE_URL = "postgresql+psycopg://cef:cef@${DbHost}:$DbPort/cef_news"
$env:TICKERS_CSV_PATH = $tickersCsv
$env:CORS_ORIGINS = "http://localhost:3000,http://127.0.0.1:3000,http://localhost:3005,http://127.0.0.1:3005"
$env:SCHEDULER_ENABLED = "true"
if ($DisableScheduler) {
    $env:SCHEDULER_ENABLED = "false"
}

Push-Location $backendDir
try {
    $venvPython = Join-Path $backendDir ".venv\\Scripts\\python.exe"
    if (-not (Test-Path $venvPython)) {
        Write-Host "Creating backend virtual environment..."
        python -m venv .venv
    }

    # Re-resolve after potential creation.
    $venvPython = Join-Path $backendDir ".venv\\Scripts\\python.exe"
    if (-not (Test-Path $venvPython)) {
        throw "Could not create backend virtual environment at $venvPython"
    }

    # Some environments are created without pip; bootstrap it if missing.
    & $venvPython -m pip --version *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Bootstrapping pip in backend virtual environment..."
        & $venvPython -m ensurepip --upgrade
    }

    # Install backend requirements if uvicorn isn't available in the venv.
    & $venvPython -c "import uvicorn" *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Installing backend dependencies..."
        & $venvPython -m pip install -r requirements.txt -r requirements-dev.txt
    }

    # Ensure the target database exists (useful when Postgres volume was initialized with a different DB).
    $ensureDbScript = @'
import os
import sys
from urllib.parse import urlsplit, urlunsplit

import psycopg
from psycopg.sql import SQL, Identifier

raw_url = os.environ.get("DATABASE_URL", "").strip()
prefix = "postgresql+psycopg://"
if raw_url.startswith(prefix):
    raw_url = "postgresql://" + raw_url[len(prefix):]

if not raw_url:
    raise RuntimeError("DATABASE_URL is empty")

parts = urlsplit(raw_url)
db_name = parts.path.lstrip("/") or "postgres"

try:
    with psycopg.connect(raw_url):
        print(f"Database reachable: {db_name}")
except Exception as target_error:
    admin_url = urlunsplit(parts._replace(path="/postgres"))
    try:
        with psycopg.connect(admin_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
                if cur.fetchone() is None:
                    cur.execute(SQL("CREATE DATABASE {}").format(Identifier(db_name)))
                    print(f"Created database: {db_name}")
                else:
                    print(f"Database exists: {db_name}")
    except Exception as admin_error:
        print("Could not connect to target DB or create it via postgres admin DB.", file=sys.stderr)
        print(f"Target DB error: {target_error}", file=sys.stderr)
        print(f"Admin DB error: {admin_error}", file=sys.stderr)
        raise
'@
    Write-Host "Ensuring database exists for DATABASE_URL..."
    & $venvPython -c $ensureDbScript
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to ensure database exists. Check DATABASE_URL and Postgres credentials."
    }

    Write-Host "Starting backend on http://${BindHost}:$Port"
    Write-Host "DATABASE_URL=$($env:DATABASE_URL)"
    & $venvPython -m uvicorn app.main:app --reload --host $BindHost --port $Port
}
finally {
    Pop-Location
}
