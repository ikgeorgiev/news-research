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

# Root .env is the canonical app env file. This script only overrides values
# that need different local-dev behavior from Docker defaults. DATABASE_URL
# and ADMIN_API_KEY are resolved through the same Settings class as the app
# after the virtual environment is ready.
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
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to bootstrap pip in backend virtual environment."
        }
    }

    # Keep the ignored local venv aligned with both requirements files. A hash
    # stamp tracks extras as well as base pins, unlike import/version spot checks.
    $requirementsFingerprint = (
        (Get-FileHash "requirements.txt" -Algorithm SHA256).Hash + ":" +
        (Get-FileHash "requirements-dev.txt" -Algorithm SHA256).Hash
    )
    $venvDir = Split-Path -Parent (Split-Path -Parent $venvPython)
    $dependencyStampPath = Join-Path $venvDir ".cef-requirements.sha256"
    $installedRequirementsFingerprint = ""
    if (Test-Path $dependencyStampPath) {
        $installedRequirementsFingerprint = (Get-Content $dependencyStampPath -Raw).Trim()
    }
    $needsDependencyInstall = $installedRequirementsFingerprint -ne $requirementsFingerprint

    if (-not $needsDependencyInstall) {
        $pinCheckScript = @'
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
import sys

for requirements_file in (Path("requirements.txt"), Path("requirements-dev.txt")):
    for raw_line in requirements_file.read_text(encoding="utf-8").splitlines():
        requirement = raw_line.partition("#")[0].strip()
        name_with_extras, separator, expected = requirement.partition("==")
        if not requirement or not separator:
            continue
        package_name = name_with_extras.split("[", 1)[0].strip()
        try:
            actual = version(package_name)
        except PackageNotFoundError:
            sys.exit(1)
        if actual != expected.strip():
            sys.exit(1)
'@
        & $venvPython -c $pinCheckScript *> $null
        $needsDependencyInstall = $LASTEXITCODE -ne 0
    }

    if (-not $needsDependencyInstall) {
        & $venvPython -m pip check *> $null
        $needsDependencyInstall = $LASTEXITCODE -ne 0
    }

    if ($needsDependencyInstall) {
        Write-Host "Installing or updating backend dependencies..."
        & $venvPython -m pip install -r requirements.txt -r requirements-dev.txt
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to install backend dependencies."
        }
        & $venvPython -m pip check
        if ($LASTEXITCODE -ne 0) {
            throw "Backend dependencies are installed but inconsistent."
        }
        [System.IO.File]::WriteAllText($dependencyStampPath, $requirementsFingerprint)
    }

    # Resolve process-environment and root .env values exactly as the app does.
    # Capture the JSON rather than echoing either secret to the terminal.
    $readConfigScript = @'
import json

from app.config import get_settings

settings = get_settings()
print(
    json.dumps(
        {
            "database_url": settings.database_url,
            "admin_api_key": settings.admin_api_key,
        }
    )
)
'@
    $resolvedConfigJson = (& $venvPython -c $readConfigScript | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to load DATABASE_URL and ADMIN_API_KEY from the root .env file."
    }
    $resolvedConfig = $resolvedConfigJson | ConvertFrom-Json

    $databaseUri = [System.UriBuilder]::new([string]$resolvedConfig.database_url)
    # This is a local-development helper, so always apply its loopback defaults
    # instead of inheriting a potentially remote DATABASE_URL from the caller.
    # Connecting elsewhere requires an explicit -DbHost/-DbPort choice.
    $databaseUri.Host = $DbHost
    $databaseUri.Port = $DbPort
    $env:DATABASE_URL = $databaseUri.Uri.AbsoluteUri

    $resolvedAdminApiKey = [string]$resolvedConfig.admin_api_key
    if ([string]::IsNullOrWhiteSpace($resolvedAdminApiKey)) {
        $resolvedAdminApiKey = [guid]::NewGuid().ToString("N")
        Write-Host "Generated a temporary ADMIN_API_KEY for this local run (value omitted)."
        Write-Host "Set ADMIN_API_KEY in .env before starting if you need to call admin endpoints."
    }
    $env:ADMIN_API_KEY = $resolvedAdminApiKey

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

    Write-Host "Running Alembic migrations..."
    & $venvPython migrate.py
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to run Alembic migrations."
    }

    Write-Host "Starting backend on http://${BindHost}:$Port"
    $databaseName = $databaseUri.Path.TrimStart("/")
    Write-Host "Database target: $($databaseUri.Host):$($databaseUri.Port)/$databaseName (credentials omitted)"
    # Open SSE streams otherwise keep reload waiting indefinitely for shutdown.
    & $venvPython -m uvicorn app.main:app --reload --host $BindHost --port $Port --timeout-graceful-shutdown 5
}
finally {
    Pop-Location
}
