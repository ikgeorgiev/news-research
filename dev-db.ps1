[CmdletBinding()]
param(
    [switch]$Wait,
    [int]$TimeoutSeconds = 45
)

$ErrorActionPreference = "Stop"

# Script location is the repo root.
$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $repoRoot

Write-Host "Starting PostgreSQL container (service: db)..."
docker compose up -d db

if (-not $Wait) {
    Write-Host "Database start requested. Use -Wait to block until healthy."
    exit 0
}

$containerId = (docker compose ps -q db).Trim()
if (-not $containerId) {
    throw "Could not find running container id for service 'db'."
}

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
while ((Get-Date) -lt $deadline) {
    $status = (docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $containerId).Trim()
    if ($status -in @("healthy", "running")) {
        Write-Host "Database is ready ($status)."
        exit 0
    }

    Write-Host "Waiting for db to be ready... current status: $status"
    Start-Sleep -Seconds 2
}

throw "Timed out after $TimeoutSeconds seconds waiting for db readiness."
