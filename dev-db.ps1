[CmdletBinding()]
param(
    [switch]$Wait,
    [int]$TimeoutSeconds = 45
)

$ErrorActionPreference = "Stop"

# Script location is the repo root.
$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $repoRoot

function Write-LocalConnectionInfo {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ContainerId
    )

    $published = @(docker port $ContainerId 5432/tcp)
    if ($LASTEXITCODE -ne 0 -or $published.Count -eq 0) {
        throw "Cannot verify the database container's published port."
    }
    Write-Host "Published PostgreSQL bindings: $($published -join ', ')"
    foreach ($binding in $published) {
        if ($binding -notmatch '^(?:127\.\d+\.\d+\.\d+|\[::1\]):\d+$') {
            Write-Warning "PostgreSQL binding $binding is outside loopback. Compare the running container with docker-compose.yml."
        }
    }

    # Read only non-secret identity fields from the container so the report
    # matches Compose's resolved environment without exposing the password.
    $databaseIdentity = @(
        docker exec $ContainerId sh -c 'printf "%s\n%s\n" "$POSTGRES_USER" "$POSTGRES_DB"'
    )
    if ($LASTEXITCODE -ne 0 -or $databaseIdentity.Count -lt 2) {
        Write-Warning "Database started, but its resolved user/database could not be read."
        return
    }

    Write-Host (
        "Database identity: " +
        "user=$($databaseIdentity[0]) database=$($databaseIdentity[1]) (password omitted)"
    )
}

Write-Host "Starting PostgreSQL container (service: db)..."
docker compose up -d db
if ($LASTEXITCODE -ne 0) {
    throw "Docker Compose could not start the database."
}

$containerId = (docker compose ps -q db).Trim()
if (-not $containerId) {
    throw "Could not find running container id for service 'db'."
}

if (-not $Wait) {
    Write-Host "Database start requested. Use -Wait to block until healthy."
    Write-LocalConnectionInfo -ContainerId $containerId
    exit 0
}

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
while ((Get-Date) -lt $deadline) {
    $status = (docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $containerId).Trim()
    if ($status -in @("healthy", "running")) {
        Write-Host "Database is ready ($status)."
        Write-LocalConnectionInfo -ContainerId $containerId
        exit 0
    }

    Write-Host "Waiting for db to be ready... current status: $status"
    Start-Sleep -Seconds 2
}

throw "Timed out after $TimeoutSeconds seconds waiting for db readiness."
