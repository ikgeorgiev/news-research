[CmdletBinding()]
param(
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 3005,
    [string]$ApiBase = "http://127.0.0.1:8001"
)

$ErrorActionPreference = "Stop"

# Script location is the repo root.
$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$frontendDir = Join-Path $repoRoot "frontend"

$env:NEXT_PUBLIC_API_BASE = $ApiBase

Push-Location $frontendDir
try {
    Write-Host "Starting frontend on http://${BindHost}:$Port"
    Write-Host "NEXT_PUBLIC_API_BASE=$($env:NEXT_PUBLIC_API_BASE)"
    npm run dev -- --hostname $BindHost --port $Port
}
finally {
    Pop-Location
}
