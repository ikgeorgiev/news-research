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
    $nodeModulesPath = Join-Path $frontendDir "node_modules"
    $dependencyStampPath = Join-Path $nodeModulesPath ".cef-package-lock.sha256"
    $packageLockHash = (Get-FileHash (Join-Path $frontendDir "package-lock.json") -Algorithm SHA256).Hash
    $installedLockHash = ""
    if (Test-Path $dependencyStampPath) {
        $installedLockHash = (Get-Content $dependencyStampPath -Raw).Trim()
    }
    $needsInstall = -not (Test-Path $nodeModulesPath) -or $installedLockHash -ne $packageLockHash

    if ($needsInstall) {
        Write-Host "Installing frontend dependencies with npm ci..."
        npm ci
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to install frontend dependencies."
        }
        [System.IO.File]::WriteAllText($dependencyStampPath, $packageLockHash)
    }

    Write-Host "Starting frontend on http://${BindHost}:$Port"
    Write-Host "NEXT_PUBLIC_API_BASE=$($env:NEXT_PUBLIC_API_BASE)"
    npm run dev -- --hostname $BindHost --port $Port
}
finally {
    Pop-Location
}
