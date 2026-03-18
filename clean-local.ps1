[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

$pathsToRemove = @(
    ".venv",
    "node_modules",
    "_tmpbuild",
    ".pytest_cache",
    "backend\backend",
    "backend\tests\.tmp",
    "frontend\.next"
)

foreach ($relativePath in $pathsToRemove) {
    $targetPath = Join-Path $repoRoot $relativePath
    if (Test-Path $targetPath) {
        Write-Host "Removing $relativePath"
        Remove-Item -LiteralPath $targetPath -Recurse -Force
    }
}

$directoryPatterns = @("__pycache__", ".pytest_cache", ".tmp")
foreach ($pattern in $directoryPatterns) {
    Get-ChildItem -Path $repoRoot -Directory -Recurse -Force |
        Where-Object {
            $_.Name -eq $pattern -and
            $_.FullName -notlike (Join-Path $repoRoot "backend\.venv*") -and
            $_.FullName -notlike (Join-Path $repoRoot "frontend\node_modules*")
        } |
        ForEach-Object {
            Write-Host "Removing $($_.FullName.Substring($repoRoot.Length + 1))"
            Remove-Item -LiteralPath $_.FullName -Recurse -Force
        }
}
