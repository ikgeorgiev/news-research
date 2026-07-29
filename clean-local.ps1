[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [switch]$Dependencies
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

$pathsToRemove = @(
    "_tmpbuild",
    ".coverage",
    "coverage",
    "coverage.xml",
    "htmlcov",
    ".pytest_cache",
    "backend\.coverage",
    "backend\coverage",
    "backend\coverage.xml",
    "backend\htmlcov",
    "backend\.pytest_cache",
    "backend\.ruff_cache",
    "backend\backend",
    "backend\tests\.tmp",
    "frontend\coverage",
    "frontend\.next",
    "frontend\next-env.d.ts",
    "frontend\tsconfig.tsbuildinfo"
)

if ($Dependencies) {
    $pathsToRemove += @(
        "backend\.venv",
        "frontend\node_modules"
    )
}

foreach ($relativePath in $pathsToRemove) {
    $targetPath = Join-Path $repoRoot $relativePath
    if ((Test-Path -LiteralPath $targetPath) -and $PSCmdlet.ShouldProcess($targetPath, "Remove generated path")) {
        Write-Host "Removing $relativePath"
        Remove-Item -LiteralPath $targetPath -Recurse -Force
    }
}

$directoryPatterns = @("__pycache__", ".pytest_cache", ".ruff_cache", ".tmp")
foreach ($pattern in $directoryPatterns) {
    Get-ChildItem -Path $repoRoot -Directory -Recurse -Force |
        Where-Object {
            $_.Name -eq $pattern -and
            $_.FullName -notlike (Join-Path $repoRoot "backend\.venv*") -and
            $_.FullName -notlike (Join-Path $repoRoot "frontend\node_modules*")
        } |
        ForEach-Object {
            if ($PSCmdlet.ShouldProcess($_.FullName, "Remove generated directory")) {
                Write-Host "Removing $($_.FullName.Substring($repoRoot.Length + 1))"
                Remove-Item -LiteralPath $_.FullName -Recurse -Force
            }
        }
}
