param([switch]$CheckOnly)
$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$now = [DateTimeOffset]::UtcNow
$dates = @('2026-11-09','2026-11-10','2026-11-11','2026-11-12','2026-11-13')
if ($now.ToString('yyyy-MM-dd') -notin $dates -or $now.Hour -lt 8 -or $now.Hour -ge 22 -or ($now.Hour -eq 21 -and $now.Minute -eq 59)) {
    [pscustomobject]@{status='outside_authorized_window';at_utc=$now.ToString('o');started=$false} | ConvertTo-Json -Compress
    return
}
$store = Join-Path $root '.tmp\realtime-research-20261109\live-comparison'
$scriptPath = Join-Path $root 'tools\news_live.py'
$python = Join-Path $root 'backend\.venv\Scripts\python.exe'
$mutex = [Threading.Mutex]::new($false,'Local\CEFNewsYahooComparison20261109')
$owned = $false
try {
    $owned = $mutex.WaitOne(0)
    if (-not $owned) { throw 'Another comparison launcher is checking the worker.' }
    $matching = @(Get-CimInstance Win32_Process | Where-Object {
        $_.Name -match '^python(w)?\.exe$' -and $_.CommandLine -match 'news_live\.py' -and $_.CommandLine -match '\byahoo-run\b'
    } | Select-Object ProcessId,ParentProcessId,CreationDate,ExecutablePath,CommandLine)
    if ($matching.Count -gt 0) {
        [pscustomobject]@{status='matching_process_present_no_launch';processes=$matching;started=$false} | ConvertTo-Json -Depth 5
        return
    }
    if ($CheckOnly) {
        [pscustomobject]@{status='eligible_no_matching_worker';started=$false;store=$store} | ConvertTo-Json -Compress
        return
    }
    $health = Invoke-RestMethod -Uri 'http://127.0.0.1:8001/health' -TimeoutSec 5 -MaximumRedirection 0
    if ($health.status -ne 'ok') { throw 'Healthy app required before starting this comparison.' }
    New-Item -ItemType Directory -Force -Path $store | Out-Null
    $reportText = & $python $scriptPath --store $store report
    if ($LASTEXITCODE -ne 0) { throw 'Could not inspect saved comparison controls.' }
    $report = $reportText | ConvertFrom-Json
    if ($report.controls.'yahoo:hold') {
        [pscustomobject]@{status='persisted_refusal_no_launch';control=$report.controls.'yahoo:hold';started=$false} | ConvertTo-Json -Depth 5
        return
    }
    $lockPath = Join-Path $store 'yahoo.lock'
    if (Test-Path -LiteralPath $lockPath) {
        $old = Get-Content -LiteralPath $lockPath -Raw | ConvertFrom-Json
        if (-not $old.pid -or -not $old.created_at) { throw 'Malformed prior lock needs review.' }
        $owner = Get-CimInstance Win32_Process -Filter ('ProcessId = {0}' -f [int]$old.pid)
        if ($owner -and $owner.CommandLine -match 'news_live\.py') { throw 'A lock owner is still present; inspect its full identity.' }
        # Rename only this verified task-local lock; retain the original record.
        $resolvedLock = [IO.Path]::GetFullPath($lockPath)
        if (-not $resolvedLock.StartsWith($store+[IO.Path]::DirectorySeparatorChar,[StringComparison]::OrdinalIgnoreCase)) { throw 'Lock path escaped comparison store.' }
        $archive = Join-Path $store ('stale-lock-'+$now.ToString('yyyyMMddTHHmmss')+'-'+[Guid]::NewGuid().ToString('N')+'.json')
        Move-Item -LiteralPath $resolvedLock -Destination $archive
    }
    $prefix = Join-Path $store ('session-'+$now.ToString('yyyyMMddTHHmmss'))
    $arguments = @(('"'+$scriptPath+'"'),'--store',('"'+$store+'"'),'yahoo-run','--symbols','OXLC','GOF','PDI','UTG')
    $launch = Start-Process -FilePath $python -ArgumentList $arguments -WorkingDirectory $root -WindowStyle Hidden -PassThru -RedirectStandardOutput ($prefix+'.stdout.log') -RedirectStandardError ($prefix+'.stderr.log')
    Start-Sleep -Seconds 2
    $processes = @(Get-CimInstance Win32_Process | Where-Object {
        $_.Name -match '^python(w)?\.exe$' -and $_.CommandLine -like ('*'+$scriptPath+'*') -and $_.CommandLine -match '\byahoo-run\b'
    } | Select-Object ProcessId,ParentProcessId,CreationDate,ExecutablePath,CommandLine)
    $state = if (Test-Path -LiteralPath $lockPath) { Get-Content -LiteralPath $lockPath -Raw | ConvertFrom-Json } else { $null }
    if (-not $state -or $state.pid -notin $processes.ProcessId) { throw 'Launch not verified. Inspect the saved logs; do not relaunch blindly.' }
    $result = [pscustomobject]@{status='started_and_pid_verified';at_utc=[DateTimeOffset]::UtcNow.ToString('o');wrapper_pid=$launch.Id;state=$state;processes=$processes;store=$store;stdout=$prefix+'.stdout.log';stderr=$prefix+'.stderr.log'}
    $result | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath ($prefix+'.json') -Encoding utf8
    $result | ConvertTo-Json -Depth 8
} finally {
    if ($owned) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
}
