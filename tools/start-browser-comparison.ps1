[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateSet('Godel','Newsfilter')][string]$Source,
    [ValidateRange(1,20)][int]$Minutes = 20,
    [switch]$UntilClose,
    [switch]$CompatibilityCheck,
    [switch]$CheckOnly
)
$ErrorActionPreference='Stop'
$taskRoot=(Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$taskPython=Join-Path $taskRoot 'backend\.venv\Scripts\python.exe'
$isGodel=$Source -eq 'Godel'
$taskScript=if($isGodel){'news_live.py'}else{'newsfilter_live.py'}
$taskCommand=if($isGodel){'godel-serve'}else{'serve'}
$taskPortNumber=if($isGodel){8766}else{8767}
$taskStoreRelative=if($isGodel){'.tmp/realtime-research-20261109/live-comparison'}else{'.tmp/realtime-research-20261109/newsfilter-comparison'}
$taskStore=Join-Path $taskRoot $taskStoreRelative
$taskHoldKey=if($isGodel){'godel-browser:hold'}else{'newsfilter-browser:hold'}
$mutex=[Threading.Mutex]::new($false,('Local\CEFNewsBrowserComparison'+$Source))
if(-not $mutex.WaitOne(0)){throw 'Another launcher is inspecting this receiver; no duplicate started.'}
try {
    $taskNow=[DateTimeOffset]::UtcNow
    $taskAllowed=$taskNow.ToString('yyyy-MM-dd') -in @('2026-11-09','2026-11-10','2026-11-11','2026-11-12','2026-11-13') -and $taskNow.Hour -ge 8 -and $taskNow.Hour -lt 22
    $taskWorkers=@(Get-CimInstance Win32_Process | Where-Object {
        $_.Name -match '^python(w)?\.exe$' -and $_.CommandLine -match [regex]::Escape($taskScript) -and $_.CommandLine -match ('\b'+$taskCommand+'\b')
    })
    $taskPort=@(Get-NetTCPConnection -State Listen -LocalPort $taskPortNumber -ErrorAction SilentlyContinue)
    $taskHold=$null
    if(Test-Path -LiteralPath (Join-Path $taskStore 'live.sqlite')) {
        $taskReport=& $taskPython (Join-Path $PSScriptRoot $taskScript) --store $taskStore report
        if($LASTEXITCODE -ne 0){throw 'Cannot inspect saved controls; no receiver started.'}
        $taskSavedReport=$taskReport | ConvertFrom-Json
        $taskSourceKey=if($isGodel){'godel-browser'}else{'newsfilter-browser'}
        $taskHold=$taskSavedReport.controls.$taskHoldKey
        if($taskSavedReport.PSObject.Properties.Name -contains 'effective_browser_holds') {
            $taskHold=$taskSavedReport.effective_browser_holds.$taskSourceKey
        }
    }
    $info=[ordered]@{checked_at_utc=$taskNow.ToString('o');source=$Source;authorized_weekday_window=$taskAllowed;store=$taskStore;hold=$taskHold;workers=@($taskWorkers|Select-Object ProcessId,ParentProcessId,CreationDate,ExecutablePath,CommandLine);listeners=@($taskPort|Select-Object LocalAddress,LocalPort,OwningProcess);starts_workers=$false}
    if($CheckOnly){$info|ConvertTo-Json -Depth 6;return}
    if($UntilClose -and $CompatibilityCheck){throw 'All-day mode is weekday-only; compatibility mode cannot extend it.'}
    if(-not $taskAllowed -and -not $CompatibilityCheck){throw 'Outside November9–13,08:00–22:00UTC; no receiver started.'}
    if($taskWorkers.Count -or $taskPort.Count){throw 'A matching receiver or listener already exists. Inspect -CheckOnly; no duplicate started.'}
    if($taskHold){throw 'Saved source refusal/failure requires review. It was not cleared.'}
    New-Item -ItemType Directory -Path $taskStore -Force | Out-Null
    $taskSessionDir=Join-Path $taskStore ('session-'+$taskNow.ToString('yyyyMMddTHHmmssfff')+'-'+$Source.ToLower())
    New-Item -ItemType Directory -Path $taskSessionDir | Out-Null
    $info|ConvertTo-Json -Depth 6|Set-Content -LiteralPath (Join-Path $taskSessionDir 'preflight.json') -Encoding utf8
    $taskPairing=Join-Path $taskStore 'pairing.json'
    if(Test-Path -LiteralPath $taskPairing){Copy-Item -LiteralPath $taskPairing -Destination (Join-Path $taskSessionDir 'previous-pairing.json')}
    $taskArgs=@(('tools/'+$taskScript),'--store',$taskStoreRelative,$taskCommand,'--minutes',"$Minutes")
    if($UntilClose){$taskArgs+='--until-close'}
    if($CompatibilityCheck){$taskArgs+='--compatibility-check'}
    $taskOut=Join-Path $taskSessionDir 'stdout.log'
    $taskErr=Join-Path $taskSessionDir 'stderr.log'
    $wrapper=Start-Process -FilePath $taskPython -ArgumentList $taskArgs -WorkingDirectory $taskRoot -WindowStyle Hidden -RedirectStandardOutput $taskOut -RedirectStandardError $taskErr -PassThru
    for($i=0;$i -lt 50;$i++){
        Start-Sleep -Milliseconds 100
        if((Test-Path $taskOut) -and (Get-Item $taskOut).Length -gt 0){break}
        if($wrapper.HasExited){throw "Receiver exited; inspect $taskErr before retrying."}
    }
    $ready=Get-Content $taskOut -First 1|ConvertFrom-Json
    if($ready.status -ne 'ready'){throw "Readiness unknown; inspect $taskSessionDir before retrying."}
    $actual=Get-CimInstance Win32_Process -Filter "ProcessId=$($ready.pid)"
    if(-not $actual -or $actual.CommandLine -notmatch [regex]::Escape($taskScript) -or $actual.CommandLine -notmatch $taskCommand -or $actual.CreationDate.ToUniversalTime() -lt $taskNow.UtcDateTime.AddSeconds(-1)) {throw 'Receiver identity mismatch; inspect before any process action.'}
    @(Get-CimInstance Win32_Process|Where-Object{$_.ProcessId -in @($wrapper.Id,$ready.pid)}|Select-Object ProcessId,ParentProcessId,CreationDate,ExecutablePath,CommandLine)|ConvertTo-Json|Set-Content -LiteralPath (Join-Path $taskSessionDir 'process.json') -Encoding utf8
    [ordered]@{status='ready';source=$Source;connection_code=$ready.connection_code;pair_until_utc=$ready.pair_until;expires_at_utc=$ready.expires_at;mode=$ready.mode;pid=$ready.pid;wrapper_pid=$wrapper.Id;store=$taskStore;session=$ready.session;compatibility_history=[bool]$CompatibilityCheck}|ConvertTo-Json
}finally{$mutex.ReleaseMutex();$mutex.Dispose()}
