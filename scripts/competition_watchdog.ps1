$ErrorActionPreference = "Stop"

$Repo = Split-Path -Parent $PSScriptRoot
$LogDir = Join-Path $Repo "data\competition_watchdog"
$LogFile = Join-Path $LogDir "watchdog_20260428.ndjson"
$Python = "python"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Set-Location $Repo

$startedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
try {
    $output = & $Python "scripts\competition_monitor.py" "--dry-run" "--no-orders" "--json" 2>&1
    $status = "ok"
    $exitCode = $LASTEXITCODE
} catch {
    $output = $_.Exception.Message
    $status = "error"
    $exitCode = 1
}

$record = [ordered]@{
    created_at = $startedAt
    status = $status
    exit_code = $exitCode
    note = "watchdog only; orders are invalid unless posted live in the chat thread"
    output = ($output -join "`n")
}

($record | ConvertTo-Json -Compress -Depth 8) | Add-Content -Path $LogFile -Encoding UTF8
