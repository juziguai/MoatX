# MoatX 孤儿进程清理脚本
# 每次跑长时间命令前执行，杀掉之前残留的 Python 子进程
# 用法: .\scripts\kill_orphans.ps1

$ErrorActionPreference = "SilentlyContinue"
$currentPid = $PID
$killed = 0

Get-Process python* | Where-Object { $_.Id -ne $currentPid } | ForEach-Object {
    $age = (Get-Date) - $_.StartTime
    # 只杀跑了超过 30 秒的（避免误杀刚启动的正常进程）
    if ($age.TotalSeconds -gt 30) {
        Write-Host "  KILL orphan: PID=$($_.Id) CPU=$([math]::Round($_.CPU,1))s AGE=$([math]::Round($age.TotalMinutes,1))m"
        Stop-Process -Id $_.Id -Force
        $killed++
    }
}

if ($killed -eq 0) {
    Write-Host "  No orphans found"
} else {
    Write-Host "  Cleaned $killed orphan process(es)"
}
