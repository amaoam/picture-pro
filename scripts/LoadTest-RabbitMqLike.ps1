param(
    [switch]$RunMatrix,
    [string]$ProjectPath = "C:\Users\ZMT\Desktop\project\picture-backend-pro",
    [string]$BaseUrl = "http://localhost:8123/api",
    [string]$K6Script = ".\k6\community_like_mq.js",
    [string]$K6Cores = "8-13",
    [string]$Duration = "60s",
    [int]$PreVus = 200,
    [int]$MaxVus = 400,
    [int[]]$Rates = @(100, 300, 500, 800, 1000),
    [int]$DrainTimeoutSeconds = 300
)

$ErrorActionPreference = "Stop"
if (-not (Test-Path $ProjectPath)) { throw "找不到项目目录：$ProjectPath" }
Set-Location $ProjectPath
New-Item -ItemType Directory -Force -Path ".\results" | Out-Null

$env:BASE_URL = $BaseUrl
$env:TARGET_TYPE = if ($env:TARGET_TYPE) { $env:TARGET_TYPE } else { "POST" }
$env:PRE_VUS = "$PreVus"
$env:MAX_VUS = "$MaxVus"
$env:SESSION_COOKIE_NAME = if ($env:SESSION_COOKIE_NAME) { $env:SESSION_COOKIE_NAME } else { "satoken" }
$env:USERS_FILE   = if ($env:USERS_FILE)   { $env:USERS_FILE }   else { Join-Path $ProjectPath "k6\users.local.json" }
$env:TARGETS_FILE = if ($env:TARGETS_FILE) { $env:TARGETS_FILE } else { Join-Path $ProjectPath "k6\targets.local.json" }

if (-not (Test-Path $env:USERS_FILE))   { throw "找不到用户文件：$env:USERS_FILE" }
if (-not (Test-Path $env:TARGETS_FILE)) { throw "找不到目标文件：$env:TARGETS_FILE" }

function Get-ProcessorMask {
    param([Parameter(Mandatory=$true)][string]$Cores)
    [long]$mask = 0
    if ($Cores -match "-") {
        $parts = $Cores.Split("-")
        if ($parts.Count -ne 2) { throw "无效的 CPU 范围：$Cores" }
        $start = [int]$parts[0]
        $end = [int]$parts[1]
        if ($start -lt 0 -or $end -lt $start -or $end -gt 62) { throw "CPU 范围不合法：$Cores" }
        for ($i = $start; $i -le $end; $i++) { $mask = $mask -bor (1L -shl $i) }
    }
    else {
        foreach ($core in $Cores.Split(",")) {
            $i = [int]$core.Trim()
            if ($i -lt 0 -or $i -gt 62) { throw "CPU 编号不合法：$i" }
            $mask = $mask -bor (1L -shl $i)
        }
    }
    return $mask
}

function Run-K6Pinned {
    param(
        [Parameter(Mandatory=$true)][string]$Rate,
        [Parameter(Mandatory=$true)][string]$SummaryFile,
        [Parameter(Mandatory=$false)][string]$Cores = $K6Cores
    )
    $env:RATE = $Rate
    $env:DURATION = $Duration
    $env:BASE_URL = $BaseUrl
    $env:PRE_VUS = "$PreVus"
    $env:MAX_VUS = "$MaxVus"
    $mask = Get-ProcessorMask -Cores $Cores

    $summaryDir = Split-Path -Parent $SummaryFile
    if ($summaryDir -and -not (Test-Path $summaryDir)) { New-Item -ItemType Directory -Force -Path $summaryDir | Out-Null }
    $k6Exe = (Get-Command "k6" -ErrorAction Stop).Source

    # PS 5.1 兼容：手动拼接带引号的参数字符串
    $argList = @("run", $K6Script, "--summary-export", $SummaryFile)
    $argString = ($argList | ForEach-Object {
        if ($_ -match '[\s"]') { '"' + ($_ -replace '"', '\"') + '"' } else { $_ }
    }) -join ' '

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $k6Exe
    $psi.WorkingDirectory = $ProjectPath
    $psi.UseShellExecute = $false
    $psi.Arguments = $argString

    $p = [System.Diagnostics.Process]::Start($psi)
    Start-Sleep -Milliseconds 500
    try {
        $p.ProcessorAffinity = [IntPtr]$mask
        Write-Host ("k6 PID={0} 已绑核 {1} (掩码: 0x{2:X})，MODE={3}，RATE={4}" -f $p.Id, $Cores, $mask, $env:TARGET_MODE, $Rate) -ForegroundColor Yellow
    }
    catch {
        Write-Warning "k6 已启动，但绑核失败：$($_.Exception.Message)"
    }

    $p.WaitForExit()
    $exitCode = $p.ExitCode
    $p.Dispose()

    if ($null -eq $exitCode) {
        if (-not (Test-Path $SummaryFile)) {
            throw "无法获取 k6 退出码且 SummaryFile 不存在：$SummaryFile"
        }
        Write-Warning "无法获取 k6 退出码，但 SummaryFile 已生成，继续：$SummaryFile"
    }
    elseif ($exitCode -ne 0) {
        throw "k6 退出码为 $exitCode，请检查测试输出与 SummaryFile=$SummaryFile"
    }

    Write-Host "k6 完成：$SummaryFile" -ForegroundColor Green
}

function Get-PrometheusMetricValue {
    param([Parameter(Mandatory=$true)][string]$MetricName)
    $uri = "$($BaseUrl.TrimEnd('/'))/actuator/prometheus"
    try {
        $content = (Invoke-WebRequest -UseBasicParsing -Uri $uri -TimeoutSec 5).Content
    }
    catch {
        throw "无法读取 Prometheus 指标 $uri。确认 Actuator 已暴露 prometheus：$($_.Exception.Message)"
    }
    $escaped = [regex]::Escape($MetricName)
    $pattern = '^' + $escaped + '(?:\{[^}]*\})?\s+([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)\s*$'
    $line = $content -split "`n" | Where-Object { $_ -match $pattern } | Select-Object -First 1
    if (-not $line) { throw "Prometheus 中找不到指标 $MetricName；请确认 RabbitMQ V1 代码已启动且配置正确。" }
    if ($line -match $pattern) {
        return [double]::Parse($Matches[1], [Globalization.CultureInfo]::InvariantCulture)
    }
    throw "无法解析指标行：$line"
}

function Wait-MqBacklogDrain {
    param(
        [int]$TimeoutSeconds = $DrainTimeoutSeconds,
        [int]$StableSamples = 5,
        [int]$PollSeconds = 2
    )
    $names = @(
        "community_like_rabbitmq_relay_stream_entries",
        "community_like_rabbitmq_relay_stream_pending",
        "community_like_rabbitmq_queue_depth",
        "community_like_rabbitmq_dead_depth",
        "community_like_rabbitmq_consumer_inflight"
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $stable = 0
    Write-Host "等待 Redis Stream、RabbitMQ 主队列/死信队列和消费者批次全部归零..." -ForegroundColor Cyan
    while ((Get-Date) -lt $deadline) {
        try {
            $values = @{}
            foreach ($name in $names) { $values[$name] = Get-PrometheusMetricValue -MetricName $name }
            if ($values["community_like_rabbitmq_dead_depth"] -gt 0) {
                Write-Warning "检测到死信队列非空。当前实验不应继续；请先检查死信原因并处理，再重新执行。"
                return $false
            }
            $zero = $true
            foreach ($name in $names) { if ($values[$name] -ne 0) { $zero = $false; break } }
            if ($zero) { $stable++ } else { $stable = 0 }
            Write-Host ("ready={0}, streamEntries={1}, streamPending={2}, queue={3}, dead={4}, inFlight={5}" -f `
                $stable, $values[$names[0]], $values[$names[1]], $values[$names[2]], $values[$names[3]], $values[$names[4]])
            if ($stable -ge $StableSamples) { Write-Host "积压已连续归零，可以开始下一组压测。" -ForegroundColor Green; return $true }
        }
        catch {
            Write-Warning $_.Exception.Message
            $stable = 0
        }
        Start-Sleep -Seconds $PollSeconds
    }
    Write-Warning "等待积压归零超时。不要继续下一组；检查消费者日志、Redis retry-counts、死信队列和 MySQL。"
    return $false
}

function Run-MqLikeMatrix {
    $experiments = @(
        @{ Mode = "single"; Prefix = "mq-A-single"; HotRatio = $null },
        @{ Mode = "multi";  Prefix = "mq-B-multi";  HotRatio = $null },
        @{ Mode = "mixed";  Prefix = "mq-C-mixed80"; HotRatio = "0.8" }
    )
    foreach ($experiment in $experiments) {
        $env:TARGET_MODE = $experiment.Mode
        if ($experiment.HotRatio) { $env:HOT_RATIO = $experiment.HotRatio } else { Remove-Item Env:\HOT_RATIO -ErrorAction SilentlyContinue }
        foreach ($rate in $Rates) {
            $summary = ".\results\$($experiment.Prefix)-${rate}rps-pinned.json"
            Run-K6Pinned -Rate "$rate" -SummaryFile $summary -Cores $K6Cores
            if (-not (Wait-MqBacklogDrain)) {
                throw "积压未归零，中止后续实验。最后一组=$summary"
            }
        }
    }
    Write-Host "全部 MQ 点赞压测矩阵完成。结果位于 .\results\。" -ForegroundColor Green
}

Write-Host "已载入 Run-K6Pinned、Wait-MqBacklogDrain、Run-MqLikeMatrix。" -ForegroundColor Green
Write-Host "手动单组示例：`$env:TARGET_MODE='single'; Run-K6Pinned -Rate '100' -SummaryFile '.\results\mq-A-single-100rps-pinned.json' -Cores '8-13'" -ForegroundColor Cyan
if ($RunMatrix) { Run-MqLikeMatrix }
