[CmdletBinding()]
param(
    [string]$ProjectPath = "C:\Users\ZMT\Desktop\project\picture-backend-pro",
    [string]$SourcePath = "",
    [string]$OutputPath = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0

if ([string]::IsNullOrWhiteSpace($SourcePath)) {
    $SourcePath = Join-Path $ProjectPath "Benchmark-CommunityLike.ps1"
}
if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path $ProjectPath "Benchmark-CommunityLike-DeadLetterAware.ps1"
}
if (-not (Test-Path -LiteralPath $SourcePath -PathType Leaf)) {
    throw "找不到当前仓库的 Benchmark 基线脚本：$SourcePath。请确认使用 GitHub master 中的 Benchmark-CommunityLike.ps1。"
}

function Replace-RegexOnce {
    param(
        [Parameter(Mandatory = $true)][string]$Text,
        [Parameter(Mandatory = $true)][string]$Pattern,
        [Parameter(Mandatory = $true)][string]$Replacement,
        [Parameter(Mandatory = $true)][string]$Description
    )
    $regex = [regex]::new($Pattern, [System.Text.RegularExpressions.RegexOptions]::Multiline -bor [System.Text.RegularExpressions.RegexOptions]::Singleline)
    $found = $regex.Matches($Text)
    if ($found.Count -ne 1) {
        throw "无法安全修改 $Description：预期匹配 1 处，实际匹配 $($found.Count) 处。文件可能不是本更新包对应的仓库版本；没有覆盖原文件。"
    }
    $replacementValue = $Replacement
    $evaluatorBlock = { param($m) return $replacementValue }.GetNewClosure()
    $evaluator = [System.Text.RegularExpressions.MatchEvaluator]$evaluatorBlock
    return $regex.Replace($Text, $evaluator, 1)
}

function Replace-TextOnce {
    param(
        [Parameter(Mandatory = $true)][string]$Text,
        [Parameter(Mandatory = $true)][string]$Old,
        [Parameter(Mandatory = $true)][string]$New,
        [Parameter(Mandatory = $true)][string]$Description
    )
    $count = ([regex]::Matches($Text, [regex]::Escape($Old))).Count
    if ($count -ne 1) {
        throw "无法安全修改 $Description：预期匹配 1 处，实际匹配 $count 处。没有覆盖原文件。"
    }
    return $Text.Replace($Old, $New)
}

$content = [System.IO.File]::ReadAllText((Resolve-Path -LiteralPath $SourcePath).Path, [System.Text.Encoding]::UTF8)
# Normalize to LF so exact block replacements work for both Git LF and Windows CRLF checkouts.
$content = $content.Replace("`r`n", "`n").Replace("`r", "`n")

# 1) Preflight：明确要求 Retry Queue 与三个死信审计指标已经被应用暴露。
$newRequired = @'
    $required = @(
        "community_like_rabbitmq_relay_stream_entries",
        "community_like_rabbitmq_relay_stream_pending",
        "community_like_rabbitmq_queue_depth",
        "community_like_rabbitmq_retry_depth",
        "community_like_rabbitmq_dead_depth",
        "community_like_rabbitmq_consumer_inflight",
        "community_like_rabbitmq_dead_letter_published_total",
        "community_like_rabbitmq_dead_letter_audit_persisted_total",
        "community_like_rabbitmq_dead_letter_audit_failures_total"
    )
'@
$content = Replace-RegexOnce -Text $content `
    -Pattern '(?ms)^    \$required = @\(\r?\n.*?^    \)' `
    -Replacement $newRequired.TrimEnd([char[]]@("`r", "`n")) `
    -Description "Test-ApiPreflight 指标清单"

# 2) Replace the drain function. DLQ depth is now waited out, not failed immediately.
$newDrain = @'
function Wait-MqBacklogDrain {
    param(
        [string]$RunId,
        [string]$Mode,
        [int]$Rate,
        [int]$Repetition,
        [hashtable]$CounterBefore
    )

    $gaugeNames = @(
        "community_like_rabbitmq_relay_stream_entries",
        "community_like_rabbitmq_relay_stream_pending",
        "community_like_rabbitmq_queue_depth",
        "community_like_rabbitmq_retry_depth",
        "community_like_rabbitmq_dead_depth",
        "community_like_rabbitmq_consumer_inflight"
    )
    $publishedName = "community_like_rabbitmq_dead_letter_published_total"
    $persistedName = "community_like_rabbitmq_dead_letter_audit_persisted_total"
    $auditFailuresName = "community_like_rabbitmq_dead_letter_audit_failures_total"

    foreach ($name in @($publishedName, $persistedName, $auditFailuresName)) {
        if ($null -eq $CounterBefore -or -not $CounterBefore.ContainsKey($name)) {
            throw "排空前 Counter 快照缺少 $name，不能确认死信审计状态。"
        }
    }

    $deadline = (Get-Date).AddSeconds($script:DrainTimeoutSeconds)
    $stable = 0
    $drainStart = Get-Date
    $lastMap = $null
    Write-Host "等待 Redis Stream、RabbitMQ 主队列/Retry Queue/DLQ、消费者及死信审计全部排空..." -ForegroundColor Cyan

    while ((Get-Date) -lt $deadline) {
        $map = Write-PrometheusSample -Phase "drain" -RunId $RunId -Mode $Mode -Rate $Rate -Repetition $Repetition
        if ($null -eq $map) {
            $stable = 0
            Start-Sleep -Seconds $script:DrainPollSeconds
            continue
        }
        $lastMap = $map
        $values = @{}
        foreach ($name in $gaugeNames) {
            if (-not $map.ContainsKey($name)) {
                throw "排空检查缺少 Gauge $name；为避免把未知状态当作归零，中止矩阵。"
            }
            $values[$name] = [double]$map[$name]
            if ($values[$name] -lt 0) {
                throw "指标 $name 返回负值 $($values[$name])，RabbitAdmin 查询可能失败，中止矩阵。"
            }
        }

        $publishedDelta = Get-CounterDelta -Before $CounterBefore -After $map -MetricName $publishedName
        $persistedDelta = Get-CounterDelta -Before $CounterBefore -After $map -MetricName $persistedName
        $auditFailuresDelta = Get-CounterDelta -Before $CounterBefore -After $map -MetricName $auditFailuresName
        if ($null -eq $publishedDelta -or $null -eq $persistedDelta -or $null -eq $auditFailuresDelta) {
            throw "死信审计 Counter 缺失，不能判定排空状态。"
        }
        if ($publishedDelta -lt 0 -or $persistedDelta -lt 0 -or $auditFailuresDelta -lt 0) {
            throw "死信审计 Counter 出现回退，可能是压测期间应用重启或 Prometheus 目标切换；本轮结果不可比较。"
        }

        # DLQ Gauge 只统计 Ready，不含已投递但尚未 ACK 的消息。
        # published > persisted 可以覆盖这段审计在途窗口。
        $auditPending = [math]::Max(0.0, ([double]$publishedDelta - [double]$persistedDelta))
        $allGaugesZero = $true
        foreach ($name in $gaugeNames) {
            if ($values[$name] -ne 0) { $allGaugesZero = $false; break }
        }
        $auditCaughtUp = ([double]$persistedDelta -ge [double]$publishedDelta)
        $allDrained = $allGaugesZero -and $auditCaughtUp

        if ($allDrained) { $stable++ } else { $stable = 0 }
        if ($values["community_like_rabbitmq_dead_depth"] -gt 0) {
            $note = "waiting_for_dead_letter_audit_consumer"
        }
        elseif ($auditPending -gt 0) {
            $note = "waiting_for_audit_persisted_counter"
        }
        elseif (-not $allGaugesZero) {
            $note = "backlog_remaining"
        }
        else {
            $note = "all_zero_and_audit_caught_up"
        }

        Write-CsvRow -Path (Join-Path $script:CurrentRunDir "drain-check.csv") -Row ([pscustomobject]@{
            timestamp_utc = [DateTime]::UtcNow.ToString("o")
            run_id = $RunId
            stable_samples = $stable
            ready = ($stable -ge $script:DrainStableSamples)
            stream_entries = $values["community_like_rabbitmq_relay_stream_entries"]
            stream_pending = $values["community_like_rabbitmq_relay_stream_pending"]
            queue_depth = $values["community_like_rabbitmq_queue_depth"]
            retry_depth = $values["community_like_rabbitmq_retry_depth"]
            dead_depth = $values["community_like_rabbitmq_dead_depth"]
            consumer_inflight = $values["community_like_rabbitmq_consumer_inflight"]
            dead_published_delta = $publishedDelta
            dead_audit_persisted_delta = $persistedDelta
            dead_audit_pending_delta = $auditPending
            dead_audit_failures_delta = $auditFailuresDelta
            note = $note
        })

        Write-Host ("stable={0}/{1}, stream={2}, streamPending={3}, main={4}, retry={5}, dead={6}, inFlight={7}, audit={8}/{9}, auditPending={10}, auditFailures={11}" -f `
            $stable, $script:DrainStableSamples,
            $values["community_like_rabbitmq_relay_stream_entries"],
            $values["community_like_rabbitmq_relay_stream_pending"],
            $values["community_like_rabbitmq_queue_depth"],
            $values["community_like_rabbitmq_retry_depth"],
            $values["community_like_rabbitmq_dead_depth"],
            $values["community_like_rabbitmq_consumer_inflight"],
            $persistedDelta, $publishedDelta, $auditPending, $auditFailuresDelta)

        if ($stable -ge $script:DrainStableSamples) {
            $seconds = (New-TimeSpan -Start $drainStart -End (Get-Date)).TotalSeconds
            return [pscustomobject]@{
                success = $true
                reason = "queues_zero_and_dead_letter_audit_caught_up"
                seconds = [math]::Round($seconds, 2)
                after_metrics = $map
            }
        }
        Start-Sleep -Seconds $script:DrainPollSeconds
    }

    $reason = "drain_timeout"
    if ($null -ne $lastMap -and $lastMap.ContainsKey($publishedName) -and $lastMap.ContainsKey($persistedName)) {
        $p = Get-CounterDelta -Before $CounterBefore -After $lastMap -MetricName $publishedName
        $a = Get-CounterDelta -Before $CounterBefore -After $lastMap -MetricName $persistedName
        if ($null -ne $p -and $null -ne $a -and $p -gt $a) { $reason = "drain_timeout_dead_letter_audit_pending" }
    }
    return [pscustomobject]@{
        success = $false
        reason = $reason
        seconds = [math]::Round((New-TimeSpan -Start $drainStart -End (Get-Date)).TotalSeconds, 2)
        after_metrics = $lastMap
    }
}
'@
$newDrainReplacement = $newDrain.TrimEnd([char[]]@("`r", "`n")) + "`n"
$content = Replace-RegexOnce -Text $content `
    -Pattern '(?ms)^function Wait-MqBacklogDrain \{.*?^\}\r?\n(?:\r?\n)*(?=function Scrub-K6Log)' `
    -Replacement $newDrainReplacement `
    -Description "Wait-MqBacklogDrain 函数"

# 3) Every run must start with a clean pipeline. Do not hide previous-run backlog in this run's delta.
$beforeBlock = @'
            $beforeMap = Write-PrometheusSample -Phase "pre" -RunId $runId -Mode $mode -Rate $rate -Repetition $rep
            if ($null -eq $beforeMap) { throw "正式压测前无法读取 Prometheus，停止矩阵。" }
'@
$afterBlock = @'
            $beforeMap = Write-PrometheusSample -Phase "pre" -RunId $runId -Mode $mode -Rate $rate -Repetition $rep
            if ($null -eq $beforeMap) { throw "正式压测前无法读取 Prometheus，停止矩阵。" }

            $initialGaugeNames = @(
                "community_like_rabbitmq_relay_stream_entries",
                "community_like_rabbitmq_relay_stream_pending",
                "community_like_rabbitmq_queue_depth",
                "community_like_rabbitmq_retry_depth",
                "community_like_rabbitmq_dead_depth",
                "community_like_rabbitmq_consumer_inflight"
            )
            $initialBacklog = @()
            foreach ($name in $initialGaugeNames) {
                if (-not $beforeMap.ContainsKey($name)) { throw "开始本轮前缺少指标 $name。" }
                $value = [double]$beforeMap[$name]
                if ($value -lt 0) { throw "开始本轮前指标 $name=$value，RabbitAdmin 查询状态异常。" }
                if ($value -ne 0) { $initialBacklog += "$name=$value" }
            }
            $initialPublished = [double]$beforeMap["community_like_rabbitmq_dead_letter_published_total"]
            $initialPersisted = [double]$beforeMap["community_like_rabbitmq_dead_letter_audit_persisted_total"]
            if ($initialPublished -gt $initialPersisted) {
                $initialBacklog += "dead_letter_audit_pending=$($initialPublished - $initialPersisted)"
            }
            if ($initialBacklog.Count -gt 0) {
                throw "本轮开始前异步链路并非空闲：$($initialBacklog -join ', ')。请先排空/审计历史消息，再开始实验。"
            }
'@
$content = Replace-TextOnce -Text $content -Old $beforeBlock.TrimEnd([char[]]@("`r", "`n")) -New $afterBlock.TrimEnd([char[]]@("`r", "`n")) -Description "每轮开始前的异步链路空闲检查"

# 4) Capture per-run dead-letter counter deltas immediately after drain, before the summary is built.
$summaryAnchor = @'
            $drainEndUtc = [DateTime]::UtcNow
'@
$summaryInsert = @'
            $drainEndUtc = [DateTime]::UtcNow

            $deadLetterPublishedDelta = Get-CounterDelta -Before $beforeMap -After $afterDrainMap -MetricName "community_like_rabbitmq_dead_letter_published_total"
            $deadLetterAuditPersistedDelta = Get-CounterDelta -Before $beforeMap -After $afterDrainMap -MetricName "community_like_rabbitmq_dead_letter_audit_persisted_total"
            $deadLetterAuditFailuresDelta = Get-CounterDelta -Before $beforeMap -After $afterDrainMap -MetricName "community_like_rabbitmq_dead_letter_audit_failures_total"
            if ($null -eq $deadLetterPublishedDelta -or $null -eq $deadLetterAuditPersistedDelta -or $null -eq $deadLetterAuditFailuresDelta) {
                Write-Warning "本轮最终 Prometheus 快照不可用，死信 Counter 增量将记为 null；本轮不能作为有效性能结果。"
                $deadLetterPublishedDelta = $null
                $deadLetterAuditPersistedDelta = $null
                $deadLetterAuditFailuresDelta = $null
                $deadLetterAuditPendingDelta = $null
            }
            else {
                $deadLetterAuditPendingDelta = [math]::Max(0.0, ([double]$deadLetterPublishedDelta - [double]$deadLetterAuditPersistedDelta))
            }
'@
$content = Replace-TextOnce -Text $content -Old $summaryAnchor.TrimEnd([char[]]@("`r", "`n")) -New $summaryInsert.TrimEnd([char[]]@("`r", "`n")) -Description "本轮死信 Counter 汇总计算"

# 5) Append fields to run metadata and CSV summary.
$counterAnchor = @'
                consumer_dead_lettered_delta = Get-CounterDelta -Before $beforeMap -After $afterDrainMap -MetricName "community_like_rabbitmq_consumer_dead_lettered_total"
'@
$counterReplacement = @'
                consumer_dead_lettered_delta = Get-CounterDelta -Before $beforeMap -After $afterDrainMap -MetricName "community_like_rabbitmq_consumer_dead_lettered_total"
                dead_letter_published_delta = $deadLetterPublishedDelta
                dead_letter_audit_persisted_delta = $deadLetterAuditPersistedDelta
                dead_letter_audit_failures_delta = $deadLetterAuditFailuresDelta
                dead_letter_audit_pending_delta = $deadLetterAuditPendingDelta
                dead_letter_detected = ($deadLetterPublishedDelta -gt 0)
                dead_letter_audit_complete = ($deadLetterAuditPendingDelta -eq 0)
                dead_letter_clean_run = ($deadLetterPublishedDelta -eq 0 -and $deadLetterAuditFailuresDelta -eq 0)
'@
$content = Replace-TextOnce -Text $content -Old $counterAnchor.TrimEnd([char[]]@("`r", "`n")) -New $counterReplacement.TrimEnd([char[]]@("`r", "`n")) -Description "run-metadata 死信统计字段"

$retrySummaryAnchor = @'
                dead_queue_depth_max_during_load = Get-MetricSummary -Path (Join-Path $runDir "prometheus-timeseries.csv") -MetricName "community_like_rabbitmq_dead_depth" -Suffix "max"
'@
$retrySummaryReplacement = @'
                retry_queue_depth_max_during_load = Get-MetricSummary -Path (Join-Path $runDir "prometheus-timeseries.csv") -MetricName "community_like_rabbitmq_retry_depth" -Suffix "max"
                dead_queue_depth_max_during_load = Get-MetricSummary -Path (Join-Path $runDir "prometheus-timeseries.csv") -MetricName "community_like_rabbitmq_dead_depth" -Suffix "max"
'@
$content = Replace-TextOnce -Text $content -Old $retrySummaryAnchor.TrimEnd([char[]]@("`r", "`n")) -New $retrySummaryReplacement.TrimEnd([char[]]@("`r", "`n")) -Description "Retry Queue 峰值汇总字段"

# 6) A drained run can still be an invalid performance run if it produced a DLQ event
# or the audit consumer had to retry its persistence. Save artifacts first, then stop the matrix.
$qualityAnchor = @'
            if ($exitCode -ne 0 -and $exitCode -ne 99) {
                throw "k6 返回非阈值失败退出码 $exitCode。已保存本轮数据和排空结果，停止后续实验：$runId"
            }
'@
$qualityReplacement = @'
            if ($exitCode -ne 0 -and $exitCode -ne 99) {
                throw "k6 返回非阈值失败退出码 $exitCode。已保存本轮数据和排空结果，停止后续实验：$runId"
            }
            if ($null -eq $deadLetterPublishedDelta -or $null -eq $deadLetterAuditFailuresDelta) {
                throw "本轮死信 Counter 快照不可用；该轮结果不能纳入基线，停止后续矩阵：$runId"
            }
            if ($deadLetterPublishedDelta -gt 0 -or $deadLetterAuditFailuresDelta -gt 0) {
                throw "本轮虽然完成排空，但产生死信/审计失败：published=$deadLetterPublishedDelta, auditFailures=$deadLetterAuditFailuresDelta。该轮标记为异常并停止后续矩阵：$runId"
            }
'@
$content = Replace-TextOnce -Text $content -Old $qualityAnchor.TrimEnd([char[]]@("`r", "`n")) -New $qualityReplacement.TrimEnd([char[]]@("`r", "`n")) -Description "死信质量门禁"

# Final guard: confirm all expected changes exist before writing the output.
$expectedMarkers = @(
    "community_like_rabbitmq_retry_depth",
    "community_like_rabbitmq_dead_letter_published_total",
    "community_like_rabbitmq_dead_letter_audit_persisted_total",
    "community_like_rabbitmq_dead_letter_audit_failures_total",
    "waiting_for_dead_letter_audit_consumer",
    "dead_letter_audit_pending_delta",
    "dead_letter_clean_run"
)
foreach ($marker in $expectedMarkers) {
    if (-not $content.Contains($marker)) { throw "生成前检查失败，缺少预期更新标记：$marker。没有写入输出文件。" }
}

# Write a new file; never modify the user's original benchmark in-place.
$outputDirectory = Split-Path -Parent $OutputPath
if ($outputDirectory -and -not (Test-Path -LiteralPath $outputDirectory)) {
    New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null
}
$encoding = New-Object System.Text.UTF8Encoding($true)
[System.IO.File]::WriteAllText($OutputPath, $content, $encoding)

Write-Host "已生成死信感知版 Benchmark：$OutputPath" -ForegroundColor Green
Write-Host "原始脚本未被修改：$SourcePath" -ForegroundColor Cyan
Write-Host "修改点：Preflight 指标、Retry Queue 排空、DLQ 审计追平、每轮死信增量、死信质量门禁。" -ForegroundColor Cyan
