[CmdletBinding()]
param(
    [string]$ProjectPath = "C:\Users\ZMT\Desktop\project\picture-backend-pro",
    [string]$BaseUrl = "http://localhost:8123/api",
    [string]$K6Script = ".\k6\community_like_mq_benchmark.js",
    [string]$K6Cores = "8-13",
    [string]$Duration = "60s",
    [int]$PreVus = 200,
    [int]$MaxVus = 400,
    [int[]]$Rates = @(100, 300, 500, 800, 1000),
    [int]$Repetitions = 3,
    [int]$SampleIntervalSeconds = 2,
    [int]$DockerSampleIntervalSeconds = 5,
    [int]$BaselineSeconds = 15,
    [int]$DrainTimeoutSeconds = 300,
    [int]$DrainStableSamples = 5,
    [int]$DrainPollSeconds = 2,
    [int]$CooldownSeconds = 5,
    [int]$JavaPid = 0,
    [string]$TargetType = "POST",
    [string]$SessionCookieName = "satoken",
    [string]$SetupTimeout = "5m",
    [string]$RequestTimeout = "10s",
    [string]$OutputRoot = ".\benchmark-results",
    [string]$AppLogPath = "",
    [int]$RandomSeed = 0,
    [switch]$SkipCpuAffinity,
    [switch]$SkipDockerStats
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0

# ------------------------------
# Helpers
# ------------------------------
function Write-CsvRows {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][object[]]$Rows
    )
    $items = @($Rows)
    if ($items.Count -eq 0) { return }
    $parent = Split-Path -Parent $Path
    if ($parent -and -not (Test-Path $parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    if (Test-Path $Path) {
        $items | Export-Csv -Path $Path -NoTypeInformation -Append -Encoding UTF8
    }
    else {
        $items | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
    }
}

function Write-CsvRow {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][object]$Row
    )
    Write-CsvRows -Path $Path -Rows @($Row)
}

function Get-Sha256 {
    param([string]$Path)
    if (-not (Test-Path $Path)) { return $null }
    return (Get-FileHash -Algorithm SHA256 -Path $Path).Hash
}

function Get-ProcessorMask {
    param([Parameter(Mandatory = $true)][string]$Cores)
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

function Get-QuotedArgument {
    param([string]$Value)
    # Start-Process receives one Windows command-line string; quote every argument.
    $escaped = $Value.Replace('"', '\"')
    return '"' + $escaped + '"'
}

function Get-ShuffledItems {
    param(
        [object[]]$Items,
        [System.Random]$Random
    )
    $list = [System.Collections.Generic.List[object]]::new()
    foreach ($item in $Items) { $list.Add($item) }
    for ($i = $list.Count - 1; $i -gt 0; $i--) {
        $j = $Random.Next(0, $i + 1)
        $temp = $list[$i]
        $list[$i] = $list[$j]
        $list[$j] = $temp
    }
    return ,$list.ToArray()
}

function Get-ObjectPropertyValue {
    param(
        [object]$Object,
        [string]$Name,
        [object]$Default = $null
    )
    if ($null -eq $Object) { return $Default }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) { return $Default }
    return $property.Value
}

function Get-SummaryMetric {
    param(
        [object]$Summary,
        [string]$MetricName
    )
    $metrics = Get-ObjectPropertyValue -Object $Summary -Name "metrics"
    if ($null -eq $metrics) { return $null }
    $property = $metrics.PSObject.Properties[$MetricName]
    if ($null -eq $property) { return $null }
    return $property.Value
}

function Get-SummaryValue {
    param(
        [object]$Summary,
        [string[]]$MetricNames,
        [string]$ValueName
    )
    foreach ($metricName in $MetricNames) {
        $metric = Get-SummaryMetric -Summary $Summary -MetricName $metricName
        if ($null -eq $metric) { continue }

        # k6 --summary-export stores metric fields directly under metrics.<name>
        # (for example: metrics.iterations.count), not under a nested "values" object.
        $value = Get-ObjectPropertyValue -Object $metric -Name $ValueName
        if ($null -ne $value) { return $value }
    }
    return $null
}

function ConvertTo-DurationSeconds {
    param([Parameter(Mandatory = $true)][string]$Duration)

    $durationMatches = [regex]::Matches($Duration, '(?<value>\d+(?:\.\d+)?)(?<unit>ms|s|m|h)')
    if ($durationMatches.Count -eq 0) { return 0.0 }

    $totalSeconds = 0.0
    foreach ($durationMatch in $durationMatches) {
        $number = [double]::Parse($durationMatch.Groups["value"].Value, [Globalization.CultureInfo]::InvariantCulture)
        switch ($durationMatch.Groups["unit"].Value) {
            "ms" { $totalSeconds += ($number / 1000.0) }
            "s"  { $totalSeconds += $number }
            "m"  { $totalSeconds += ($number * 60.0) }
            "h"  { $totalSeconds += ($number * 3600.0) }
        }
    }
    return $totalSeconds
}

function Get-PrometheusText {
    param([string]$Phase = "sample")
    $uri = "$($script:BaseUrl.TrimEnd('/'))/actuator/prometheus"
    try {
        return (Invoke-WebRequest -UseBasicParsing -Uri $uri -TimeoutSec 8).Content
    }
    catch {
        $message = $_.Exception.Message
        Write-CsvRow -Path (Join-Path $script:CurrentRunDir "telemetry-errors.csv") -Row ([pscustomobject]@{
            timestamp_utc = [DateTime]::UtcNow.ToString("o")
            phase = $Phase
            source = "actuator_prometheus"
            error = $message
        })
        Write-Warning "Prometheus 抓取失败（$Phase）：$message"
        return $null
    }
}

function Convert-PrometheusTextToMap {
    param([string]$Text)
    $map = @{}
    if ([string]::IsNullOrWhiteSpace($Text)) { return $map }
    $numberPattern = '(?<value>[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?|NaN|[+-]?Inf)'
    $linePattern = '^(?<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?<labels>\{.*\})?\s+' + $numberPattern + '(?:\s+\d+)?\s*$'
    foreach ($line in ($Text -split "`r?`n")) {
        if ($line.StartsWith("#") -or [string]::IsNullOrWhiteSpace($line)) { continue }
        if ($line -match $linePattern) {
            $name = $Matches["name"]
            $rawValue = $Matches["value"]
            if ($rawValue -in @("NaN", "Inf", "+Inf", "-Inf")) { continue }
            $value = [double]::Parse($rawValue, [Globalization.CultureInfo]::InvariantCulture)
            if ($map.ContainsKey($name)) { $map[$name] += $value } else { $map[$name] = $value }
        }
    }
    return $map
}

function Write-PrometheusSample {
    param(
        [string]$Phase,
        [string]$RunId,
        [string]$Mode,
        [int]$Rate,
        [int]$Repetition
    )
    $text = Get-PrometheusText -Phase $Phase
    if ($null -eq $text) { return $null }
    $timestamp = [DateTime]::UtcNow.ToString("o")
    $linePattern = '^(?<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?<labels>\{.*\})?\s+(?<value>[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?|NaN|[+-]?Inf)(?:\s+\d+)?\s*$'
    $sampleRows = [System.Collections.Generic.List[object]]::new()
    foreach ($line in ($text -split "`r?`n")) {
        if ($line.StartsWith("#") -or [string]::IsNullOrWhiteSpace($line)) { continue }
        if ($line -match $linePattern) {
            $sampleRows.Add([pscustomobject]@{
                timestamp_utc = $timestamp
                run_id = $RunId
                phase = $Phase
                mode = $Mode
                target_rps = $Rate
                repetition = $Repetition
                metric_name = $Matches["name"]
                labels = $Matches["labels"]
                value = $Matches["value"]
            })
        }
    }
    Write-CsvRows -Path (Join-Path $script:CurrentRunDir "prometheus-timeseries.csv") -Rows $sampleRows.ToArray()
    return (Convert-PrometheusTextToMap -Text $text)
}

function Get-PromValue {
    param(
        [hashtable]$MetricMap,
        [string]$MetricName
    )
    if ($null -eq $MetricMap -or -not $MetricMap.ContainsKey($MetricName)) { return $null }
    return [double]$MetricMap[$MetricName]
}

function Get-ProcessCpuData {
    param(
        [int]$ProcessId,
        [DateTime]$TimestampUtc
    )
    if ($ProcessId -le 0) {
        return [pscustomobject]@{ cpu_seconds = $null; cpu_percent_one_core = $null; cpu_percent_machine = $null; working_set_mb = $null }
    }
    try {
        $proc = Get-Process -Id $ProcessId -ErrorAction Stop
        $cpuSeconds = $proc.TotalProcessorTime.TotalSeconds
        $workingSet = [math]::Round($proc.WorkingSet64 / 1MB, 2)
        $cpuOneCore = $null
        $cpuMachine = $null
        if ($script:PreviousProcessSamples.ContainsKey([string]$ProcessId)) {
            $previous = $script:PreviousProcessSamples[[string]$ProcessId]
            $elapsed = ($TimestampUtc - $previous.timestamp).TotalSeconds
            if ($elapsed -gt 0) {
                $delta = [math]::Max(0, $cpuSeconds - $previous.cpu_seconds)
                $cpuOneCore = [math]::Round(100 * $delta / $elapsed, 2)
                $cpuMachine = [math]::Round(100 * $delta / ($elapsed * $script:LogicalProcessorCount), 2)
            }
        }
        $script:PreviousProcessSamples[[string]$ProcessId] = @{ timestamp = $TimestampUtc; cpu_seconds = $cpuSeconds }
        return [pscustomobject]@{
            cpu_seconds = [math]::Round($cpuSeconds, 3)
            cpu_percent_one_core = $cpuOneCore
            cpu_percent_machine = $cpuMachine
            working_set_mb = $workingSet
        }
    }
    catch {
        return [pscustomobject]@{ cpu_seconds = $null; cpu_percent_one_core = $null; cpu_percent_machine = $null; working_set_mb = $null }
    }
}

function Write-HostSample {
    param(
        [string]$Phase,
        [string]$RunId,
        [string]$Mode,
        [int]$Rate,
        [int]$Repetition,
        [int]$K6ProcessId
    )
    $now = [DateTime]::UtcNow
    $cpuTotal = $null
    $freeMb = $null
    $memoryUsedPct = $null
    try {
        $cpuObject = Get-CimInstance -ClassName Win32_PerfFormattedData_PerfOS_Processor -Filter "Name='_Total'" -ErrorAction Stop
        if ($cpuObject) { $cpuTotal = [double]$cpuObject.PercentProcessorTime }
    }
    catch { }
    try {
        $os = Get-CimInstance -ClassName Win32_OperatingSystem -ErrorAction Stop
        $freeMb = [math]::Round($os.FreePhysicalMemory / 1024, 2)
        if ([double]$os.TotalVisibleMemorySize -gt 0) {
            $memoryUsedPct = [math]::Round(100 * (1 - ($os.FreePhysicalMemory / $os.TotalVisibleMemorySize)), 2)
        }
    }
    catch { }
    $k6 = Get-ProcessCpuData -ProcessId $K6ProcessId -TimestampUtc $now
    $java = Get-ProcessCpuData -ProcessId $script:JavaPid -TimestampUtc $now
    Write-CsvRow -Path (Join-Path $script:CurrentRunDir "host-timeseries.csv") -Row ([pscustomobject]@{
        timestamp_utc = $now.ToString("o")
        run_id = $RunId
        phase = $Phase
        mode = $Mode
        target_rps = $Rate
        repetition = $Repetition
        logical_processors = $script:LogicalProcessorCount
        system_cpu_percent = $cpuTotal
        memory_used_percent = $memoryUsedPct
        memory_available_mb = $freeMb
        k6_pid = if ($K6ProcessId -gt 0) { $K6ProcessId } else { $null }
        k6_cpu_percent_one_core = $k6.cpu_percent_one_core
        k6_cpu_percent_machine = $k6.cpu_percent_machine
        k6_working_set_mb = $k6.working_set_mb
        java_pid = if ($script:JavaPid -gt 0) { $script:JavaPid } else { $null }
        java_cpu_percent_one_core = $java.cpu_percent_one_core
        java_cpu_percent_machine = $java.cpu_percent_machine
        java_working_set_mb = $java.working_set_mb
    })
}

function Write-DockerStats {
    param(
        [string]$Phase,
        [string]$RunId,
        [string]$Mode,
        [int]$Rate,
        [int]$Repetition
    )
    if ($script:SkipDockerStats -or -not $script:DockerAvailable) { return }
    try {
        $output = & $script:DockerExe stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}|{{.NetIO}}|{{.BlockIO}}' 2>$null
        if ($LASTEXITCODE -ne 0) { return }
        $timestamp = [DateTime]::UtcNow.ToString("o")
        foreach ($line in $output) {
            $parts = ([string]$line).Split('|')
            if ($parts.Count -lt 6) { continue }
            Write-CsvRow -Path (Join-Path $script:CurrentRunDir "docker-stats.csv") -Row ([pscustomobject]@{
                timestamp_utc = $timestamp
                run_id = $RunId
                phase = $Phase
                mode = $Mode
                target_rps = $Rate
                repetition = $Repetition
                container = $parts[0]
                cpu_percent = $parts[1]
                memory_usage = $parts[2]
                memory_percent = $parts[3]
                network_io = $parts[4]
                block_io = $parts[5]
            })
        }
    }
    catch { }
}

function Capture-TelemetrySample {
    param(
        [string]$Phase,
        [string]$RunId,
        [string]$Mode,
        [int]$Rate,
        [int]$Repetition,
        [int]$K6ProcessId = 0
    )
    $map = Write-PrometheusSample -Phase $Phase -RunId $RunId -Mode $Mode -Rate $Rate -Repetition $Repetition
    Write-HostSample -Phase $Phase -RunId $RunId -Mode $Mode -Rate $Rate -Repetition $Repetition -K6ProcessId $K6ProcessId
    $now = [DateTime]::UtcNow
    $shouldDocker = $true
    if ($script:LastDockerSampleUtc) {
        $shouldDocker = (($now - $script:LastDockerSampleUtc).TotalSeconds -ge $script:DockerSampleIntervalSeconds)
    }
    if ($shouldDocker) {
        Write-DockerStats -Phase $Phase -RunId $RunId -Mode $Mode -Rate $Rate -Repetition $Repetition
        $script:LastDockerSampleUtc = $now
    }
    return $map
}

function Resolve-JavaPid {
    if ($script:RequestedJavaPid -gt 0) { return $script:RequestedJavaPid }
    try {
        $candidates = Get-CimInstance Win32_Process -Filter "Name='java.exe' OR Name='javaw.exe'" -ErrorAction Stop
        $matchingProcesses = @($candidates | Where-Object {
            $_.CommandLine -and ($_.CommandLine -match 'picture-backend-pro|PictureBackendProApplication|com\.zmt\.picturebackendpro')
        })
        if ($matchingProcesses.Count -eq 1) { return [int]$matchingProcesses[0].ProcessId }
        if ($matchingProcesses.Count -gt 1) {
            Write-Warning "发现多个可能的项目 Java 进程：$($matchingProcesses.ProcessId -join ', ')。如需采集本机 Java 进程 CPU，请用 -JavaPid 指定。"
        }
    }
    catch { }
    return 0
}

function Get-SystemInformation {
    $cpuName = $null
    $coreCount = $null
    $logicalCount = [Environment]::ProcessorCount
    $ramGb = $null
    $osCaption = $null
    $osVersion = $null
    try {
        $processors = @(Get-CimInstance Win32_Processor)
        if ($processors.Count -gt 0) {
            $cpuName = (($processors | Select-Object -ExpandProperty Name -Unique) -join " | ")
            $coreCount = ($processors | Measure-Object -Property NumberOfCores -Sum).Sum
            $logicalCount = ($processors | Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum
        }
    }
    catch { }
    try {
        $computer = Get-CimInstance Win32_ComputerSystem
        $ramGb = [math]::Round($computer.TotalPhysicalMemory / 1GB, 2)
    }
    catch { }
    try {
        $os = Get-CimInstance Win32_OperatingSystem
        $osCaption = $os.Caption
        $osVersion = "$($os.Version) build $($os.BuildNumber)"
    }
    catch { }
    return [pscustomobject]@{
        os_caption = $osCaption
        os_version = $osVersion
        cpu_name = $cpuName
        physical_core_count = $coreCount
        logical_processor_count = $logicalCount
        ram_gb = $ramGb
        powershell_version = $PSVersionTable.PSVersion.ToString()
        current_culture = [Globalization.CultureInfo]::CurrentCulture.Name
        is_64bit_process = [Environment]::Is64BitProcess
    }
}

function Get-CommandVersionText {
    param([string]$Command, [string[]]$Arguments)
    try {
        $result = & $Command @Arguments 2>&1 | Out-String
        return $result.Trim()
    }
    catch { return "not_available: $($_.Exception.Message)" }
}

function Get-GitInformation {
    $git = Get-Command git -ErrorAction SilentlyContinue
    if (-not $git) { return [pscustomobject]@{ commit = $null; branch = $null; dirty = $null; status = "git_not_found" } }
    try {
        $commit = (& git -C $script:ProjectPath rev-parse HEAD 2>$null | Out-String).Trim()
        $branch = (& git -C $script:ProjectPath branch --show-current 2>$null | Out-String).Trim()
        $statusLines = @(& git -C $script:ProjectPath status --short 2>$null)
        return [pscustomobject]@{
            commit = $commit
            branch = $branch
            dirty = ($statusLines.Count -gt 0)
            status = ($statusLines -join "`n")
        }
    }
    catch { return [pscustomobject]@{ commit = $null; branch = $null; dirty = $null; status = $_.Exception.Message } }
}

function Test-ApiPreflight {
    $healthUrl = "$($script:BaseUrl.TrimEnd('/'))/actuator/health"
    $promUrl = "$($script:BaseUrl.TrimEnd('/'))/actuator/prometheus"
    try {
        $health = Invoke-RestMethod -Uri $healthUrl -TimeoutSec 8
        $health | ConvertTo-Json -Depth 10 | Set-Content -Path (Join-Path $script:SessionDir "preflight-health.json") -Encoding UTF8
        if ($health.status -and $health.status -ne "UP") { throw "Actuator health 状态不是 UP：$($health.status)" }
    }
    catch { throw "应用健康检查失败：$healthUrl；$($_.Exception.Message)" }
    try {
        $promText = (Invoke-WebRequest -UseBasicParsing -Uri $promUrl -TimeoutSec 8).Content
    }
    catch { throw "Prometheus endpoint 不可访问：$promUrl；$($_.Exception.Message)" }

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
    $map = Convert-PrometheusTextToMap -Text $promText
    $missing = @($required | Where-Object { -not $map.ContainsKey($_) })
    if ($missing.Count -gt 0) {
        throw "Prometheus 缺少积压排空必需指标：$($missing -join ', ')。请确认应用使用 RabbitMQ 模式且指标注册成功。"
    }
    $bad = @($required | Where-Object { [double]$map[$_] -lt 0 })
    if ($bad.Count -gt 0) {
        throw "以下队列/积压指标返回负值，通常表示 RabbitAdmin 查询失败，不能开始正式压测：$($bad -join ', ')"
    }
    return [pscustomobject]@{ health_url = $healthUrl; prometheus_url = $promUrl; required_metrics = $required; health = $health.status }
}

function Get-MetricSummary {
    param(
        [string]$Path,
        [string]$MetricName,
        [string]$Suffix
    )
    if (-not (Test-Path $Path)) { return $null }
    $rows = @(Import-Csv $Path | Where-Object { $_.phase -eq "load" -and $_.metric_name -eq $MetricName })
    if ($rows.Count -eq 0) { return $null }
    $numbers = @($rows | ForEach-Object {
        $d = 0.0
        if ([double]::TryParse($_.value, [Globalization.NumberStyles]::Float, [Globalization.CultureInfo]::InvariantCulture, [ref]$d)) { $d }
    })
    if ($numbers.Count -eq 0) { return $null }
    if ($Suffix -eq "avg") { return [math]::Round(($numbers | Measure-Object -Average).Average, 3) }
    if ($Suffix -eq "max") { return [math]::Round(($numbers | Measure-Object -Maximum).Maximum, 3) }
    if ($Suffix -eq "min") { return [math]::Round(($numbers | Measure-Object -Minimum).Minimum, 3) }
    return $null
}

function Get-CounterDelta {
    param(
        [hashtable]$Before,
        [hashtable]$After,
        [string]$MetricName
    )
    $a = Get-PromValue -MetricMap $Before -MetricName $MetricName
    $b = Get-PromValue -MetricMap $After -MetricName $MetricName
    if ($null -eq $a -or $null -eq $b) { return $null }
    return [math]::Round(($b - $a), 3)
}

function Get-K6SummaryFields {
    param(
        [string]$SummaryPath,
        [string]$LoadDuration
    )

    $summary = Get-Content -Raw -Path $SummaryPath | ConvertFrom-Json
    $likeReqMetricName = "http_reqs{name:like_toggle}"
    $likeDurationMetricName = "http_req_duration{name:like_toggle}"

    $iterations = Get-SummaryValue -Summary $summary -MetricNames @("iterations") -ValueName "count"
    $durationSeconds = ConvertTo-DurationSeconds -Duration $LoadDuration

    # iterations.rate in k6 summary includes setup() time. For load RPS, divide
    # iteration count by the configured arrival-rate scenario duration instead.
    $actualRps = $null
    if ($null -ne $iterations -and $durationSeconds -gt 0) {
        $actualRps = [math]::Round(([double]$iterations / $durationSeconds), 3)
    }

    $requestCount = if ($null -ne $iterations) {
        $iterations
    }
    else {
        Get-SummaryValue -Summary $summary -MetricNames @($likeReqMetricName, "http_reqs") -ValueName "count"
    }

    $p95Source = if (Get-SummaryMetric -Summary $summary -MetricName $likeDurationMetricName) {
        $likeDurationMetricName
    }
    else {
        "http_req_duration (fallback; may include setup requests)"
    }

    $requestCountSource = if ($null -ne $iterations) {
        "iterations.count (one like API request per iteration)"
    }
    elseif (Get-SummaryMetric -Summary $summary -MetricName $likeReqMetricName) {
        $likeReqMetricName
    }
    else {
        "http_reqs (fallback; may include setup requests)"
    }

    $p95 = Get-SummaryValue -Summary $summary -MetricNames @($likeDurationMetricName, "http_req_duration") -ValueName "p(95)"
    $p99 = Get-SummaryValue -Summary $summary -MetricNames @($likeDurationMetricName, "http_req_duration") -ValueName "p(99)"
    $avg = Get-SummaryValue -Summary $summary -MetricNames @($likeDurationMetricName, "http_req_duration") -ValueName "avg"
    $max = Get-SummaryValue -Summary $summary -MetricNames @($likeDurationMetricName, "http_req_duration") -ValueName "max"

    # k6 Rate metrics export "value" (0..1); "passes" is the count of true samples.
    $httpFailureRate = Get-SummaryValue -Summary $summary -MetricNames @("http_req_failed{name:like_toggle}", "http_req_failed") -ValueName "value"
    $businessFailureRate = Get-SummaryValue -Summary $summary -MetricNames @("business_failures") -ValueName "value"
    $businessFailureCount = Get-SummaryValue -Summary $summary -MetricNames @("business_failures") -ValueName "passes"
    $dropped = Get-SummaryValue -Summary $summary -MetricNames @("dropped_iterations") -ValueName "count"

    return [pscustomobject]@{
        actual_rps = $actualRps
        actual_rps_metric = "iterations.count / configured_load_duration_seconds"
        like_http_requests = $requestCount
        like_http_requests_metric = $requestCountSource
        http_req_duration_avg_ms = $avg
        http_req_duration_p95_ms = $p95
        http_req_duration_percentile_source = $p95Source
        http_req_duration_p99_ms = $p99
        http_req_duration_max_ms = $max
        http_req_failed_rate = $httpFailureRate
        business_failure_rate = $businessFailureRate
        business_failure_count = $businessFailureCount
        iterations = $iterations
        dropped_iterations = $dropped
        summary = $summary
    }
}

function Test-LoadMarkerInLogs {
    param([string[]]$Paths)

    foreach ($path in $Paths) {
        if (-not (Test-Path $path)) { continue }
        try {
            $content = Get-Content -Raw -Path $path -ErrorAction Stop
            if ($content -match "BENCHMARK_LOAD_STARTED") { return $true }
        }
        catch { }
    }
    return $false
}

function Get-K6FallbackExitCode {
    param(
        [string]$SummaryPath,
        [string]$StdoutPath
    )

    # PowerShell 5.1 may fail to expose ExitCode for a Process object in some
    # launcher/shim configurations. Infer only when k6 printed a complete
    # TOTAL RESULTS section and all benchmark thresholds are evaluable.
    if (-not (Test-Path $SummaryPath) -or -not (Test-Path $StdoutPath)) { return $null }

    $stdout = Get-Content -Raw -Path $StdoutPath
    if ($stdout -notmatch "TOTAL RESULTS") { return $null }

    try {
        $summary = Get-Content -Raw -Path $SummaryPath | ConvertFrom-Json
        $businessRate = Get-SummaryValue -Summary $summary -MetricNames @("business_failures") -ValueName "value"
        $httpFailureRate = Get-SummaryValue -Summary $summary -MetricNames @("http_req_failed{name:like_toggle}", "http_req_failed") -ValueName "value"
        $dropped = Get-SummaryValue -Summary $summary -MetricNames @("dropped_iterations") -ValueName "count"
        $p95 = Get-SummaryValue -Summary $summary -MetricNames @("http_req_duration{name:like_toggle}", "http_req_duration") -ValueName "p(95)"

        $p99 = $null
        if ($stdout -match 'p\(99\)=([0-9]+(?:\.[0-9]+)?)ms') {
            $p99 = [double]::Parse($Matches[1], [Globalization.CultureInfo]::InvariantCulture)
        }

        if ($null -eq $businessRate -or $null -eq $httpFailureRate -or
            $null -eq $dropped -or $null -eq $p95 -or $null -eq $p99) {
            return $null
        }

        # These are the benchmark k6 thresholds in community_like_mq_benchmark.js:
        # business_failures < 1%, http_req_failed < 1%, dropped_iterations == 0,
        # like endpoint p95 < 500ms, like endpoint p99 < 1000ms.
        $failed = ([double]$businessRate -ge 0.01) -or
                  ([double]$httpFailureRate -ge 0.01) -or
                  ([double]$dropped -gt 0) -or
                  ([double]$p95 -ge 500.0) -or
                  ([double]$p99 -ge 1000.0)

        if ($failed) { return 99 }
        return 0
    }
    catch {
        return $null
    }
}

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
function Scrub-K6Log {
    param([string]$Path, [object[]]$Credentials)
    if (-not (Test-Path $Path)) { return }
    try {
        $content = Get-Content -Raw -Path $Path
        foreach ($credential in $Credentials) {
            $account = [string](Get-ObjectPropertyValue -Object $credential -Name "userAccount" -Default "")
            if (-not [string]::IsNullOrWhiteSpace($account)) {
                $content = $content.Replace($account, "[REDACTED_TEST_ACCOUNT]")
            }
        }
        Set-Content -Path $Path -Value $content -Encoding UTF8
    }
    catch { Write-Warning "日志脱敏失败：$Path；$($_.Exception.Message)" }
}

# ------------------------------
# Main validation and preparation
# ------------------------------
if (-not (Test-Path $ProjectPath)) { throw "找不到项目目录：$ProjectPath" }
$script:ProjectPath = (Resolve-Path $ProjectPath).Path
Set-Location $script:ProjectPath
$script:BaseUrl = $BaseUrl
$script:DrainTimeoutSeconds = $DrainTimeoutSeconds
$script:DrainStableSamples = $DrainStableSamples
$script:DrainPollSeconds = $DrainPollSeconds
$script:RequestedJavaPid = $JavaPid
$script:SkipDockerStats = [bool]$SkipDockerStats
$script:LogicalProcessorCount = [Environment]::ProcessorCount
$script:PreviousProcessSamples = @{}
$script:LastDockerSampleUtc = $null

if ($Repetitions -lt 1) { throw "Repetitions 必须大于等于 1。" }
if ($SampleIntervalSeconds -lt 1) { throw "SampleIntervalSeconds 不能小于 1。" }
if ($Rates.Count -lt 1 -or @($Rates | Where-Object { $_ -le 0 }).Count -gt 0) { throw "Rates 必须全部为正整数。" }
if ($MaxVus -lt $PreVus -or $PreVus -le 0) { throw "请确保 PreVus > 0 且 MaxVus >= PreVus。" }
if ($TargetType -notin @("POST", "PICTURE")) { throw "TargetType 只能是 POST 或 PICTURE。" }

$scriptFile = Join-Path $script:ProjectPath $K6Script
if (-not (Test-Path $scriptFile)) { throw "找不到 k6 脚本：$scriptFile。请先把压测包中的 k6\community_like_mq_benchmark.js 复制到项目的 k6 目录，或用 -K6Script 指定已有脚本。" }
$k6ExeCommand = Get-Command k6 -ErrorAction Stop
$script:K6Exe = $k6ExeCommand.Source
$existingK6Processes = @(Get-Process -Name "k6" -ErrorAction SilentlyContinue)
if ($existingK6Processes.Count -gt 0) {
    throw "检测到已有 k6 进程（PID: $($existingK6Processes.Id -join ', ')）。请先确认没有其它压测正在运行，避免多个负载发生重叠。"
}

$usersFile = if ($env:USERS_FILE) { $env:USERS_FILE } else { ".\k6\users.local.json" }
$targetsFile = if ($env:TARGETS_FILE) { $env:TARGETS_FILE } else { ".\k6\targets.local.json" }
$usersPath = if ([IO.Path]::IsPathRooted($usersFile)) { $usersFile } else { Join-Path $script:ProjectPath $usersFile }
$targetsPath = if ([IO.Path]::IsPathRooted($targetsFile)) { $targetsFile } else { Join-Path $script:ProjectPath $targetsFile }
if (-not (Test-Path $usersPath)) { throw "找不到测试账号文件：$usersPath" }
if (-not (Test-Path $targetsPath)) { throw "找不到测试目标文件：$targetsPath" }

try {
    $credentialData = Get-Content -Raw -Path $usersPath | ConvertFrom-Json
    $targetData = Get-Content -Raw -Path $targetsPath | ConvertFrom-Json
}
catch { throw "测试账号或目标 JSON 格式错误：$($_.Exception.Message)" }
if (@($credentialData).Count -lt $MaxVus) { throw "测试账号数 $(@($credentialData).Count) 小于 MaxVus=$MaxVus；当前 k6 脚本要求账号数至少等于 MAX_VUS。" }
if (-not $targetData.hotTargetId -or @($targetData.targetIds).Count -lt 2) { throw "targets.local.json 必须提供 hotTargetId 和至少两个 targetIds。" }

$env:BASE_URL = $BaseUrl
$env:TARGET_TYPE = $TargetType
$env:PRE_VUS = "$PreVus"
$env:MAX_VUS = "$MaxVus"
$env:SESSION_COOKIE_NAME = $SessionCookieName
$env:USERS_FILE = $usersPath
$env:TARGETS_FILE = $targetsPath
$env:SETUP_TIMEOUT = $SetupTimeout
$env:REQUEST_TIMEOUT = $RequestTimeout
$env:K6_NO_COLOR = "1"

$timestampName = Get-Date -Format "yyyyMMdd-HHmmss"
$outputRootPath = if ([IO.Path]::IsPathRooted($OutputRoot)) { $OutputRoot } else { Join-Path $script:ProjectPath $OutputRoot }
New-Item -ItemType Directory -Force -Path $outputRootPath | Out-Null
$script:SessionDir = Join-Path $outputRootPath "community-like-benchmark-$timestampName"
New-Item -ItemType Directory -Force -Path $script:SessionDir | Out-Null
$script:ResolvedAppLogPath = $null
if (-not [string]::IsNullOrWhiteSpace($AppLogPath)) {
    $script:ResolvedAppLogPath = if ([IO.Path]::IsPathRooted($AppLogPath)) { $AppLogPath } else { Join-Path $script:ProjectPath $AppLogPath }
    if (-not (Test-Path $script:ResolvedAppLogPath)) {
        Write-Warning "指定的 AppLogPath 当前不存在：$script:ResolvedAppLogPath；将不复制应用日志。"
        $script:ResolvedAppLogPath = $null
    }
}

$script:JavaPid = Resolve-JavaPid
$systemInfo = Get-SystemInformation
$script:LogicalProcessorCount = [int]$systemInfo.logical_processor_count
if ($script:LogicalProcessorCount -lt 1) { $script:LogicalProcessorCount = [Environment]::ProcessorCount }
$gitInfo = Get-GitInformation
$k6Version = Get-CommandVersionText -Command $script:K6Exe -Arguments @("version")
$javaVersion = Get-CommandVersionText -Command "java" -Arguments @("-version")
$dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
$script:DockerAvailable = [bool]$dockerCommand
$script:DockerExe = if ($dockerCommand) { $dockerCommand.Source } else { $null }
$dockerVersion = if ($script:DockerAvailable) { Get-CommandVersionText -Command $script:DockerExe -Arguments @("version", "--format", "{{.Server.Version}}") } else { "not_available" }

$script:K6Cores = $K6Cores
$script:SkipCpuAffinity = [bool]$SkipCpuAffinity
$affinityMask = $null
if (-not $SkipCpuAffinity) {
    try {
        $affinityMask = Get-ProcessorMask -Cores $K6Cores
        if ($affinityMask -eq 0) { throw "CPU affinity mask is zero。" }
    }
    catch {
        Write-Warning "CPU 绑核配置无效，将不绑核：$($_.Exception.Message)"
        $script:SkipCpuAffinity = $true
    }
}

$preflight = Test-ApiPreflight
$seed = if ($RandomSeed -gt 0) { $RandomSeed } else { Get-Random -Minimum 100000 -Maximum 2147483000 }
$rng = [System.Random]::new([int]$seed)

$manifest = [pscustomobject]@{
    created_at_local = (Get-Date).ToString("o")
    created_at_utc = [DateTime]::UtcNow.ToString("o")
    project_name = (Split-Path $script:ProjectPath -Leaf)
    git = $gitInfo
    system = $systemInfo
    k6_version = $k6Version
    java_version = $javaVersion
    docker_server_version = $dockerVersion
    load_generator_java_pid = $script:JavaPid
    application_base_url = $BaseUrl
    actuator_health = $preflight
    k6_script_relative_path = $K6Script
    k6_script_sha256 = Get-Sha256 -Path $scriptFile
    users_file_sha256 = Get-Sha256 -Path $usersPath
    targets_file_sha256 = Get-Sha256 -Path $targetsPath
    # Intentionally do not copy users.local.json / targets.local.json: they contain local test credentials and data.
    target_type = $TargetType
    session_cookie_name = $SessionCookieName
    duration = $Duration
    pre_vus = $PreVus
    max_vus = $MaxVus
    rates = $Rates
    modes = @(
        @{ mode = "single"; distribution = "100% hotspot"; hot_ratio = $null },
        @{ mode = "multi"; distribution = "VU-to-target assignment across targetIds"; hot_ratio = $null },
        @{ mode = "mixed"; distribution = "80% hotspot / 20% other targets"; hot_ratio = "0.8" }
    )
    repetitions = $Repetitions
    randomized_order_seed = $seed
    sample_interval_seconds = $SampleIntervalSeconds
    docker_sample_interval_seconds = $DockerSampleIntervalSeconds
    baseline_seconds = $BaselineSeconds
    cpu_affinity = if ($script:SkipCpuAffinity) { "disabled" } else { $K6Cores }
    cpu_affinity_mask_hex = if ($null -ne $affinityMask -and -not $script:SkipCpuAffinity) { "0x{0:X}" -f $affinityMask } else { $null }
    drain_timeout_seconds = $DrainTimeoutSeconds
    drain_stable_samples = $DrainStableSamples
    drain_poll_seconds = $DrainPollSeconds
    cooldown_seconds = $CooldownSeconds
    notes = @(
        "Target RPS is an arrival-rate target, not a guaranteed achieved RPS.",
        "Compare modes on the same hardware/software/configuration; do not claim this local RPS as universal capacity.",
        "Prometheus captures all Actuator metrics, including JVM/process/system, RabbitMQ, consumer, and datasource metrics when exposed.",
        "Host CPU/memory fields describe the machine running this PowerShell/k6 script; backend system CPU can be read from system_cpu_usage in Prometheus if Micrometer exposes it."
    )
}
$manifest | ConvertTo-Json -Depth 20 | Set-Content -Path (Join-Path $script:SessionDir "manifest.json") -Encoding UTF8

# Save simple environment/health captures without exposing local credential files.
Set-Content -Path (Join-Path $script:SessionDir "k6-version.txt") -Value $k6Version -Encoding UTF8
Set-Content -Path (Join-Path $script:SessionDir "java-version.txt") -Value $javaVersion -Encoding UTF8
if ($script:DockerAvailable) { Set-Content -Path (Join-Path $script:SessionDir "docker-version.txt") -Value $dockerVersion -Encoding UTF8 }

Write-Host "实验目录：$script:SessionDir" -ForegroundColor Green
Write-Host "计划：3 种模式 × $($Rates.Count) 档速率 × $Repetitions 次重复 = $((3 * $Rates.Count * $Repetitions)) 组正式压测。" -ForegroundColor Cyan
Write-Host "实验顺序将在每一轮重复内随机化，seed=$seed。"
Write-Host "k6 CPU 绑核：$(if ($script:SkipCpuAffinity) { '关闭' } else { $K6Cores })；Java PID：$(if ($script:JavaPid -gt 0) { $script:JavaPid } else { '未在本机识别（后端指标仍会从 Actuator 采集）' })"

# ------------------------------
# Baseline idle capture
# ------------------------------
$baselineDir = Join-Path $script:SessionDir "baseline"
New-Item -ItemType Directory -Force -Path $baselineDir | Out-Null
$script:CurrentRunDir = $baselineDir
$script:PreviousProcessSamples = @{}
$script:LastDockerSampleUtc = $null
Write-Host "采集压测前基线：${BaselineSeconds}s ..." -ForegroundColor Cyan
$baselineStart = Get-Date
while (((Get-Date) - $baselineStart).TotalSeconds -lt $BaselineSeconds) {
    Capture-TelemetrySample -Phase "baseline" -RunId "BASELINE" -Mode "none" -Rate 0 -Repetition 0 -K6ProcessId 0 | Out-Null
    Start-Sleep -Seconds $SampleIntervalSeconds
}

# ------------------------------
# Execute randomized benchmark matrix
# ------------------------------
$indexPath = Join-Path $script:SessionDir "run-index.csv"
$metricIndexPath = Join-Path $script:SessionDir "k6-metrics.csv"
$runSequence = 0
$abortReason = $null

try {
    for ($rep = 1; $rep -le $Repetitions; $rep++) {
        $planned = @()
        foreach ($mode in @("single", "multi", "mixed")) {
            foreach ($rate in $Rates) {
                $planned += [pscustomobject]@{ mode = $mode; rate = [int]$rate }
            }
        }
        $planned = Get-ShuffledItems -Items $planned -Random $rng
        $orderInRep = 0

        foreach ($case in $planned) {
            $runSequence++
            $orderInRep++
            $mode = [string]$case.mode
            $rate = [int]$case.rate
            $runId = ("R{0:D2}-O{1:D2}-{2}-{3}rps" -f $rep, $orderInRep, $mode, $rate)
            $runDir = Join-Path $script:SessionDir $runId
            New-Item -ItemType Directory -Force -Path $runDir | Out-Null
            $script:CurrentRunDir = $runDir
            $script:PreviousProcessSamples = @{}
            $script:LastDockerSampleUtc = $null

            $env:TARGET_MODE = $mode
            if ($mode -eq "mixed") { $env:HOT_RATIO = "0.8" }
            else { Remove-Item Env:\HOT_RATIO -ErrorAction SilentlyContinue }
            $env:RATE = "$rate"
            $env:DURATION = $Duration
            $env:BASE_URL = $BaseUrl
            $env:PRE_VUS = "$PreVus"
            $env:MAX_VUS = "$MaxVus"
            $env:TARGET_TYPE = $TargetType
            $env:SESSION_COOKIE_NAME = $SessionCookieName
            $env:SETUP_TIMEOUT = $SetupTimeout
            $env:REQUEST_TIMEOUT = $RequestTimeout

            $summaryPath = Join-Path $runDir "k6-summary.json"
            $stdoutPath = Join-Path $runDir "k6-stdout.log"
            $stderrPath = Join-Path $runDir "k6-stderr.log"
            $k6Args = @("run", $scriptFile, "--summary-export", $summaryPath)
            $argumentLine = (($k6Args | ForEach-Object { Get-QuotedArgument ([string]$_) }) -join " ")
            $startTime = Get-Date
            $startUtc = [DateTime]::UtcNow
            Write-Host "[$runSequence/$($Repetitions * 3 * $Rates.Count)] 开始 $runId (target=$rate RPS, duration=$Duration)" -ForegroundColor Yellow

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

            $process = Start-Process -FilePath $script:K6Exe -ArgumentList $argumentLine -WorkingDirectory $script:ProjectPath `
                -PassThru -NoNewWindow -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
            $affinityApplied = $false
            if (-not $script:SkipCpuAffinity) {
                try {
                    $process.ProcessorAffinity = [IntPtr]$affinityMask
                    $affinityApplied = $true
                }
                catch { Write-Warning "k6 已启动，但 CPU 绑核失败：$($_.Exception.Message)" }
            }
            $processId = $process.Id
            $loadMarkerSeen = $false
            $loadStartedAt = $null
            $lastMarkerScan = [DateTime]::MinValue
            while ($true) {
                $process.Refresh()
                $now = Get-Date
                if (-not $loadMarkerSeen -and (($now - $lastMarkerScan).TotalSeconds -ge 1)) {
                    $lastMarkerScan = $now
                    # k6 console.log is written to stderr in this environment,
                    # so check both redirected files rather than stdout only.
                    if (Test-LoadMarkerInLogs -Paths @($stdoutPath, $stderrPath)) {
                        $loadMarkerSeen = $true
                        $loadStartedAt = $now
                        Write-Host "检测到 k6 正式负载开始标记，后续 Prometheus/主机采样归入 load 阶段。" -ForegroundColor Cyan
                    }
                }
                if ($process.HasExited) { break }
                $samplePhase = if ($loadMarkerSeen) { "load" } else { "setup" }
                Capture-TelemetrySample -Phase $samplePhase -RunId $runId -Mode $mode -Rate $rate -Repetition $rep -K6ProcessId $processId | Out-Null
                Start-Sleep -Seconds $SampleIntervalSeconds
            }
            $process.WaitForExit()
            $process.Refresh()
            $exitCode = $null
            try { $exitCode = $process.ExitCode } catch { }
            $process.Dispose()
            if (-not $loadMarkerSeen -and (Test-LoadMarkerInLogs -Paths @($stdoutPath, $stderrPath))) {
                $loadMarkerSeen = $true
                $loadStartedAt = Get-Date
            }
            $endTime = Get-Date
            $endUtc = [DateTime]::UtcNow
            $elapsedSeconds = [math]::Round(($endTime - $startTime).TotalSeconds, 2)
            $setupSeconds = if ($loadStartedAt) { [math]::Round(($loadStartedAt - $startTime).TotalSeconds, 2) } else { $null }

            $afterLoadMap = Write-PrometheusSample -Phase "postload" -RunId $runId -Mode $mode -Rate $rate -Repetition $rep
            if (-not (Test-Path $summaryPath)) {
                throw "k6 没有生成 summary 文件，退出码=$exitCode；查看 $stdoutPath 与 $stderrPath。"
            }
            $k6Summary = Get-K6SummaryFields -SummaryPath $summaryPath -LoadDuration $Duration

            # p(99) is printed in the k6 threshold summary but is not included
            # in this summary-export JSON by the default trend stats.
            try {
                $k6StdoutText = Get-Content -Raw -Path $stdoutPath
                if ($k6StdoutText -match 'p\(99\)=([0-9]+(?:\.[0-9]+)?)ms') {
                    $k6Summary.http_req_duration_p99_ms = [double]::Parse($Matches[1], [Globalization.CultureInfo]::InvariantCulture)
                }
            }
            catch { }

            # Do not archive setup_data: it contains the satoken cookies returned
            # by k6 setup() for all test users.
            if ($k6Summary.summary.PSObject.Properties["setup_data"]) {
                $k6Summary.summary.PSObject.Properties.Remove("setup_data")
                $k6Summary.summary | ConvertTo-Json -Depth 30 | Set-Content -Path $summaryPath -Encoding UTF8
            }

            # If Start-Process did not expose ExitCode, infer only from a complete
            # k6 TOTAL RESULTS output plus all configured benchmark thresholds.
            $exitCodeInferred = $false
            if ($null -eq $exitCode) {
                $fallbackExitCode = Get-K6FallbackExitCode -SummaryPath $summaryPath -StdoutPath $stdoutPath
                if ($null -eq $fallbackExitCode) {
                    throw "无法读取 k6 进程退出码，且无法从完整 summary/阈值输出确认结果；请检查 $stdoutPath 与 $stderrPath。"
                }
                $exitCode = [int]$fallbackExitCode
                $exitCodeInferred = $true
                Write-Warning "PowerShell 未返回 k6 原生退出码；已根据完整 summary 和基准阈值推断退出状态=$exitCode。该状态会在 run-metadata 中标记为 inferred。"
            }

            $k6Summary.summary.metrics.PSObject.Properties | ForEach-Object {
                Write-CsvRow -Path $metricIndexPath -Row ([pscustomobject]@{
                    run_id = $runId
                    repetition = $rep
                    mode = $mode
                    target_rps = $rate
                    metric_name = $_.Name
                    metric_type = (Get-ObjectPropertyValue -Object $_.Value -Name "type")
                    metric_values_json = ($_.Value | ConvertTo-Json -Depth 10 -Compress)
                })
            }

            # Wait until asynchronous RabbitMQ/Redis work has actually drained before the next run.
            $drain = Wait-MqBacklogDrain -RunId $runId -Mode $mode -Rate $rate -Repetition $rep -CounterBefore $beforeMap
            $afterDrainMap = $drain.after_metrics
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

            # Scrub any test-account names that might have appeared in error logs.
            Scrub-K6Log -Path $stdoutPath -Credentials @($credentialData)
            Scrub-K6Log -Path $stderrPath -Credentials @($credentialData)

            $summaryObject = [pscustomobject]@{
                run_id = $runId
                repetition = $rep
                order_in_repetition = $orderInRep
                run_sequence = $runSequence
                mode = $mode
                hot_ratio = if ($mode -eq "mixed") { "0.8" } else { $null }
                target_rps = $rate
                duration = $Duration
                pre_vus = $PreVus
                max_vus = $MaxVus
                target_type = $TargetType
                affinity_requested = if ($script:SkipCpuAffinity) { "disabled" } else { $K6Cores }
                affinity_applied = $affinityApplied
                k6_pid = $processId
                start_local = $startTime.ToString("o")
                end_local = $endTime.ToString("o")
                start_utc = $startUtc.ToString("o")
                end_utc = $endUtc.ToString("o")
                elapsed_seconds_including_setup = $elapsedSeconds
                setup_seconds_until_first_iteration = $setupSeconds
                load_marker_seen = $loadMarkerSeen
                k6_exit_code = $exitCode
                k6_exit_code_inferred = $exitCodeInferred
                k6_thresholds_failed = ($exitCode -eq 99)
                k6_summary_exists = (Test-Path $summaryPath)
                drain_success = $drain.success
                drain_reason = $drain.reason
                drain_seconds_to_stable_zero = $drain.seconds
                actual_rps = $k6Summary.actual_rps
                actual_rps_metric = $k6Summary.actual_rps_metric
                like_http_requests = $k6Summary.like_http_requests
                like_http_requests_metric = $k6Summary.like_http_requests_metric
                http_req_duration_avg_ms = $k6Summary.http_req_duration_avg_ms
                http_req_duration_p95_ms = $k6Summary.http_req_duration_p95_ms
                http_req_duration_percentile_source = $k6Summary.http_req_duration_percentile_source
                http_req_duration_p99_ms = $k6Summary.http_req_duration_p99_ms
                http_req_duration_max_ms = $k6Summary.http_req_duration_max_ms
                http_req_failed_rate = $k6Summary.http_req_failed_rate
                business_failure_rate = $k6Summary.business_failure_rate
                business_failure_count = $k6Summary.business_failure_count
                iterations = $k6Summary.iterations
                dropped_iterations = $k6Summary.dropped_iterations
                relay_published_delta = Get-CounterDelta -Before $beforeMap -After $afterDrainMap -MetricName "community_like_rabbitmq_relay_published_total"
                relay_failures_delta = Get-CounterDelta -Before $beforeMap -After $afterDrainMap -MetricName "community_like_rabbitmq_relay_failures_total"
                consumer_messages_delta = Get-CounterDelta -Before $beforeMap -After $afterDrainMap -MetricName "community_like_rabbitmq_consumer_messages_total"
                consumer_failures_delta = Get-CounterDelta -Before $beforeMap -After $afterDrainMap -MetricName "community_like_rabbitmq_consumer_failures_total"
                consumer_retry_attempts_delta = Get-CounterDelta -Before $beforeMap -After $afterDrainMap -MetricName "community_like_rabbitmq_consumer_retry_attempts_total"
                consumer_dead_lettered_delta = Get-CounterDelta -Before $beforeMap -After $afterDrainMap -MetricName "community_like_rabbitmq_consumer_dead_lettered_total"
                dead_letter_published_delta = $deadLetterPublishedDelta
                dead_letter_audit_persisted_delta = $deadLetterAuditPersistedDelta
                dead_letter_audit_failures_delta = $deadLetterAuditFailuresDelta
                dead_letter_audit_pending_delta = $deadLetterAuditPendingDelta
                dead_letter_detected = ($deadLetterPublishedDelta -gt 0)
                dead_letter_audit_complete = ($deadLetterAuditPendingDelta -eq 0)
                dead_letter_clean_run = ($deadLetterPublishedDelta -eq 0 -and $deadLetterAuditFailuresDelta -eq 0)
                rabbitmq_published_delta = Get-CounterDelta -Before $beforeMap -After $afterDrainMap -MetricName "rabbitmq_published_total"
                rabbitmq_consumed_delta = Get-CounterDelta -Before $beforeMap -After $afterDrainMap -MetricName "rabbitmq_consumed_total"
                rabbitmq_failed_publish_delta = Get-CounterDelta -Before $beforeMap -After $afterDrainMap -MetricName "rabbitmq_failed_to_publish_total"
                avg_stream_entries_during_load = Get-MetricSummary -Path (Join-Path $runDir "prometheus-timeseries.csv") -MetricName "community_like_rabbitmq_relay_stream_entries" -Suffix "avg"
                max_stream_entries_during_load = Get-MetricSummary -Path (Join-Path $runDir "prometheus-timeseries.csv") -MetricName "community_like_rabbitmq_relay_stream_entries" -Suffix "max"
                max_stream_pending_during_load = Get-MetricSummary -Path (Join-Path $runDir "prometheus-timeseries.csv") -MetricName "community_like_rabbitmq_relay_stream_pending" -Suffix "max"
                avg_rabbitmq_queue_depth_during_load = Get-MetricSummary -Path (Join-Path $runDir "prometheus-timeseries.csv") -MetricName "community_like_rabbitmq_queue_depth" -Suffix "avg"
                max_rabbitmq_queue_depth_during_load = Get-MetricSummary -Path (Join-Path $runDir "prometheus-timeseries.csv") -MetricName "community_like_rabbitmq_queue_depth" -Suffix "max"
                max_consumer_inflight_during_load = Get-MetricSummary -Path (Join-Path $runDir "prometheus-timeseries.csv") -MetricName "community_like_rabbitmq_consumer_inflight" -Suffix "max"
                retry_queue_depth_max_during_load = Get-MetricSummary -Path (Join-Path $runDir "prometheus-timeseries.csv") -MetricName "community_like_rabbitmq_retry_depth" -Suffix "max"
                dead_queue_depth_max_during_load = Get-MetricSummary -Path (Join-Path $runDir "prometheus-timeseries.csv") -MetricName "community_like_rabbitmq_dead_depth" -Suffix "max"
                run_folder = $runId
            }
            $summaryObject | ConvertTo-Json -Depth 12 | Set-Content -Path (Join-Path $runDir "run-metadata.json") -Encoding UTF8
            Write-CsvRow -Path $indexPath -Row $summaryObject

            if (-not $drain.success) {
                throw "本轮结束后消息未可靠排空（$($drain.reason)）。已保存本轮数据，停止后续实验：$runId"
            }
            if ($exitCode -ne 0 -and $exitCode -ne 99) {
                throw "k6 返回非阈值失败退出码 $exitCode。已保存本轮数据和排空结果，停止后续实验：$runId"
            }
            if ($null -eq $deadLetterPublishedDelta -or $null -eq $deadLetterAuditFailuresDelta) {
                throw "本轮死信 Counter 快照不可用；该轮结果不能纳入基线，停止后续矩阵：$runId"
            }
            if ($deadLetterPublishedDelta -gt 0 -or $deadLetterAuditFailuresDelta -gt 0) {
                throw "本轮虽然完成排空，但产生死信/审计失败：published=$deadLetterPublishedDelta, auditFailures=$deadLetterAuditFailuresDelta。该轮标记为异常并停止后续矩阵：$runId"
            }
            Write-Host ("完成 {0}: actualRPS={1}, p95={2}ms, businessFail={3}, dropped={4}, drain={5}s" -f `
                $runId, $k6Summary.actual_rps, $k6Summary.http_req_duration_p95_ms, $k6Summary.business_failure_rate, $k6Summary.dropped_iterations, $drain.seconds) -ForegroundColor Green
            if ($CooldownSeconds -gt 0) { Start-Sleep -Seconds $CooldownSeconds }
        }
    }
}
catch {
    $abortReason = $_.Exception.Message
    Write-Warning "矩阵中止：$abortReason"
}

# ------------------------------
# Final package and README notes
# ------------------------------
$finalStatus = if ($abortReason) { "partial_or_aborted" } else { "completed" }
$finalManifestPath = Join-Path $script:SessionDir "completion.json"
[pscustomobject]@{
    status = $finalStatus
    abort_reason = $abortReason
    completed_at_local = (Get-Date).ToString("o")
    completed_at_utc = [DateTime]::UtcNow.ToString("o")
    planned_runs = (3 * $Rates.Count * $Repetitions)
    recorded_runs = if (Test-Path $indexPath) { @(Import-Csv $indexPath).Count } else { 0 }
} | ConvertTo-Json -Depth 5 | Set-Content -Path $finalManifestPath -Encoding UTF8

if ($script:ResolvedAppLogPath -and (Test-Path $script:ResolvedAppLogPath)) {
    $appLogDir = Join-Path $script:SessionDir "application-logs"
    New-Item -ItemType Directory -Force -Path $appLogDir | Out-Null
    Copy-Item -LiteralPath $script:ResolvedAppLogPath -Destination (Join-Path $appLogDir "backend-app.log") -Force
}

$readmePath = Join-Path $script:SessionDir "HOW_TO_READ_RESULTS.txt"
@"
Community Like Benchmark Results

This archive contains comparative measurements for single / multi / mixed target distributions.

Key files:
- manifest.json: test configuration, machine, software versions, Git commit, hashes of local input files.
- run-index.csv: one row per test round; compare actual_rps, p95/p99, failures, dropped iterations, consumer deltas, backlog and drain time.
- k6-metrics.csv: all k6 summary metric values for each run.
- baseline/: idle host and Actuator Prometheus baseline samples.
- Rxx-.../: per-run k6 summary/logs, full Prometheus time series, Windows host samples, Docker stats and drain-check records.
- completion.json: whether all runs completed or the matrix stopped early.
- application-logs/backend-app.log: optional copy of the Spring Boot log if -AppLogPath was supplied.

Important interpretation rules:
1. target_rps is the scheduled arrival rate. Use actual_rps and dropped_iterations to judge whether k6 achieved it.
2. The benchmark k6 copy emits BENCHMARK_LOAD_STARTED on its first actual load iteration. Prometheus/host samples before that marker are phase=setup (the 400 account logins); samples after it are phase=load.
3. Prefer http_req_duration{name:like_toggle} for API latency. If absent, the index falls back to aggregate http_req_duration and marks the actual-RPS source similarly; this fallback may include setup/login requests.
4. The k6 API latency is request acceptance latency, not MySQL persistence latency. Use consumer_messages_delta, RabbitMQ queue/Stream gauges, batch duration metrics, and drain time to analyze asynchronous persistence.
5. A non-empty dead queue, non-zero failures/retries, or business failure rate is a reliability signal, not just a performance number.
6. Results are relative comparisons under this exact hardware/software configuration. Do not present the measured RPS as a universal server capacity claim.
7. users.local.json and targets.local.json are intentionally not included. Their SHA-256 hashes are in manifest.json so the local test inputs can be identified without exposing credentials or target IDs.
8. Prometheus metric samples include every numeric metric exposed by /actuator/prometheus, including JVM, process/system CPU, datasource, RabbitMQ and custom consumer metrics when enabled.
9. host-timeseries.csv describes the Windows machine running this script/k6. If backend is on another host, use system_cpu_usage / process_cpu_usage in Prometheus for backend CPU where available.

The ZIP is intended to be sent back for analysis. Do not add users.local.json, passwords, session cookies, database dumps or production data. If an application log was copied, inspect it for secrets before sharing.
"@ | Set-Content -Path $readmePath -Encoding UTF8

$zipPath = Join-Path $outputRootPath "community-like-benchmark-$timestampName.zip"
if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
Compress-Archive -Path (Join-Path $script:SessionDir "*") -DestinationPath $zipPath -CompressionLevel Optimal

if ($abortReason) {
    Write-Warning "已打包部分结果：$zipPath"
    Write-Warning "矩阵未完整完成：$abortReason"
    exit 2
}
Write-Host "所有压测完成并已打包：$zipPath" -ForegroundColor Green
