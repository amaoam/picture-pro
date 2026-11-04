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
    [string]$PrometheusBaseUrl = "http://localhost:9090",
    [string]$PrometheusJob = "picture-backend",
    [int]$PrometheusStepSeconds = 5,
    [string]$DockerContext = "",
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

# 保存调用脚本前的 PowerShell 状态；本脚本会临时切换工作目录并设置 k6 环境变量。
$script:ManagedEnvironmentVariables = @(
    "BASE_URL", "TARGET_TYPE", "PRE_VUS", "MAX_VUS", "SESSION_COOKIE_NAME",
    "USERS_FILE", "TARGETS_FILE", "SETUP_TIMEOUT", "REQUEST_TIMEOUT", "K6_NO_COLOR",
    "TARGET_MODE", "HOT_RATIO", "RATE", "DURATION"
)
$script:EnvironmentSnapshot = @{}
foreach ($environmentName in $script:ManagedEnvironmentVariables) {
    $environmentPath = "Env:\$environmentName"
    $script:EnvironmentSnapshot[$environmentName] = [pscustomobject]@{
        Exists = (Test-Path $environmentPath)
        Value = [Environment]::GetEnvironmentVariable($environmentName, "Process")
    }
}
$script:LocationPushed = $false
$script:FinalExitCode = 0
$script:ActiveK6Process = $null
$script:PendingDrain = $null
$script:NeedsRepackageAfterCleanup = $false

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


function ConvertTo-PrometheusUnixSeconds {
    param([Parameter(Mandatory = $true)][DateTime]$Timestamp)
    $dto = [DateTimeOffset]$Timestamp
    $seconds = [double]$dto.ToUnixTimeMilliseconds() / 1000.0
    return $seconds.ToString("0.###", [Globalization.CultureInfo]::InvariantCulture)
}

function Test-PrometheusServer {
    param(
        [string]$BaseUrl,
        [string]$JobName
    )

    $query = 'up{job="' + $JobName.Replace('"', '\\"') + '"}'
    $encoded = [Uri]::EscapeDataString($query)
    $uri = "$($BaseUrl.TrimEnd('/'))/api/v1/query?query=$encoded"
    try {
        $response = Invoke-RestMethod -Method Get -Uri $uri -TimeoutSec 15
    }
    catch {
        throw "Prometheus HTTP API 不可访问：$uri；$($_.Exception.Message)"
    }
    if ($response.status -ne "success" -or $null -eq $response.data -or $null -eq $response.data.result) {
        throw "Prometheus 查询失败或返回结构不正确：$uri"
    }
    $targets = @($response.data.result)
    if ($targets.Count -eq 0) {
        throw "Prometheus 没有找到 job='$JobName' 的抓取目标。请检查 config/prometheus/prometheus.yml 和 /targets。"
    }
    $down = @($targets | Where-Object {
        ($null -eq $_.value) -or ($_.value.Count -lt 2) -or ([string]$_.value[1] -ne "1")
    })
    if ($down.Count -gt 0) {
        throw "Prometheus job='$JobName' 存在 DOWN 目标：$($down.Count) 个。请先打开 $($BaseUrl.TrimEnd('/'))/targets 检查错误。"
    }
    return [pscustomobject]@{
        api_url = $BaseUrl.TrimEnd('/')
        job = $JobName
        target_count = $targets.Count
        targets_up = $true
    }
}

function Write-PrometheusRangeSamples {
    param(
        [string]$Phase,
        [string]$RunId,
        [string]$Mode,
        [int]$Rate,
        [int]$Repetition,
        [DateTime]$StartTime,
        [DateTime]$EndTime
    )

    if ($EndTime -le $StartTime) {
        throw "Prometheus query_range 时间范围不合法：start=$StartTime end=$EndTime"
    }

    $selector = '{job="' + $script:PrometheusJob.Replace('"', '\\"') + '"}'
    $encodedQuery = [Uri]::EscapeDataString($selector)
    # Start one scrape step after the load marker so the first evaluation does not reuse a pre-load sample.
    $queryStartTime = $StartTime.AddSeconds($script:PrometheusStepSeconds)
    if ($queryStartTime -ge $EndTime) { $queryStartTime = $StartTime }
    $startValue = ConvertTo-PrometheusUnixSeconds -Timestamp $queryStartTime
    $endValue = ConvertTo-PrometheusUnixSeconds -Timestamp $EndTime
    $step = [string]$script:PrometheusStepSeconds + 's'
    $uri = "$($script:PrometheusBaseUrl)/api/v1/query_range?query=$encodedQuery&start=$startValue&end=$endValue&step=$step"

    Write-Host "从 Prometheus TSDB 回取 $Phase 时序：step=$step，window=$startValue..$endValue" -ForegroundColor Cyan
    try {
        $response = Invoke-RestMethod -Method Get -Uri $uri -TimeoutSec 90
    }
    catch {
        throw "Prometheus query_range 请求失败：$($_.Exception.Message)"
    }
    if ($response.status -ne "success" -or $null -eq $response.data -or $null -eq $response.data.result) {
        $apiError = if ($response.error) { [string]$response.error } else { "未知 API 响应" }
        throw "Prometheus query_range 返回错误：$apiError"
    }

    $csvPath = Join-Path $script:CurrentRunDir "prometheus-timeseries.csv"
    $batch = New-Object 'System.Collections.Generic.List[object]'
    $written = 0
    $seriesCount = 0
    foreach ($series in $response.data.result) {
        $metricNameProperty = $series.metric.PSObject.Properties["__name__"]
        if ($null -eq $metricNameProperty) { continue }
        $metricName = [string]$metricNameProperty.Value
        if ([string]::IsNullOrWhiteSpace($metricName)) { continue }
        $seriesCount++

        $labelPairs = New-Object 'System.Collections.Generic.List[string]'
        foreach ($labelProperty in ($series.metric.PSObject.Properties | Where-Object { $_.Name -ne "__name__" } | Sort-Object Name)) {
            $labelValue = ([string]$labelProperty.Value).Replace('\\', '\\\\').Replace('"', '\\"')
            $labelPairs.Add(($labelProperty.Name + '="' + $labelValue + '"'))
        }
        $labelsText = if ($labelPairs.Count -gt 0) { '{' + ($labelPairs -join ',') + '}' } else { $null }

        foreach ($point in $series.values) {
            if ($null -eq $point -or $point.Count -lt 2) { continue }
            $epochSeconds = 0.0
            if (-not [double]::TryParse([string]$point[0], [Globalization.NumberStyles]::Float,
                    [Globalization.CultureInfo]::InvariantCulture, [ref]$epochSeconds)) { continue }
            $epochMilliseconds = [long][math]::Round($epochSeconds * 1000.0)
            $timestampUtc = [DateTimeOffset]::FromUnixTimeMilliseconds($epochMilliseconds).UtcDateTime.ToString("o")
            $batch.Add([pscustomobject]@{
                timestamp_utc = $timestampUtc
                run_id = $RunId
                phase = $Phase
                mode = $Mode
                target_rps = $Rate
                repetition = $Repetition
                metric_name = $metricName
                labels = $labelsText
                value = [string]$point[1]
            })
            if ($batch.Count -ge 2000) {
                Write-CsvRows -Path $csvPath -Rows $batch.ToArray()
                $written += $batch.Count
                $batch.Clear()
            }
        }
    }
    if ($batch.Count -gt 0) {
        Write-CsvRows -Path $csvPath -Rows $batch.ToArray()
        $written += $batch.Count
        $batch.Clear()
    }

    $rangeMeta = [pscustomobject]@{
        status = "success"
        phase = $Phase
        run_id = $RunId
        job = $script:PrometheusJob
        selector = $selector
        load_window_start_utc = ([DateTime]$StartTime).ToUniversalTime().ToString("o")
        query_start_utc = ([DateTime]$queryStartTime).ToUniversalTime().ToString("o")
        end_utc = ([DateTime]$EndTime).ToUniversalTime().ToString("o")
        step_seconds = $script:PrometheusStepSeconds
        series_count = $seriesCount
        samples_written = $written
        query_range_api = "$($script:PrometheusBaseUrl)/api/v1/query_range"
    }
    $metaPath = Join-Path $script:CurrentRunDir ("prometheus-query-range-{0}.json" -f $Phase)
    $rangeMeta | ConvertTo-Json -Depth 6 | Set-Content -Path $metaPath -Encoding UTF8
    if ($written -le 0) {
        throw "Prometheus query_range 没有返回任何时序样本。请检查 job='$($script:PrometheusJob)'、时间范围、Prometheus 保留期和 /targets。"
    }
    Write-Host "Prometheus 回取完成：series=$seriesCount，samples=$written" -ForegroundColor Green
    return $rangeMeta
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
        $dockerArguments = @()
        if (-not [string]::IsNullOrWhiteSpace($script:DockerContext)) {
            $dockerArguments += @("--context", $script:DockerContext)
        }
        $dockerArguments += @("stats", "--no-stream", "--format", '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}|{{.NetIO}}|{{.BlockIO}}')
        $output = & $script:DockerExe @dockerArguments 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-CsvRow -Path (Join-Path $script:CurrentRunDir "docker-stats-errors.csv") -Row ([pscustomobject]@{
                timestamp_utc = [DateTime]::UtcNow.ToString("o")
                phase = $Phase
                error = (($output | Out-String).Trim())
                docker_context = $script:DockerContext
            })
            return
        }
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
                docker_context = $script:DockerContext
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
    # During baseline/setup/load, application metrics are taken from Prometheus TSDB
    # after the interval. Do not repeatedly scrape the full Actuator endpoint from this
    # controller loop: that creates avoidable load and makes the loop drift beyond its interval.
    $map = $null
    if ($Phase -notin @("baseline", "setup", "load")) {
        $map = Write-PrometheusSample -Phase $Phase -RunId $RunId -Mode $Mode -Rate $Rate -Repetition $Repetition
    }
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

    # Parse each run's large CSV once, then reuse the aggregate for every metric summary field.
    $cacheVariable = Get-Variable -Name LoadMetricSummaryCache -Scope Script -ErrorAction SilentlyContinue
    if ($null -eq $cacheVariable) { $script:LoadMetricSummaryCache = @{} }

    if (-not $script:LoadMetricSummaryCache.ContainsKey($Path)) {
        $trackedMetrics = @(
            "community_like_rabbitmq_relay_stream_entries",
            "community_like_rabbitmq_relay_stream_pending",
            "community_like_rabbitmq_queue_depth",
            "community_like_rabbitmq_consumer_inflight",
            "community_like_rabbitmq_retry_depth",
            "community_like_rabbitmq_dead_depth"
        )
        $stats = @{}
        foreach ($name in $trackedMetrics) {
            $stats[$name] = @{ sum = 0.0; count = 0; min = [double]::PositiveInfinity; max = [double]::NegativeInfinity }
        }

        Import-Csv -Path $Path | ForEach-Object {
            if ($_.phase -eq "load" -and $stats.ContainsKey($_.metric_name)) {
                $parsed = 0.0
                if ([double]::TryParse(
                        $_.value,
                        [Globalization.NumberStyles]::Float,
                        [Globalization.CultureInfo]::InvariantCulture,
                        [ref]$parsed)) {
                    $stat = $stats[$_.metric_name]
                    $stat.sum += $parsed
                    $stat.count++
                    if ($parsed -lt $stat.min) { $stat.min = $parsed }
                    if ($parsed -gt $stat.max) { $stat.max = $parsed }
                }
            }
        }

        $aggregated = @{}
        foreach ($name in $trackedMetrics) {
            $stat = $stats[$name]
            if ($stat.count -gt 0) {
                $aggregated[$name] = @{
                    avg = [math]::Round(($stat.sum / $stat.count), 3)
                    min = [math]::Round($stat.min, 3)
                    max = [math]::Round($stat.max, 3)
                }
            }
        }
        $script:LoadMetricSummaryCache[$Path] = $aggregated
    }

    $pathSummary = $script:LoadMetricSummaryCache[$Path]
    if (-not $pathSummary.ContainsKey($MetricName)) { return $null }
    if (-not $pathSummary[$MetricName].ContainsKey($Suffix)) { return $null }
    return $pathSummary[$MetricName][$Suffix]
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


function Get-K6ThresholdSummary {
    param([object]$Summary)

    $metrics = Get-ObjectPropertyValue -Object $Summary -Name "metrics"
    if ($null -eq $metrics) {
        return [pscustomobject]@{
            present = $false
            failed = $null
            failed_count = $null
            failed_names = $null
            threshold_count = 0
        }
    }

    $thresholdCount = 0
    $failedNames = New-Object 'System.Collections.Generic.List[string]'
    foreach ($metricProperty in $metrics.PSObject.Properties) {
        $thresholds = Get-ObjectPropertyValue -Object $metricProperty.Value -Name "thresholds"
        if ($null -eq $thresholds) { continue }
        foreach ($thresholdProperty in $thresholds.PSObject.Properties) {
            $thresholdCount++
            # In k6 summary JSON, true means the threshold failed; false means it passed.
            if ([bool]$thresholdProperty.Value) {
                $failedNames.Add("$($metricProperty.Name):$($thresholdProperty.Name)")
            }
        }
    }

    if ($thresholdCount -eq 0) {
        return [pscustomobject]@{
            present = $false
            failed = $null
            failed_count = $null
            failed_names = $null
            threshold_count = 0
        }
    }

    return [pscustomobject]@{
        present = $true
        failed = ($failedNames.Count -gt 0)
        failed_count = $failedNames.Count
        failed_names = ($failedNames -join ";")
        threshold_count = $thresholdCount
    }
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
    $thresholdSummary = Get-K6ThresholdSummary -Summary $summary
    $successfulHttpRequests = $null
    $successfulBusinessRequests = $null
    $successfulHttpRps = $null
    $successfulBusinessRps = $null
    if ($null -ne $requestCount -and $null -ne $httpFailureRate) {
        $successfulHttpRequests = [math]::Round(([double]$requestCount * (1.0 - [double]$httpFailureRate)), 3)
        if ($durationSeconds -gt 0) {
            $successfulHttpRps = [math]::Round(($successfulHttpRequests / $durationSeconds), 3)
        }
    }
    if ($null -ne $requestCount -and $null -ne $businessFailureRate) {
        $successfulBusinessRequests = [math]::Round(([double]$requestCount * (1.0 - [double]$businessFailureRate)), 3)
        if ($durationSeconds -gt 0) {
            $successfulBusinessRps = [math]::Round(($successfulBusinessRequests / $durationSeconds), 3)
        }
    }

    return [pscustomobject]@{
        actual_rps = $actualRps
        successful_http_requests = $successfulHttpRequests
        successful_http_rps = $successfulHttpRps
        successful_business_requests = $successfulBusinessRequests
        successful_business_rps = $successfulBusinessRps
        thresholds_present = $thresholdSummary.present
        thresholds_failed = $thresholdSummary.failed
        thresholds_failed_count = $thresholdSummary.failed_count
        thresholds_failed_names = $thresholdSummary.failed_names
        thresholds_count = $thresholdSummary.threshold_count
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
        [string]$StdoutPath,
        [string]$StderrPath
    )

    # Exit-code inference is deliberately conservative: require a summary and
    # a completed TOTAL RESULTS section. Threshold status comes from summary JSON,
    # not from a guessed native exit code or a console-text regex.
    if (-not (Test-Path $SummaryPath)) { return $null }

    $completeResultsFound = $false
    foreach ($logPath in @($StdoutPath, $StderrPath)) {
        if (-not [string]::IsNullOrWhiteSpace($logPath) -and (Test-Path $logPath)) {
            try {
                if ((Get-Content -Raw -Path $logPath -ErrorAction Stop) -match "TOTAL RESULTS") {
                    $completeResultsFound = $true
                    break
                }
            }
            catch { }
        }
    }
    if (-not $completeResultsFound) { return $null }

    try {
        $summary = Get-Content -Raw -Path $SummaryPath | ConvertFrom-Json
        $thresholdSummary = Get-K6ThresholdSummary -Summary $summary
        if (-not $thresholdSummary.present -or $null -eq $thresholdSummary.failed) { return $null }
        if ($thresholdSummary.failed) { return 99 }
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
function Invoke-PendingDrainCleanup {
    param([string]$Reason = "unexpected_script_exit")

    if ($null -eq $script:PendingDrain) { return $null }
    $pending = $script:PendingDrain
    $cleanupError = $null
    $processStopped = $true

    # 必须先确认 k6 停止，不能在它仍可能产生消息时开始排空检查。
    if ($null -ne $script:ActiveK6Process) {
        try {
            $activeProcess = $script:ActiveK6Process
            $activeProcess.Refresh()
            if (-not $activeProcess.HasExited) {
                Stop-Process -Id $activeProcess.Id -Force -ErrorAction Stop
            }
            if (-not $activeProcess.WaitForExit(30000)) {
                Stop-Process -Id $activeProcess.Id -Force -ErrorAction SilentlyContinue
                if (-not $activeProcess.WaitForExit(30000)) {
                    throw "等待 k6 进程退出超时，PID=$($activeProcess.Id)"
                }
            }
        }
        catch {
            $processStopped = $false
            $cleanupError = "停止 k6 进程失败：$($_.Exception.Message)"
        }
        finally {
            if ($processStopped) {
                try { $script:ActiveK6Process.Dispose() } catch { }
                $script:ActiveK6Process = $null
            }
        }
    }

    $drain = $null
    if ($processStopped) {
        try {
            $script:CurrentRunDir = $pending.RunDir
            $drain = Wait-MqBacklogDrain `
                -RunId $pending.RunId `
                -Mode $pending.Mode `
                -Rate $pending.Rate `
                -Repetition $pending.Repetition `
                -CounterBefore $pending.CounterBefore
            # A returned result means the full configured drain window was checked.
            # A timeout remains a failure; do not silently repeat another 300 seconds.
            $script:PendingDrain = $null
        }
        catch {
            $cleanupError = if ($cleanupError) {
                "$cleanupError；排空检查失败：$($_.Exception.Message)"
            } else {
                "排空检查失败：$($_.Exception.Message)"
            }
            # The drain routine itself could not complete (for example, required
            # telemetry disappeared). Record the failure and avoid an infinite
            # series of identical retries during final packaging.
            $script:PendingDrain = $null
        }
    }

    $cleanupRecord = [pscustomobject]@{
        timestamp_utc = [DateTime]::UtcNow.ToString("o")
        reason = $Reason
        process_stopped = $processStopped
        drain_attempted = ($null -ne $drain)
        drain_success = if ($null -ne $drain) { $drain.success } else { $false }
        drain_reason = if ($null -ne $drain) { $drain.reason } else { $null }
        drain_seconds = if ($null -ne $drain) { $drain.seconds } else { $null }
        error = $cleanupError
    }

    try {
        if ($pending.RunDir -and (Test-Path $pending.RunDir)) {
            $cleanupRecord | ConvertTo-Json -Depth 8 | Set-Content -Path (Join-Path $pending.RunDir "emergency-cleanup.json") -Encoding UTF8
        }
    }
    catch {
        Write-Warning "无法保存 emergency-cleanup.json：$($_.Exception.Message)"
    }

    if ($cleanupError) { Write-Warning $cleanupError }
    if ($null -ne $drain -and -not $drain.success) {
        Write-Warning "异常清理期间未能确认消息排空：$($drain.reason)"
    }
    return $cleanupRecord
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
try {
if (-not (Test-Path $ProjectPath)) { throw "找不到项目目录：$ProjectPath" }
$script:ProjectPath = (Resolve-Path $ProjectPath).Path
Push-Location -LiteralPath $script:ProjectPath
$script:LocationPushed = $true
$script:BaseUrl = $BaseUrl
$script:PrometheusBaseUrl = $PrometheusBaseUrl.TrimEnd('/')
$script:PrometheusJob = $PrometheusJob
$script:PrometheusStepSeconds = $PrometheusStepSeconds
$script:DockerContext = $DockerContext
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
if ($DockerSampleIntervalSeconds -lt 1) { throw "DockerSampleIntervalSeconds 必须大于等于 1。" }
if ($PrometheusStepSeconds -lt 1) { throw "PrometheusStepSeconds 必须大于等于 1。" }
if ([string]::IsNullOrWhiteSpace($PrometheusJob)) { throw "PrometheusJob 不能为空。" }
if ($BaselineSeconds -lt 0) { throw "BaselineSeconds 不能小于 0。" }
if ($DrainTimeoutSeconds -lt 1) { throw "DrainTimeoutSeconds 必须大于等于 1。" }
if ($DrainStableSamples -lt 1) { throw "DrainStableSamples 必须大于等于 1。" }
if ($DrainPollSeconds -lt 1) { throw "DrainPollSeconds 必须大于等于 1。" }
if ($CooldownSeconds -lt 0) { throw "CooldownSeconds 不能小于 0。" }
if ($JavaPid -lt 0) { throw "JavaPid 不能小于 0。" }
if (@($Rates).Count -lt 1 -or @($Rates | Where-Object { $_ -le 0 }).Count -gt 0) { throw "Rates 必须全部为正整数。" }
if (@($Rates | Select-Object -Unique).Count -ne @($Rates).Count) { throw "Rates 不应包含重复速率。" }
if ($MaxVus -lt $PreVus -or $PreVus -le 0) { throw "请确保 PreVus > 0 且 MaxVus >= PreVus。" }
if ($TargetType -notin @("POST", "PICTURE")) { throw "TargetType 只能是 POST 或 PICTURE。" }
if ([string]::IsNullOrWhiteSpace($SessionCookieName)) { throw "SessionCookieName 不能为空。" }
if ((ConvertTo-DurationSeconds -Duration $Duration) -le 0) { throw "Duration 格式无效，请使用例如 60s、2m 或 1m30s。" }
if ([string]::IsNullOrWhiteSpace($SetupTimeout) -or [string]::IsNullOrWhiteSpace($RequestTimeout)) {
    throw "SetupTimeout 和 RequestTimeout 不能为空。"
}

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
$credentialItems = @($credentialData)
if ($credentialItems.Count -lt $MaxVus) {
    throw "测试账号数 $($credentialItems.Count) 小于 MaxVus=$MaxVus；当前 k6 脚本要求账号数至少等于 MAX_VUS。"
}
$seenAccounts = @{}
foreach ($credential in $credentialItems) {
    $accountValue = Get-ObjectPropertyValue -Object $credential -Name "userAccount" -Default $null
    $passwordValue = Get-ObjectPropertyValue -Object $credential -Name "userPassword" -Default $null
    if ($accountValue -isnot [string] -or [string]::IsNullOrWhiteSpace($accountValue)) {
        throw "测试账号 JSON 中的 userAccount 必须是非空字符串。"
    }
    if ($passwordValue -isnot [string] -or [string]::IsNullOrWhiteSpace($passwordValue)) {
        throw "测试账号 $accountValue 的 userPassword 必须是非空字符串。"
    }
    if ($accountValue -ne $accountValue.Trim()) {
        throw "测试账号 userAccount 不应包含首尾空格：$accountValue。"
    }
    $accountKey = $accountValue.Trim()
    if ($seenAccounts.ContainsKey($accountKey)) { throw "测试账号文件中存在重复 userAccount：$accountKey。每个 VU 必须使用独立账号。" }
    $seenAccounts[$accountKey] = $true
}

$configuredHotTargetIdValue = Get-ObjectPropertyValue -Object $targetData -Name "hotTargetId" -Default $null
$configuredTargetIds = Get-ObjectPropertyValue -Object $targetData -Name "targetIds" -Default @()
$environmentHotTargetId = [Environment]::GetEnvironmentVariable("TARGET_ID", "Process")
if (-not [string]::IsNullOrWhiteSpace($environmentHotTargetId)) {
    $hotTargetCandidate = $environmentHotTargetId
}
else {
    if ($configuredHotTargetIdValue -isnot [string]) {
        throw "targets.local.json 的 hotTargetId 必须使用字符串保存，避免 64 位 ID 被 JavaScript Number 精度截断。"
    }
    $hotTargetCandidate = $configuredHotTargetIdValue
}
if ($hotTargetCandidate -notmatch '^\d+$') { throw "targets.local.json 必须提供有效的数字 hotTargetId，或通过 TARGET_ID 指定。" }
if (@($configuredTargetIds).Count -lt 1) { throw "targets.local.json 的 targetIds 至少需要提供一个目标 ID。" }
$targetIdCandidates = @($hotTargetCandidate)
foreach ($candidate in @($configuredTargetIds)) {
    if ($candidate -isnot [string]) {
        throw "targetIds 中的 ID 必须用 JSON 字符串保存，避免 64 位 ID 被 JavaScript Number 精度截断。"
    }
    $candidateText = $candidate
    if ($candidateText -notmatch '^\d+$') { throw "targetIds 中存在无效数字 ID：$candidateText。" }
    $targetIdCandidates += $candidateText
}
$uniqueTargetIds = @($targetIdCandidates | Select-Object -Unique)
if ($uniqueTargetIds.Count -lt 2) {
    throw "single/multi/mixed 实验矩阵至少需要 hotTargetId 之外的一个不同目标 ID。"
}

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

$timestampName = Get-Date -Format "yyyyMMdd-HHmmss-fff"
$outputRootPath = if ([IO.Path]::IsPathRooted($OutputRoot)) { $OutputRoot } else { Join-Path $script:ProjectPath $OutputRoot }
New-Item -ItemType Directory -Force -Path $outputRootPath | Out-Null
$script:SessionDir = Join-Path $outputRootPath "community-like-benchmark-$timestampName"
New-Item -ItemType Directory -Force -Path $script:SessionDir | Out-Null
$script:ResolvedAppLogPath = $null
if (-not [string]::IsNullOrWhiteSpace($AppLogPath)) {
    $script:ResolvedAppLogPath = if ([IO.Path]::IsPathRooted($AppLogPath)) { $AppLogPath } else { Join-Path $script:ProjectPath $AppLogPath }
    if (-not (Test-Path -LiteralPath $script:ResolvedAppLogPath -PathType Leaf)) {
        Write-Warning "指定的 AppLogPath 不是现存文件：$script:ResolvedAppLogPath；将不复制应用日志。"
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
$dockerVersionArguments = @()
if (-not [string]::IsNullOrWhiteSpace($script:DockerContext)) { $dockerVersionArguments += @("--context", $script:DockerContext) }
$dockerVersionArguments += @("version", "--format", "{{.Server.Version}}")
$dockerVersion = if ($script:DockerAvailable) { Get-CommandVersionText -Command $script:DockerExe -Arguments $dockerVersionArguments } else { "not_available" }

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
$prometheusPreflight = Test-PrometheusServer -BaseUrl $script:PrometheusBaseUrl -JobName $script:PrometheusJob
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
    prometheus_server = $prometheusPreflight
    prometheus_base_url = $script:PrometheusBaseUrl
    prometheus_job = $script:PrometheusJob
    prometheus_range_step_seconds = $PrometheusStepSeconds
    docker_context = $DockerContext
    k6_script_relative_path = $K6Script
    k6_script_sha256 = Get-Sha256 -Path $scriptFile
    users_file_sha256 = Get-Sha256 -Path $usersPath
    targets_file_sha256 = Get-Sha256 -Path $targetsPath
    # Intentionally do not copy users.local.json / targets.local.json: they contain local test credentials and data.
    target_type = $TargetType
    session_cookie_name = $SessionCookieName
    duration = $Duration
    summary_trend_stats = @("avg", "min", "med", "max", "p(90)", "p(95)", "p(99)")
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
        "Prometheus application metrics are recovered from TSDB query_range after each run; set Prometheus scrape interval and query step to the same value.",
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
$baselineStartUtc = [DateTime]::UtcNow
$nextBaselineSample = Get-Date
while (((Get-Date) - $baselineStart).TotalSeconds -lt $BaselineSeconds) {
    $now = Get-Date
    if ($now -ge $nextBaselineSample) {
        Capture-TelemetrySample -Phase "baseline" -RunId "BASELINE" -Mode "none" -Rate 0 -Repetition 0 -K6ProcessId 0 | Out-Null
        $nextBaselineSample = $nextBaselineSample.AddSeconds($SampleIntervalSeconds)
        if ($nextBaselineSample -le (Get-Date)) { $nextBaselineSample = (Get-Date).AddSeconds($SampleIntervalSeconds) }
    }
    Start-Sleep -Milliseconds 200
}
$baselineEndUtc = [DateTime]::UtcNow
$script:CurrentRunDir = $baselineDir
$baselineRange = Write-PrometheusRangeSamples -Phase "baseline" -RunId "BASELINE" -Mode "none" -Rate 0 -Repetition 0 `
    -StartTime $baselineStartUtc -EndTime $baselineEndUtc

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
            $k6Args = @("run", $scriptFile, "--summary-export", $summaryPath, "--summary-trend-stats=avg,min,med,max,p(90),p(95),p(99)")
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

            $processId = 0
            $process = $null
            $affinityApplied = $false
            $loadMarkerSeen = $false
            $loadStartedAt = $null
            $exitCode = $null
            $exitCodeReadError = $null
            $runProcessingError = $null
            $postloadCaptureError = $null
            $summaryError = $null
            $exitCodeInferred = $false
            $afterLoadMap = $null

            $k6Summary = [pscustomobject]@{
                actual_rps = $null
                successful_http_requests = $null
                successful_http_rps = $null
                successful_business_requests = $null
                successful_business_rps = $null
                thresholds_present = $false
                thresholds_failed = $null
                thresholds_failed_count = $null
                thresholds_failed_names = $null
                thresholds_count = 0
                actual_rps_metric = "unavailable: summary missing or invalid"
                like_http_requests = $null
                like_http_requests_metric = "unavailable"
                http_req_duration_avg_ms = $null
                http_req_duration_p95_ms = $null
                http_req_duration_percentile_source = $null
                http_req_duration_p99_ms = $null
                http_req_duration_max_ms = $null
                http_req_failed_rate = $null
                business_failure_rate = $null
                business_failure_count = $null
                iterations = $null
                dropped_iterations = $null
                summary = $null
            }

            $process = Start-Process -FilePath $script:K6Exe -ArgumentList $argumentLine -WorkingDirectory $script:ProjectPath `
                -PassThru -NoNewWindow -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
            $script:ActiveK6Process = $process
            $script:PendingDrain = [pscustomobject]@{
                RunId = $runId
                RunDir = $runDir
                Mode = $mode
                Rate = $rate
                Repetition = $rep
                CounterBefore = $beforeMap
            }
            $processId = $process.Id

            if (-not $script:SkipCpuAffinity) {
                try {
                    $process.ProcessorAffinity = [IntPtr]$affinityMask
                    $affinityApplied = $true
                }
                catch { Write-Warning "k6 已启动，但 CPU 绑核失败：$($_.Exception.Message)" }
            }

            $lastMarkerScan = [DateTime]::MinValue
            $nextHostSample = Get-Date
            try {
                while ($true) {
                    $process.Refresh()
                    $now = Get-Date
                    if (-not $loadMarkerSeen -and (($now - $lastMarkerScan).TotalMilliseconds -ge 250)) {
                        $lastMarkerScan = $now
                        # k6 console.log is written to stderr in some environments;
                        # inspect both redirected streams with a short detection interval.
                        if (Test-LoadMarkerInLogs -Paths @($stdoutPath, $stderrPath)) {
                            $loadMarkerSeen = $true
                            $loadStartedAt = $now
                            Write-Host "检测到 k6 正式负载开始标记；应用指标将从 Prometheus TSDB 回取，主机指标按设定间隔采集。" -ForegroundColor Cyan
                        }
                    }
                    if ($process.HasExited) { break }
                    if ($now -ge $nextHostSample) {
                        $samplePhase = if ($loadMarkerSeen) { "load" } else { "setup" }
                        Capture-TelemetrySample -Phase $samplePhase -RunId $runId -Mode $mode -Rate $rate -Repetition $rep -K6ProcessId $processId | Out-Null
                        $nextHostSample = $nextHostSample.AddSeconds($SampleIntervalSeconds)
                        if ($nextHostSample -le (Get-Date)) {
                            $nextHostSample = (Get-Date).AddSeconds($SampleIntervalSeconds)
                        }
                    }
                    Start-Sleep -Milliseconds 200
                }
            }
            catch {
                $runProcessingError = $_.Exception.Message
                Write-Warning "压测监控过程发生异常，将终止 k6 并优先排空已产生的消息：$runProcessingError"
                try {
                    $process.Refresh()
                    if (-not $process.HasExited) {
                        Stop-Process -Id $processId -Force -ErrorAction Stop
                    }
                }
                catch {
                    Write-Warning "尝试停止异常运行的 k6 进程失败：$($_.Exception.Message)"
                }
            }

            $processExitConfirmed = $false
            try {
                $process.Refresh()
                if (-not $process.HasExited) {
                    # Monitoring can fail while k6 is still running. Give the
                    # forced stop a bounded window rather than waiting forever.
                    if (-not $process.WaitForExit(30000)) {
                        Stop-Process -Id $processId -Force -ErrorAction Stop
                        if (-not $process.WaitForExit(30000)) {
                            throw "停止 k6 后仍未退出，PID=$processId。"
                        }
                    }
                }
                $process.WaitForExit()
                $process.Refresh()
                $processExitConfirmed = $process.HasExited
                if (-not $processExitConfirmed) {
                    throw "无法确认 k6 进程已经退出，PID=$processId。"
                }
                $exitCode = [int]$process.ExitCode
            }
            catch {
                $exitCodeReadError = $_.Exception.Message
                Write-Warning "读取/等待 k6 进程退出状态失败：$exitCodeReadError"
            }
            finally {
                if ($processExitConfirmed) {
                    try { $process.Dispose() } catch { }
                    $script:ActiveK6Process = $null
                }
            }
            if (-not $processExitConfirmed) {
                throw "不能确认 k6 进程已停止；停止后端消息生产后才能排空，交由异常清理逻辑重试。PID=$processId"
            }

            if (-not $loadMarkerSeen -and (Test-LoadMarkerInLogs -Paths @($stdoutPath, $stderrPath))) {
                $loadMarkerSeen = $true
                $loadStartedAt = Get-Date
            }
            $endTime = Get-Date
            $endUtc = [DateTime]::UtcNow
            $elapsedSeconds = [math]::Round(($endTime - $startTime).TotalSeconds, 2)
            $setupSeconds = if ($loadStartedAt) { [math]::Round(($loadStartedAt - $startTime).TotalSeconds, 2) } else { $null }

            try {
                $afterLoadMap = Write-PrometheusSample -Phase "postload" -RunId $runId -Mode $mode -Rate $rate -Repetition $rep
            }
            catch {
                $postloadCaptureError = $_.Exception.Message
                Write-Warning "压测后 Prometheus 快照采集失败：$postloadCaptureError"
            }

            # Summary failure must not bypass the mandatory RabbitMQ/Redis drain.
            try {
                if (-not (Test-Path $summaryPath)) {
                    throw "k6 没有生成 summary 文件，退出码=$exitCode；请查看 $stdoutPath 与 $stderrPath。"
                }
                $k6Summary = Get-K6SummaryFields -SummaryPath $summaryPath -LoadDuration $Duration

                # Remove setup_data immediately: it contains satoken cookies from k6 setup().
                # If sanitization cannot be persisted, delete the raw summary rather than
                # allowing credentials to enter the result ZIP.
                if ($null -ne $k6Summary.summary -and $k6Summary.summary.PSObject.Properties["setup_data"]) {
                    $k6Summary.summary.PSObject.Properties.Remove("setup_data")
                    try {
                        $k6Summary.summary | ConvertTo-Json -Depth 30 | Set-Content -Path $summaryPath -Encoding UTF8
                    }
                    catch {
                        Remove-Item -LiteralPath $summaryPath -Force -ErrorAction SilentlyContinue
                        throw "summary 含 setup_data，脱敏写回失败，已删除原始 summary 文件以保护会话 Cookie：$($_.Exception.Message)"
                    }
                }

                if ($null -eq $k6Summary.http_req_duration_p99_ms) {
                    throw "summary 中缺少 p(99)。请确认 k6 支持 --summary-trend-stats 且本次命令已生效。"
                }
                if ($null -eq $k6Summary.iterations -or $null -eq $k6Summary.http_req_duration_p95_ms -or
                    $null -eq $k6Summary.http_req_failed_rate -or $null -eq $k6Summary.business_failure_rate -or
                    $null -eq $k6Summary.dropped_iterations) {
                    throw "summary 缺少计算基准结果所需的关键指标（iterations/p95/failure-rate/dropped-iterations）。"
                }
            }
            catch {
                $summaryError = $_.Exception.Message
                Write-Warning "k6 summary 无法作为有效结果使用；仍将先执行消息排空：$summaryError"
            }

            # If Start-Process did not expose ExitCode, infer only from a complete
            # k6 TOTAL RESULTS section plus all threshold inputs in the summary.
            if ($null -eq $exitCode) {
                $fallbackExitCode = Get-K6FallbackExitCode `
                    -SummaryPath $summaryPath `
                    -StdoutPath $stdoutPath `
                    -StderrPath $stderrPath

                if ($null -eq $fallbackExitCode) {
                    Write-Warning "无法确认 k6 退出状态，先执行异步消息排空并保存本轮数据。"
                }
                else {
                    $exitCode = [int]$fallbackExitCode
                    $exitCodeInferred = $true
                }
            }

            # Export all available k6 metrics. This is optional telemetry and must
            # never prevent the mandatory message drain if the summary is partial.
            if ($null -ne $k6Summary.summary) {
                $summaryMetrics = Get-ObjectPropertyValue -Object $k6Summary.summary -Name "metrics"
                if ($null -ne $summaryMetrics) {
                    foreach ($metricProperty in $summaryMetrics.PSObject.Properties) {
                        Write-CsvRow -Path $metricIndexPath -Row ([pscustomobject]@{
                            run_id = $runId
                            repetition = $rep
                            mode = $mode
                            target_rps = $rate
                            metric_name = $metricProperty.Name
                            metric_type = (Get-ObjectPropertyValue -Object $metricProperty.Value -Name "type")
                            metric_values_json = ($metricProperty.Value | ConvertTo-Json -Depth 10 -Compress)
                        })
                    }
                }
            }

            # This is the critical cleanup point: only after k6 has stopped do we
            # wait for Redis Stream, RabbitMQ main/retry/DLQ and audit persistence.
            $drainError = $null
            try {
                $drain = Wait-MqBacklogDrain -RunId $runId -Mode $mode -Rate $rate -Repetition $rep -CounterBefore $beforeMap
                $script:PendingDrain = $null
            }
            catch {
                $drainError = $_.Exception.Message
                Write-Warning "本轮排空过程异常：$drainError；最终清理阶段将再尝试一次。"
                $drain = [pscustomobject]@{
                    success = $false
                    reason = "drain_exception"
                    seconds = $null
                    after_metrics = $null
                }
            }
            $afterDrainMap = $drain.after_metrics
            $drainEndUtc = [DateTime]::UtcNow

            $prometheusRangeError = $null
            $prometheusRangeSummary = $null
            if ($loadMarkerSeen -and $null -ne $loadStartedAt) {
                try {
                    $prometheusRangeSummary = Write-PrometheusRangeSamples -Phase "load" -RunId $runId -Mode $mode -Rate $rate -Repetition $rep `
                        -StartTime $loadStartedAt -EndTime $endTime
                }
                catch {
                    $prometheusRangeError = $_.Exception.Message
                    Write-Warning "Prometheus 历史时序回取失败：$prometheusRangeError"
                }
            }
            else {
                $prometheusRangeError = "未检测到 BENCHMARK_LOAD_STARTED，无法准确确定 Prometheus 负载查询窗口。"
                Write-Warning $prometheusRangeError
            }

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

            # Scrub test-account names from k6 logs before packaging.
            Scrub-K6Log -Path $stdoutPath -Credentials $credentialItems
            Scrub-K6Log -Path $stderrPath -Credentials $credentialItems

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
                monitoring_error = $runProcessingError
                postload_capture_error = $postloadCaptureError
                prometheus_range_error = $prometheusRangeError
                prometheus_range_series_count = if ($prometheusRangeSummary) { $prometheusRangeSummary.series_count } else { $null }
                prometheus_range_samples_written = if ($prometheusRangeSummary) { $prometheusRangeSummary.samples_written } else { $null }
                prometheus_range_step_seconds = $PrometheusStepSeconds
                prometheus_range_start_utc = if ($loadStartedAt) { $loadStartedAt.ToUniversalTime().ToString("o") } else { $null }
                prometheus_range_end_utc = if ($endTime) { $endTime.ToUniversalTime().ToString("o") } else { $null }
                summary_error = $summaryError
                exit_code_read_error = $exitCodeReadError
                drain_error = $drainError
                k6_exit_code = $exitCode
                k6_exit_code_inferred = $exitCodeInferred
                k6_thresholds_present = $k6Summary.thresholds_present
                k6_thresholds_failed = $k6Summary.thresholds_failed
                k6_thresholds_failed_count = $k6Summary.thresholds_failed_count
                k6_thresholds_failed_names = $k6Summary.thresholds_failed_names
                k6_thresholds_count = $k6Summary.thresholds_count
                k6_thresholds_passed = if ($null -ne $k6Summary.thresholds_failed) { -not [bool]$k6Summary.thresholds_failed } else { $null }
                k6_summary_exists = (Test-Path $summaryPath)
                drain_success = $drain.success
                drain_reason = $drain.reason
                drain_seconds_to_stable_zero = $drain.seconds
                actual_rps = $k6Summary.actual_rps
                successful_http_requests = $k6Summary.successful_http_requests
                successful_http_rps = $k6Summary.successful_http_rps
                successful_business_requests = $k6Summary.successful_business_requests
                successful_business_rps = $k6Summary.successful_business_rps
                target_rate_achieved_ratio = if ($rate -gt 0 -and $null -ne $k6Summary.actual_rps) { [math]::Round(([double]$k6Summary.actual_rps / [double]$rate), 4) } else { $null }
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
                dead_letter_detected = if ($null -ne $deadLetterPublishedDelta) { $deadLetterPublishedDelta -gt 0 } else { $null }
                dead_letter_audit_complete = if ($null -ne $deadLetterAuditPendingDelta) { $deadLetterAuditPendingDelta -eq 0 } else { $null }
                dead_letter_clean_run = if ($null -ne $deadLetterPublishedDelta -and $null -ne $deadLetterAuditFailuresDelta) { ($deadLetterPublishedDelta -eq 0 -and $deadLetterAuditFailuresDelta -eq 0) } else { $null }
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
            if ($runProcessingError) {
                throw "本轮监控过程异常，结果不纳入有效基线：$runProcessingError"
            }
            if ($postloadCaptureError) {
                throw "本轮 postload 指标采集异常，结果不纳入有效基线：$postloadCaptureError"
            }
            if ($prometheusRangeError) {
                throw "本轮 Prometheus query_range 数据不完整，结果不纳入有效基线：$prometheusRangeError"
            }
            if ($summaryError) {
                throw "本轮 k6 summary 无效，结果不纳入有效基线：$summaryError"
            }
            if ($null -eq $exitCode) {
                throw "k6 退出状态无法确认。本轮已完成排空并保存数据，但不能计入有效性能测试。"
            }
            if ($exitCode -ne 0 -and $exitCode -ne 99) {
                throw "k6 返回异常退出码：$exitCode"
            }
            if ($null -eq $deadLetterPublishedDelta -or $null -eq $deadLetterAuditFailuresDelta) {
                throw "本轮死信 Counter 快照不可用；该轮结果不能纳入基线，停止后续矩阵：$runId"
            }
            if ($deadLetterPublishedDelta -gt 0 -or $deadLetterAuditFailuresDelta -gt 0) {
                throw "本轮虽然完成排空，但产生死信/审计失败：published=$deadLetterPublishedDelta, auditFailures=$deadLetterAuditFailuresDelta。该轮标记为异常并停止后续矩阵：$runId"
            }
            $thresholdLabel = if ($null -eq $k6Summary.thresholds_failed) { "THRESHOLD_UNKNOWN" } elseif ($k6Summary.thresholds_failed) { "THRESHOLD_FAILED" } else { "THRESHOLD_PASS" }
            $thresholdColor = if ($k6Summary.thresholds_failed -eq $true) { "Yellow" } else { "Green" }
            Write-Host ("完成 {0}: status={1}, actualRPS={2}, successfulHttpRPS={3}, p95={4}ms, p99={5}ms, businessFail={6}, dropped={7}, promSamples={8}, drain={9}s" -f `
                $runId, $thresholdLabel, $k6Summary.actual_rps, $k6Summary.successful_http_rps, $k6Summary.http_req_duration_p95_ms, `
                $k6Summary.http_req_duration_p99_ms, $k6Summary.business_failure_rate, $k6Summary.dropped_iterations, `
                $prometheusRangeSummary.samples_written, $drain.seconds) -ForegroundColor $thresholdColor
            if ($CooldownSeconds -gt 0) { Start-Sleep -Seconds $CooldownSeconds }
        }
    }
}
catch {
    $abortReason = $_.Exception.Message
    Write-Warning "矩阵中止：$abortReason"
    if ($null -ne $script:PendingDrain) {
        Invoke-PendingDrainCleanup -Reason "matrix_exception" | Out-Null
    }
}

# Last chance before packaging: do not let an exception path silently leave a run pending.
if ($null -ne $script:PendingDrain) {
    Invoke-PendingDrainCleanup -Reason "before_final_package" | Out-Null
}

# ------------------------------
# Final package and README notes
# ------------------------------
$finalStatus = if ($abortReason) { "partial_or_aborted" } else { "completed" }
if ($abortReason) { $script:FinalExitCode = 2 }
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
- manifest.json: test configuration, machine, software versions, Git commit, hashes of local input files, Prometheus job/step, and Docker context.
- run-index.csv: one row per test round; compare actual_rps, p95/p99, failures, dropped iterations, consumer deltas, backlog and drain time.
- k6-metrics.csv: all k6 summary metric values for each run.
- baseline/: idle host and Actuator Prometheus baseline samples.
- Rxx-.../: per-run k6 summary/logs, Prometheus query_range time series, Windows host samples, Docker stats and drain-check records.
- completion.json: whether all runs completed or the matrix stopped early.
- emergency-cleanup.json: best-effort stop/drain record when a run exits unexpectedly.
- application-logs/backend-app.log: optional copy of the Spring Boot log if -AppLogPath was supplied.

Important interpretation rules:
1. target_rps is the scheduled arrival rate. Use actual_rps, successful_http_rps, successful_business_rps and dropped_iterations to judge the achieved load. P99 is exported explicitly via --summary-trend-stats. k6 threshold failures are read from summary.metrics.*.thresholds, not inferred from the process exit code alone.
2. The benchmark k6 copy emits BENCHMARK_LOAD_STARTED on its first actual load iteration. Windows host samples before that marker are phase=setup and after it are phase=load. Application metrics are exported after the run from Prometheus TSDB query_range using the actual load-marker/end timestamps; the query step should match the Prometheus scrape interval.
3. Prefer http_req_duration{name:like_toggle} for API latency. If absent, the index falls back to aggregate http_req_duration and marks the actual-RPS source similarly; this fallback may include setup/login requests.
4. The k6 API latency is request acceptance latency, not MySQL persistence latency. Use consumer_messages_delta, RabbitMQ queue/Stream gauges, batch duration metrics, and drain time to analyze asynchronous persistence.
5. A non-empty dead queue, non-zero failures/retries, or business failure rate is a reliability signal, not just a performance number.
6. Results are relative comparisons under this exact hardware/software configuration. Do not present the measured RPS as a universal server capacity claim.
7. users.local.json and targets.local.json are intentionally not included. Their SHA-256 hashes are in manifest.json so the local test inputs can be identified without exposing credentials or target IDs.
8. Prometheus query_range samples include every time series scraped under job=picture-backend, including JVM, process/system CPU, datasource, Spring RabbitMQ, and custom consumer metrics when exposed. query_range retrieves historical samples and does not repeatedly scrape Actuator from the load-controller loop.
9. host-timeseries.csv describes the Windows machine running this script/k6. If backend is on another host, use system_cpu_usage / process_cpu_usage in Prometheus for backend CPU where available. Docker stats use the selected Docker context; configure -DockerContext to sample the VM Docker Engine over a secure SSH Docker context when applicable.

The ZIP is intended to be sent back for analysis. Do not add users.local.json, passwords, session cookies, database dumps or production data. If an application log was copied, inspect it for secrets before sharing.
"@ | Set-Content -Path $readmePath -Encoding UTF8

$zipPath = Join-Path $outputRootPath "community-like-benchmark-$timestampName.zip"
if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
Compress-Archive -Path (Join-Path $script:SessionDir "*") -DestinationPath $zipPath -CompressionLevel Optimal

if ($abortReason) {
    Write-Warning "已打包部分结果：$zipPath"
}
else {
    Write-Host "所有压测完成并已打包：$zipPath" -ForegroundColor Green
}
 }
finally {
    # 如果矩阵异常处理之外仍遗留运行中的压测，先停止 k6 并尽力排空，再恢复 PowerShell 状态。
    if ($null -ne $script:PendingDrain) {
        $script:NeedsRepackageAfterCleanup = $true
        try { Invoke-PendingDrainCleanup -Reason "script_finally" | Out-Null }
        catch { Write-Warning "脚本最终清理失败：$($_.Exception.Message)" }
    }

    foreach ($environmentName in $script:ManagedEnvironmentVariables) {
        $snapshot = $script:EnvironmentSnapshot[$environmentName]
        if ($snapshot.Exists) {
            [Environment]::SetEnvironmentVariable($environmentName, $snapshot.Value, "Process")
        }
        else {
            [Environment]::SetEnvironmentVariable($environmentName, $null, "Process")
        }
    }

    if ($script:LocationPushed) {
        Pop-Location
        $script:LocationPushed = $false
    }
}

if ($script:NeedsRepackageAfterCleanup -and (Test-Path $zipPath) -and (Test-Path $script:SessionDir)) {
    try {
        Remove-Item -LiteralPath $zipPath -Force
        Compress-Archive -Path (Join-Path $script:SessionDir "*") -DestinationPath $zipPath -CompressionLevel Optimal
        Write-Host "已将最终异常清理记录重新写入压缩包：$zipPath" -ForegroundColor Yellow
    }
    catch {
        Write-Warning "最终压缩包刷新失败，目录中的原始结果仍保留：$($_.Exception.Message)"
    }
}

if ($script:FinalExitCode -ne 0) {
    exit $script:FinalExitCode
}
