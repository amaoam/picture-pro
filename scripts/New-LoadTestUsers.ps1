param(
    [int]$Count = 20,
    [string]$BaseUrl = "http://localhost:8123/api",
    [string]$Password = "LikeLoad@2026"
)

$ErrorActionPreference = "Stop"
if ($Count -lt 1 -or $Count -gt 1000) { throw "Count 必须在 1 到 1000 之间。" }
if ($Password.Length -lt 8) { throw "密码至少需要 8 个字符。" }

$stamp = Get-Date -Format "MMddHHmmss"
$users = @()
Write-Host "正在创建 $Count 个本地压测账号。账号用于开发/测试环境，不要在生产环境执行。" -ForegroundColor Cyan

for ($i = 1; $i -le $Count; $i++) {
    $account = ("lt{0}{1:D4}" -f $stamp, $i)
    $body = @{
        userAccount = $account
        userPassword = $Password
        checkPassword = $Password
    } | ConvertTo-Json -Compress

    try {
        $response = Invoke-RestMethod -Uri "$($BaseUrl.TrimEnd('/'))/user/register" `
            -Method Post -ContentType "application/json" -Body $body
    }
    catch {
        throw "创建账号 $account 失败。确认后端已启动、注册接口可用，并检查错误：$($_.Exception.Message)"
    }

    if ($response.code -ne 0) {
        throw "创建账号 $account 失败：$($response.message)（业务码 $($response.code)）"
    }
    $users += [PSCustomObject]@{
        userAccount = $account
        userPassword = $Password
    }
    if ($i % 10 -eq 0 -or $i -eq $Count) { Write-Host "已创建 $i / $Count" }
}

$outputPath = Join-Path $PSScriptRoot "..\k6\users.local.json"
$outputDir = Split-Path -Parent $outputPath
if (-not (Test-Path $outputDir)) { New-Item -ItemType Directory -Path $outputDir -Force | Out-Null }
$json = $users | ConvertTo-Json -Depth 4
$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[System.IO.File]::WriteAllText($outputPath, $json, $utf8NoBom)
Write-Host "完成。账号清单已写到：$outputPath" -ForegroundColor Green
Write-Host "该文件包含测试密码，已由 k6/.gitignore 排除，不要分享或提交。"
