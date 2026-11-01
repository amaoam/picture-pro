param(
    [string]$JavaExe = "D:\CODE\JAVA\bin\java.exe",
    [string]$ProjectPath = "C:\Users\ZMT\Desktop\project\picture-backend-pro",
    [string]$JarPath = "C:\Users\ZMT\Desktop\project\picture-backend-pro\target\picture-backend-pro-0.0.1-SNAPSHOT.jar",
    [switch]$KillExisting
)

$ErrorActionPreference = "Stop"
if ($KillExisting) {
    Write-Warning "将结束本机所有 java.exe 与 k6.exe 进程；先确认没有其他项目正在运行。"
    Stop-Process -Name java -Force -ErrorAction SilentlyContinue
    Stop-Process -Name k6 -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
}
if (-not (Test-Path $JavaExe)) { throw "找不到 Java 可执行文件：$JavaExe" }
if (-not (Test-Path $JarPath)) { throw "找不到 JAR：$JarPath。请先在项目目录打包。" }
if (-not (Test-Path $ProjectPath)) { throw "找不到项目目录：$ProjectPath" }

Set-Location $ProjectPath
$javaArgs = "-Djava.net.preferIPv4Stack=true -XX:ActiveProcessorCount=8 -Xms2g -Xmx2g -jar `"$JarPath`""
$p = Start-Process -FilePath $JavaExe -ArgumentList $javaArgs -PassThru -NoNewWindow
Start-Sleep -Milliseconds 800
try {
    # 0xFF = 二进制 11111111，即绑定逻辑 CPU 0-7。
    $p.ProcessorAffinity = [IntPtr]0xFF
    Write-Host "Java 启动成功！PID=$($p.Id)，绑定逻辑 CPU 0-7 (0xFF)" -ForegroundColor Green
}
catch {
    Write-Warning "Java 已启动，但设置 ProcessorAffinity 失败：$($_.Exception.Message)"
}
Write-Host "请检查服务日志/Actuator，确认 RabbitMQ 模式和 RabbitMQ 连接均正常。" -ForegroundColor Cyan
