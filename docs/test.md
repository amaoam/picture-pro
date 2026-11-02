1.生成死信感知版 Benchmark
Set-ExecutionPolicy Bypass -Scope Process
cd C:\Users\ZMT\Desktop\project\picture-backend-pro
.\Patch-Benchmark-CommunityLike.ps1
成功后会生成：

```text
Benchmark-CommunityLike-DeadLetterAware.ps1
```
2.两个 PowerShell 窗口启动流程

### 窗口 A：启动 Java

Stop-Process -Name java -Force -ErrorAction SilentlyContinue
Stop-Process -Name k6 -Force -ErrorAction SilentlyContinue

启动 Java：

```powershell
$javaExe = "D:\CODE\JAVA\bin\java.exe"
$jarPath = "C:\Users\ZMT\Desktop\project\picture-backend-pro\target\picture-backend-pro-0.0.1-SNAPSHOT.jar"
$javaArgs = "-Djava.net.preferIPv4Stack=true -XX:ActiveProcessorCount=8 -Xms2g -Xmx2g -jar $jarPath"
$p = Start-Process -FilePath $javaExe -ArgumentList $javaArgs -PassThru -NoNewWindow
Start-Sleep -Seconds 1
# 0xFF 即 255
$p.ProcessorAffinity = 0xFF
Write-Host "Java 启动成功！绑定 0-7 核" -ForegroundColor Green
```
如果 JAR 尚未重新打包：

```powershell
mvn -DskipTests clean package

```

测试账号和目标数据

当前 k6 脚本 `k6/community_like_mq_benchmark.js` 在 `setup()` 中会登录 `users.local.json` 的所有账号，并要求账号数不少于 `MAX_VUS`。

例如 `MAX_VUS=400`，至少需要 400 个独立测试账号：

```powershell
cd C:\Users\ZMT\Desktop\project\picture-backend-pro
.\scripts\New-LoadTestUsers.ps1 -Count 400
```

这会在本机生成 `k6/users.local.json`。该文件含测试账号密码，不要提交 Git，不要分享压测包中的本地凭证。

确认 `k6/targets.local.json` 使用真实存在的目标 ID，格式如下：

```json
{
  "hotTargetId": "替换成真实帖子ID",
  "targetIds": [
    "替换成真实帖子ID1",
    "替换成真实帖子ID2",
    "替换成真实帖子ID3",
    "替换成真实帖子ID4"
  ]
}
```


### 窗口 B：先检查 k6，再运行 Benchmark

```powershell
Set-ExecutionPolicy Bypass -Scope Process
cd C:\Users\ZMT\Desktop\project\picture-backend-pro
k6 version
Test-Path .\k6\community_like_mq_benchmark.js
Test-Path .\k6\users.local.json
Test-Path .\k6\targets.local.json
```

第一次建议先做一次轻量冒烟（单独运行原 k6 脚本），例如：

```powershell
$env:BASE_URL = "http://localhost:8123/api"
$env:TARGET_TYPE = "POST"
$env:TARGET_MODE = "single"
$env:RATE = "10"
$env:DURATION = "30s"
$env:PRE_VUS = "20"
$env:MAX_VUS = "30"
$env:HOT_RATIO = "0.8"
k6 run .\k6\community_like_mq_benchmark.js
```

注意：k6 的 `setup()` 会读取完整账号文件。即使 `MAX_VUS=30`，账号文件也可以有 400 个账号；k6 脚本会登录 `users.local.json` 中的所有账号，因此 setup 时长可能较长。此冒烟只检查流程，不纳入正式性能统计。

## 6. 正式压测命令

建议先使用 1 次重复、较低速率验证补丁和输出结果：

```powershell
.\Benchmark-CommunityLike-DeadLetterAware.ps1 `
  -ProjectPath "C:\Users\ZMT\Desktop\project\picture-backend-pro" `
  -BaseUrl "http://localhost:8123/api" `
  -K6Script ".\k6\community_like_mq_benchmark.js" `
  -K6Cores "8-13" `
  -Duration "30s" `
  -PreVus 100 `
  -MaxVus 200 `
  -Rates @(10, 50, 100) `
  -Repetitions 1 `
  -DrainTimeoutSeconds 300 `
  -DrainStableSamples 5 `
  -DrainPollSeconds 2 `
  -TargetType "POST"
```

验证正确后，再运行正式矩阵：

```powershell
.\Benchmark-CommunityLike-DeadLetterAware.ps1 `
  -ProjectPath "C:\Users\ZMT\Desktop\project\picture-backend-pro" `
  -BaseUrl "http://localhost:8123/api" `
  -K6Script ".\k6\community_like_mq_benchmark.js" `
  -K6Cores "8-13" `
  -Duration "60s" `
  -PreVus 200 `
  -MaxVus 400 `
  -Rates @(100, 300, 500, 800, 1000) `
  -Repetitions 3 `
  -DrainTimeoutSeconds 300 `
  -DrainStableSamples 5 `
  -DrainPollSeconds 2 `
  -CooldownSeconds 5 `
  -TargetType "POST"
```
这会执行 3 种目标分布 × 5 档 RPS × 3 次重复，共 45 组，每组 60 秒。
正式负载时间至少 45 分钟，不包含账号登录 setup、排空时间和冷却时间。
每轮顺序由脚本随机化，并在 manifest 中记录随机种子。
结果文件在哪里、先看什么

默认输出在项目的 `benchmark-results` 目录下，每次运行创建独立时间戳目录，典型文件：

```text
benchmark-results/<session-id>/
  manifest.json
  run-index.csv
  k6-metrics.csv
  baseline/
  R01-Oxx-.../
    k6-summary.json
    k6-stdout.log
    k6-stderr.log
    prometheus-timeseries.csv
    drain-check.csv
    run-metadata.json
  ...
  <session-id>.zip
```

建议依次看：

1. `manifest.json`：确认 commit、环境、脚本 hash、请求配置、随机 seed。
2. `run-index.csv`：跨组比较实际 RPS、P95/P99、失败率、排空时间、死信数。
3. 每组 `run-metadata.json`：重点看 `dead_letter_published_delta`、`dead_letter_audit_persisted_delta`、`dead_letter_audit_failures_delta`、`dead_letter_audit_complete`、`dead_letter_clean_run`。
4. `prometheus-timeseries.csv`：分析负载期间 Stream、主队列、Retry、DLQ 深度和 JVM/数据库指标。
5. `drain-check.csv`：看队列归零和审计 Counter 追平过程。
6. `k6-stdout.log` / `k6-stderr.log`：只在业务失败、k6 阈值失败或运行异常时查看。脚本会对日志中的测试账号名做脱敏。

3.如何判定一组实验有效

建议将“k6 负载达标”和“异步链路无异常”分开判断。

| 维度 | 正常基线要求 |
|---|---|
| HTTP 失败率 | < 1% |
| 业务失败率 | < 1% |
| like API P95 | < 500 ms |
| like API P99 | < 1000 ms |
| dropped iterations | 0 |
| Redis Stream / Pending | 最终为 0 |
| RabbitMQ 主 / Retry / DLQ | 最终均为 0 |
| consumer in-flight | 最终为 0 |
| DLQ published 增量 | 0 |
| DLQ audit failures 增量 | 0 |
| 审计追平 | `persisted_delta >= published_delta` |

注意：原 k6 threshold 的 p95/p99 是 HTTP API 响应时延；API 返回成功表示 Redis 命令被接受，不表示 MySQL 已经完成持久化。每轮排空后还要核对数据库消费流水、点赞关系和 `likeCount`。

数据一致性抽查

每轮完成后可按业务测试窗口抽查：

```sql
SELECT COUNT(*)
FROM community_like_event_consume
WHERE processed_at >= ? AND processed_at < ?;
```

并抽查具体目标：

```sql
SELECT id, likeCount FROM community_post WHERE id = ?;
SELECT COUNT(*) FROM community_post_like WHERE postId = ?;
```

图片区分使用 `picture` 与 `picture_like`。注意消费流水表记录的是已处理事件数，关系表是最终状态；有 LIKE/UNLIKE 抵消时，不能把事件数直接等同最终点赞关系数。

重复实验如何汇总

同一 `mode + rate` 的 3 次结果，优先比较中位数：

- actual RPS 中位数；
- P95/P99 中位数；
- 最大队列积压；
- 排空耗时中位数；
- JVM CPU、进程 CPU、堆使用率、Hikari pending；
- 死信与审计失败必须为 0。

若某一轮发生死信，不要将其 RPS 放进“正常吞吐”的中位数；保留为异常样本，单独分析原因。

4.异常时怎么处理

### 情况 A：DLQ 深度大于 0，但 audit persisted 追平中

不要清空 DLQ，也不要开始下一组。看审计消费者日志：

```text
LIKE_DEAD_LETTER_AUDITED
LIKE_DEAD_LETTER_AUDIT_PERSIST_FAILED
```

检查审计消费者是否启动、MySQL 是否可写、`community_like_dead_letter_audit` 字段是否完整。

### 情况 B：DLQ 深度为 0，但 published > persisted

这通常代表消息可能已经投递给审计消费者但还未完成写库/ACK，或者 Counter 之间存在异常差额。继续观察；超过 DrainTimeoutSeconds 仍未追平就停止矩阵并检查 RabbitMQ `messages_unacknowledged`、审计消费者日志和 MySQL。

### 情况 C：Retry Queue 长期不归零

检查 Retry Queue TTL、DLX、重试 Exchange、routing key 和主队列绑定。不要通过直接 Purge Retry Queue 来“让排空通过”，否则会丢点赞事件。

### 情况 D：Drain 超时

不要运行下一组。先保存实验目录和日志，检查 Stream pending、Relay 错误、主队列积压、MySQL 事务失败、审计入库失败。如果需要恢复，先明确每条消息当前处于 Redis Stream、主队列、Retry Queue 还是 DLQ，再决定处理方式。

### 情况 E：出现死信但审计成功

说明审计闭环正常，不说明业务处理成功。先从 `community_like_dead_letter_audit` 取出 eventId 和 error_message，再查 Java 首次失败日志、`community_like_event_consume` 和点赞关系。修复业务根因后，单独进行受控重放测试，不要把 DLQ 全量回灌主队列。
