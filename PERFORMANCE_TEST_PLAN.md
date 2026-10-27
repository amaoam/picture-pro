# V2.1 点赞链路压测手册（Windows / PowerShell / k6）

这份文档面向第一次做性能测试的人。请按顺序做，不需要一开始就冲到 1000 次/秒。

## 0. 先理解这次要测什么

- **吞吐量/RPS**：每秒系统接到多少次 HTTP 请求。本项目的 k6 脚本每次迭代只发一个点赞或取消点赞请求，因此 `RATE=1000` 表示目标约 1000 次请求/秒。
- **响应时间**：请求从发出到收到响应花了多久。P95 表示 95% 的请求不超过这个时间，P99 表示 99% 不超过这个时间。
- **错误率**：HTTP 错误或业务响应 `code != 0` 的比例。
- **Dropped iterations**：k6 想按设定速率启动迭代，但没有空闲虚拟用户时被丢弃的迭代。出现它不一定是服务端唯一瓶颈，也可能是压测机 VU 数不足。
- **Outbox 积压**：点赞关系同步写入成功，但计数/获赞统计还没被后台消费者处理的事件。测试稳定期间，最老待处理事件不应持续增长。

性能测试不是“看一次跑出多少数字”，而是逐渐加压、找出系统开始退化的位置，再确认改动有没有改善。

## 1. 安全准备（必须）

1. **只在本地或专用测试环境执行**，不要向线上生产库打 1000 RPS。
2. 备份测试数据库，并确认 COS、Redis、MySQL 都指向测试环境。
3. 确认 V2 Outbox 迁移已经完成、`picture.likeCount` 已有字段，历史计数回填完成。
4. 在切换 V2.1 前确认旧版同步点赞写入逻辑不再被其他 Controller/任务调用。不要让旧同步链路与新 Outbox 链路同时维护同一个计数。
5. 压测会在测试帖子/图片上反复点赞和取消点赞，也会产生 Outbox 事件和获赞经验流水。使用专门的测试账号与测试目标。
6. 测试目标的作者不能是压测账号之一；否则“不能给自己的帖子/图片点赞”会产生业务错误。

## 2. 应用代码接入与本地编译

1. 先把当前项目复制成备份分支或另一个目录。
2. 根据压缩包根目录的 `README.md`，逐个对比/替换 `CommunityLikeOutboxEvent.java`、`CommunityLikeOutboxMapper.java`、`CommunityLikeOutboxProcessor.java`，新增 `CommunityLikeOutboxMetrics.java` 和测试类。
3. 合并 `pom.xml` 中新增的 Actuator 与 Prometheus 依赖，以及 `application.yml` 中 `management` 和 Outbox 配置。不要不经对比就覆盖自己的 profile/密钥配置；`application-local.yml` 可能是本地文件，不在 Git 中。
4. 在 IDEA Maven 工具窗口执行 `clean`、`test`，或在项目根目录运行：

   ```powershell
   mvn clean test
   ```

5. 先只运行单元测试：

   ```powershell
   mvn -Dtest=CommunityLikeOutboxProcessorOrderTest test
   ```

6. 测试通过后启动本地后端。确认应用正常登录、点赞、取消点赞，且没有重复消费/死信异常。
7. 压测前关闭 MyBatis 每条 SQL 的标准输出。当前项目可能在 `application-local.yml` 中配置了 `StdOutImpl`，将它在本地测试配置中改成：

   ```yaml
   mybatis-plus:
     configuration:
       log-impl: org.apache.ibatis.logging.nologging.NoLoggingImpl
   ```

   否则大量 SQL 日志会影响压测结果。

## 3. 确认监控接口正常

应用端口和 context path 仍按项目配置。默认本地地址：`http://localhost:8123/api`。

在 PowerShell 运行：

```powershell
Invoke-RestMethod http://localhost:8123/api/actuator/health
Invoke-WebRequest http://localhost:8123/api/actuator/prometheus | Select-Object -ExpandProperty StatusCode
```

Prometheus 文本中应能搜索到以下指标（名字会将点转换为下划线）：

- `community_like_outbox_pending`
- `community_like_outbox_dead`
- `community_like_outbox_oldest_pending_seconds`
- `community_like_outbox_events_processed_total`
- `community_like_outbox_batches_failed_total`
- `community_like_outbox_batch_duration_seconds`

**安全提示：** Actuator/Prometheus 端点不要直接开放到公网。开发环境先用 localhost 检查；生产环境要加防火墙、内网访问策略或认证。

## 4. 安装 k6

打开 PowerShell：

```powershell
winget install k6 --source winget
```

安装后关闭并重新打开终端，再确认：

```powershell
k6 version
```

如果你不能使用 winget，可以从官方安装页下载安装程序：
https://grafana.com/docs/k6/latest/set-up/install-k6/

## 5. 创建压测用户

1. 确认本地后端已启动，并且注册接口可用。
2. 在压缩包根目录打开 PowerShell。先做小规模测试，例如创建 20 个测试账号：

   ```powershell
   powershell -ExecutionPolicy Bypass -File .\scripts\New-LoadTestUsers.ps1 -Count 300
   ```

3. 脚本会调用 `POST /api/user/register` 创建专用账号，并生成 `k6\users.local.json`。这个文件包含测试密码，已被 `.gitignore` 排除，不要提交或分享。
4. 如果以后要设置 `MAX_VUS=100`，至少要创建 100 个账号；`MAX_VUS=300` 就需要 300 个账号。脚本会在登录阶段为每个账号建立独立 SESSION Cookie，避免所有 VU 共享同一个用户导致大量重复点赞请求。
5. 若运行脚本时提示 SESSION Cookie 不存在，请先在浏览器成功登录，打开开发者工具的 Application/Storage → Cookies，确认 Cookie 名称；再设置 `SESSION_COOKIE_NAME`。当前 Spring Session 常见默认名是 `SESSION`，但以实际响应为准。

## 6. 选一个测试目标 ID

从自己的开发库中选择已发布、未删除的帖子，目标作者不能是压测账号：

```sql
SELECT id, userId, title, likeCount
FROM community_post
WHERE status = 1 AND isDelete = 0
ORDER BY id DESC
LIMIT 20;
```

或者选择已审核通过的公共图库图片：

```sql
SELECT id, userId, name, likeCount
FROM picture
WHERE spaceId IS NULL AND reviewStatus = 1 AND isDelete = 0
ORDER BY id DESC
LIMIT 20;
```

记下目标 `id`。同一次测试固定一个目标，能更容易观察热点目标上的计数竞争。请不要使用真实用户重要内容作为压测目标。

## 7. 第一次小测试（1 次/秒）

在压缩包根目录运行以下命令。把 `123456` 替换成测试帖子 ID：

```powershell
$env:BASE_URL = "http://localhost:8123/api"
$env:TARGET_TYPE = "POST"
$env:TARGET_ID = "2112391798298521601"
$env:RATE = "1000"
$env:DURATION = "3m"
$env:PRE_VUS = "200"
$env:MAX_VUS = "300"
k6 run .\k6\community_like.js --summary-export .\results\summary-1rps.json
```

观察是否有登录失败、业务码非 0、HTTP 错误或 `dropped_iterations`。如果 1 RPS 都失败，不要继续加压，先解决登录/目标权限/路由问题。

## 8. 分阶段提升负载

每个阶段完成后先看结果与 Outbox 积压；低档位稳定后再进入下一个档位。建议先创建 100 个账号，最大 VU 设置不超过 100。

| 阶段 | RATE（次/秒） | DURATION | PRE_VUS / MAX_VUS | 目的 |
|---|---:|---|---|---|
| 1 | 1 | 30s | 2 / 5 | 校验脚本、登录和路由 |
| 2 | 10 | 1m | 5 / 20 | 检查基本稳定性 |
| 3 | 50 | 1m | 10 / 50 | 建立低负载基线 |
| 4 | 100 | 2m | 20 / 100 | 观察 SQL/连接池与消费延迟 |
| 5 | 300 | 2m | 50 / 100 | 找到吞吐退化点 |
| 6 | 500 | 2m | 80 / 150 | 检查消费者积压是否增长 |
| 7 | 1000 | 3m | 100 / 200 | 目标负载测试（先创建至少 200 个账号） |

以 100 RPS 为例，只需改环境变量并重跑：

```powershell
$env:RATE = "100"
$env:DURATION = "2m"
$env:PRE_VUS = "20"
$env:MAX_VUS = "100"
k6 run .\k6\community_like.js --summary-export .\results\summary-100rps.json
```

1000 RPS 阶段需要至少 200 个测试账号时，先生成：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\New-LoadTestUsers.ps1 -Count 200
```

再设置 `RATE=1000`、`PRE_VUS=100`、`MAX_VUS=200` 后运行。注意：k6 的到达率执行器按目标速率启动迭代；如果 VU 不够，会出现 dropped iterations，因此不能只看配置的 RATE 就认定实际达到该 RPS。

## 9. 如何判断结果

本包脚本先采用以下**起始验收目标**，不是性能保证：

- `http_req_failed` 小于 1%。
- `business_failures` 小于 1%（HTTP 200 但 `code != 0` 也算业务失败）。
- P95 响应时间低于 500ms，P99 低于 1000ms。
- `dropped_iterations` 为 0。
- 稳定压测时 Outbox 最老 pending 年龄尽量保持在 2 秒内，pending 数不应持续单向增长。
- 停止发压后等待消费者追平；`status=0` 应回落到 0 或接近 0，`status=2` 应为 0（若非 0，查看并处理死信）。
- 运行 `sql/like_v21_monitoring.sql` 的第 3、4 条差异查询，完成追平后不应持续返回计数不一致记录。

结果解读：

- P95/P99 变差但 dropped=0：服务端已经慢下来，查看 MySQL 慢 SQL、锁等待、连接池和 Outbox 查询。
- dropped_iterations > 0：k6 的 VU 配额可能不够，或压测机已经跑满。先提高 max VUs/压测机资源，不能直接归因到服务端。
- pending 数持续增长、老事件年龄超过 2 秒：消费能力低于生产速度。可在一次只改一个参数的前提下测试 `target-batch-size`、`max-events-per-target`、`poll-delay-ms`，并观察 DB 锁等待。
- dead > 0：查看死信错误文本。不要无脑重置 status；目标上死信会阻止后续事件，是为了避免计数越序。
- 计数 SQL 持续查出差异：停止加压，先修复一致性问题。

## 10. 检查 Outbox 和点赞数

执行：

```sql
SELECT status, COUNT(*) AS eventCount
FROM like_event_outbox
GROUP BY status;

SELECT COALESCE(TIMESTAMPDIFF(SECOND, MIN(createTime), NOW()), 0) AS oldestPendingSeconds,
       COUNT(*) AS pendingCount
FROM like_event_outbox
WHERE status = 0;
```

完整只读校验放在 `sql/like_v21_monitoring.sql`。测试停止后，先等消费者处理积压，再做计数一致性核对。

不要在尚有 pending/dead 事件时执行历史计数全量回填，也不要直接删除 pending/dead 事件。只应按保留周期分批清理已完成、且已过归档期限的 status=1 记录；上线前要确定保留天数和审计要求。

## 11. 常见问题

- **提示未登录**：登录接口没有返回预期 Cookie，确认 Cookie 名称、session/Redis 配置和 `BASE_URL`。
- **所有请求都是无权限**：确认目标是已发布帖子或审核通过的公共图片，并且目标作者不是测试用户。
- **登录失败**：检查本地用户注册是否成功、账号密码、数据库和 Redis Session 状态。
- **没有到达 RATE**：查看 `dropped_iterations`，检查 max VUs、测试账号数量以及运行 k6 的机器 CPU。
- **请求成功但计数不变化**：计数允许有短暂延迟。等 3–5 秒，再查看 pending/dead 与一致性查询；如果仍然不一致则保存结果并排查。
- **同一目标产生死信**：先读取 `lastError` 并修复数据库/业务问题；不要直接将整批事件重置为待处理。

## 12. 做对比时要保持条件相同

每次只改变一个参数。记录 Git commit、JDK、数据库版本、测试机器 CPU/RAM、目标类型、RATE、持续时间、P95/P99、错误率、dropped iterations、最大 pending 数、最老 pending 年龄、死信数、计数差异数。

尽量将压测机和被测服务分开运行。如果都在同一台电脑，1000 RPS 的结果同时受 Windows、IDEA、MySQL、Redis 和 k6 自身资源影响，只能说明这台开发机上的表现，不能外推为生产容量。

## 官方参考

- k6 Windows 安装：https://grafana.com/docs/k6/latest/set-up/install-k6/
- k6 constant-arrival-rate：https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/constant-arrival-rate/
- Spring Boot 2.7 Actuator / Prometheus：https://docs.spring.io/spring-boot/docs/2.7.x/reference/html/actuator.html
