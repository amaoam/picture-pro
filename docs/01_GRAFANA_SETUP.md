# 教程 1：从零搭建 Prometheus + Grafana（Windows / Docker Desktop）

本文假设：后端目前在 Windows 主机运行，地址 `http://localhost:8123/api`；MySQL/Redis 已按你原来的方式运行；你不需要先掌握 PromQL。

## 一、先理解三个组件

- **Spring Boot Actuator + Micrometer**：应用把接口耗时、JVM、连接池和点赞 Outbox 指标暴露成一个 HTTP 页面。
- **Prometheus**：每隔 15 秒访问该页面并保存时间序列，方便查询“随时间变化的数值”。
- **Grafana**：连接 Prometheus，把时间序列画成曲线、数字卡片和仪表盘。

Grafana 不会自动从 Java 代码里猜出业务指标。必须先确保后端端点能访问、Prometheus 抓取得到、Grafana 数据源连接正常，最后仪表盘才会有数据。

## 二、合并后端配置

### 1. 确认依赖

检查你的 `pom.xml` 是否已有以下两个依赖。V2.1 已经添加过；如已存在，不要重复添加。

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

### 2. 合并配置片段

打开 `config/application-observability-snippet.yml`，把 `management.metrics.distribution.percentiles-histogram` 和 `community.like.outbox` 配置合并到你现有的 `application.yml` 或 `application-local.yml`。

**不要把片段当成完整 `application.yml` 覆盖原配置。** 必须保留你的数据库、Redis、COS、AI API 等配置。

本包建议的核心参数：

```yaml
community:
  like:
    outbox:
      workers: 4
      idle-min-poll-delay-ms: 25
      idle-max-poll-delay-ms: 500
      busy-poll-delay-ms: 1
      error-poll-delay-ms: 1000
      poll-jitter-percent: 10
      initial-worker-stagger-ms: 25
```

同时移除旧的 `community.like.outbox.poll-delay-ms` 或确认它不再被新 scheduler 使用。`target-batch-size`、`max-events-per-target`、`max-retries` 仍使用 V2.1 的现值，不要同时调多个参数。

### 3. 启动应用并检查 Actuator

先按你平时的方法启动后端。新开 PowerShell 执行：

```powershell
$resp = Invoke-WebRequest -Uri "http://localhost:8123/api/actuator/prometheus" -UseBasicParsing
$resp.StatusCode
$resp.Content.Substring(0, [Math]::Min(1000, $resp.Content.Length))
```

正常情况：状态码为 `200`，正文包含类似 `jvm_memory_used_bytes`、`http_server_requests_seconds_count` 等指标。

常见结果：

- `200`：继续下一步。
- `404`：检查 Actuator 依赖、`management.endpoints.web.exposure.include` 和 context path `/api`。
- `401/403`：检查你的登录拦截器或权限拦截器是否拦截了 `/actuator/**`；不要直接关闭所有安全配置，应只对本地/可信监控来源做最小授权。
- 连接失败：先确认后端正在 8123 端口监听。
- 页面能打开但没有 `http_server_requests_seconds_bucket`：确认配置合并成功并重启应用；该指标直方图由 histogram 配置开启。

## 三、启动 Prometheus + Grafana

### 1. 设置 Grafana 密码

进入本包的 `config` 文件夹。复制 `.env.example` 为 `.env`，然后修改密码：

```powershell
Set-Location .\config
Copy-Item .env.example .env
notepad .env
```

把 `GRAFANA_ADMIN_PASSWORD` 改成你自己的长密码。`.env` 已被 `.gitignore` 忽略，不应提交到 Git。

### 2. 启动容器

仍在 `config` 目录：

```powershell
docker compose config
docker compose up -d
docker compose ps
```

如果 `docker compose config` 报缺少 `GRAFANA_ADMIN_PASSWORD`，说明 `.env` 没创建或变量值没有设置。

查看日志：

```powershell
docker compose logs --tail 100 prometheus
docker compose logs --tail 100 grafana
```

本示例使用固定镜像标签以保证配置可重复；它不是“最新版本声明”。升级镜像版本时建议单独升级并验证。

### 3. 确认 Prometheus 抓取成功

浏览器打开：`http://localhost:9090/targets`

找到 `picture-backend`，检查 State 是否为 `UP`。

若为 `DOWN`：

1. 点开 endpoint/error，记录错误文字。
2. 如果提示连接被拒绝：确认后端在 Windows 主机正常运行，端口为 8123。
3. 如果提示 404：确认 `metrics_path: /api/actuator/prometheus` 路径。
4. 如果提示 401/403：查看后端安全拦截器。
5. Windows Docker Desktop 场景使用 `host.docker.internal:8123`，不是容器内的 `localhost:8123`。容器里的 `localhost` 指容器自身。

在 Prometheus 页面上方的 Graph/查询栏里输入：

```promql
up{job="picture-backend"}
```

显示 `1` 表示 Prometheus 最近一次抓取成功；显示 `0` 表示目标配置存在但抓取失败；完全没有结果则检查 job 名称和配置文件。

### 4. 打开 Grafana

浏览器打开：`http://localhost:3000`。

使用 `.env` 中的用户名密码登录，默认用户名是 `admin`（可以在 `.env` 改）。本包已自动配置 Prometheus 数据源，并预置 `Community Like - Outbox & API Overview` 仪表盘。

进入 **Dashboards → Community Like → Community Like - Outbox & API Overview**。

顶部右上角时间范围先选 `Last 15 minutes`，刷新间隔设置为 `15s`。先让服务运行一两分钟，等指标积累后再看曲线。

## 四、怎样读仪表盘

### API 区域

- **点赞接口 RPS**：每秒大致收到了多少个匹配 URI 的服务端请求。它是服务端指标，不等同于 k6 期望发出的 rate。
- **P95 延迟**：95% 的样本低于这条曲线，5% 高于它。P95 上升时，查看同一时间的数据库连接池和 Outbox 积压。
- **HTTP 5xx**：服务端 5xx 响应速率。它不是完整业务失败率，业务 code 非 0 仍需看 k6 `business_failures`。

### Outbox 区域

- **待处理事件数**：未完成事件数量。稳定负载下长期上升说明生产速度高于消费速度，或者有事件被前序死信阻塞。
- **最老待处理事件年龄**：比总数更直观地衡量统计更新是否“拖太久”。持续上涨通常比一个短暂的 pending 峰值更值得关注。
- **消费速率**：成功标记完成的事件速率。只有在负载与批次都相近时，才适合和 k6 RPS 做量级比较，因为一个请求会产生 0 或 1 个事件，且业务去重规则会影响事件产出。
- **死信数**：出现死信就应该检查日志和 `like_event_outbox.lastError`；不要直接清空或把死信强制标记成功。

### 消费者与数据库区域

- **空轮询比例**：无可处理事件的轮询占比。空闲时高是正常现象；积压很多时仍然很高才值得排查。
- **批次 P95**：需要 histogram 配置生效；它不是单条 SQL 的耗时。
- **数据库连接池等待线程**：持续大于 0 表示请求/消费者想借连接但暂时拿不到。再看 active/max。
- **连接池活跃占比**：持续接近 100% 时，先查事务时长、慢 SQL 与锁等待，不要无依据地只增加池大小。

### 为什么有些面板显示 No data？

首先打开 Prometheus 主页直接查指标名。如果 Prometheus 中没有该指标，Grafana 也不会有。新轮询指标只有在 V2.2 scheduler 启动并执行至少一轮后才会出现。连接池指标依赖 HikariCP。`P95` 面板要求 histogram 开启。接口 URI 筛选也可能不匹配你实际生成的 `uri` 标签，可在 Prometheus 输入：

```promql
http_server_requests_seconds_count{job="picture-backend"}
```

查看真实的 `uri`、`method` 和 `status` 标签，再调整仪表盘里的正则。

## 五、关闭与重启

在 `config` 目录执行：

```powershell
docker compose ps
docker compose down
```

`docker compose down` 会停止并移除容器，但保留具名数据卷；不加 `-v` 就不会删除 Prometheus 历史指标和 Grafana 数据。

要看下一次测试时重新启动：

```powershell
docker compose up -d
```

## 官方参考

- Spring Boot 2.7 Actuator/Prometheus：<https://docs.spring.io/spring-boot/docs/2.7.6/reference/html/actuator.html>
- Prometheus 配置与 scrape：<https://prometheus.io/docs/prometheus/latest/configuration/configuration/>
- Grafana provisioning：<https://grafana.com/docs/grafana/latest/administration/provisioning/>
- Grafana Docker：<https://grafana.com/docs/grafana/latest/setup-grafana/installation/docker/>
