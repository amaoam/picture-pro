# 指标字典：看到曲线后，下一步该检查什么

## 1. 新增 Outbox 调度指标

所有指标都带有限的 `worker=worker-1...worker-N` 标签，没有用户 ID 或 target ID，避免 Prometheus 时序数量暴涨。

| Micrometer 名称 | Prometheus 名称 | 意义 |
|---|---|---|
| `community.like.outbox.polls` | `community_like_outbox_polls_total` | worker 调度执行次数 |
| `community.like.outbox.polls.empty` | `community_like_outbox_polls_empty_total` | 本轮没有处理到任何事件的次数 |
| `community.like.outbox.polls.failed` | `community_like_outbox_polls_failed_total` | 本轮调度执行遇到异常的次数 |
| `community.like.outbox.poll.events.processed` | `community_like_outbox_poll_events_processed_total` | 按 worker 计的成功事件数 |
| `community.like.outbox.poll.duration` | `community_like_outbox_poll_duration_seconds_*` | 一轮处理/错误记录所花时间；不是纯 SQL 耗时 |
| `community.like.outbox.poll.delay.ms` | `community_like_outbox_poll_delay_ms` | worker 计划等待到下一轮的毫秒数 |

现有 V2.1 指标继续使用：

- `community_like_outbox_pending`：待处理事件数。
- `community_like_outbox_dead`：死信事件数。
- `community_like_outbox_oldest_pending_seconds`：最老待处理事件年龄。
- `community_like_outbox_events_processed_total`：成功处理事件总数。
- `community_like_outbox_batches_failed_total`：批处理失败总数。
- `community_like_outbox_batch_duration_seconds_*`：批次耗时计时器。

Micrometer 名称在 Prometheus 导出时把点号转换成下划线，并对 Counter/Timer 使用各自的 `_total`、`_seconds_sum`、`_seconds_count` 等后缀。查不到指标时，可以直接打开 `/api/actuator/prometheus`，搜索上述关键字确认实际名称。

## 2. 常见 PromQL（复制到 Prometheus 或 Grafana Explore）

**服务当前是否健康：**

```promql
up{job="picture-backend"}
```

**接口 RPS：**

```promql
sum(rate(http_server_requests_seconds_count{job="picture-backend",uri=~".*(like|community).*"}[1m]))
```

**接口 P95：**

```promql
histogram_quantile(0.95, sum by (le) (rate(http_server_requests_seconds_bucket{job="picture-backend",uri=~".*(like|community).*"}[5m])))
```

**Outbox 待处理量：**

```promql
community_like_outbox_pending{job="picture-backend"}
```

**Outbox 消费速率：**

```promql
sum(rate(community_like_outbox_events_processed_total{job="picture-backend"}[1m]))
```

**空轮询比例：**

```promql
100 * sum(rate(community_like_outbox_polls_empty_total{job="picture-backend"}[5m])) / clamp_min(sum(rate(community_like_outbox_polls_total{job="picture-backend"}[5m])), 0.001)
```

**连接池等待线程数：**

```promql
sum(hikaricp_connections_pending{job="picture-backend"})
```

## 3. 三条诊断路径

### 路径 A：请求 P95 上升

1. 看 HTTP 5xx 和 k6 业务失败率。
2. 看 `hikaricp_connections_pending` 是否上升。
3. 看数据库慢查询、CPU、锁等待。
4. 若这些都正常，再检查应用 CPU、Tomcat 当前线程、GC 和日志量。

### 路径 B：待处理事件数长期上升

1. 比较新增事件速率与消费事件速率。
2. 看 `oldest_pending_seconds` 是否持续上升。
3. 看 worker 批次耗时、空轮询比例和失败数。
4. 查询是否有死信阻塞同一目标的后续事件。
5. 如果只在单热点发生、多目标基本正常，进一步查同目标行锁与顺序消费限制。

### 路径 C：空轮询比例很高

- **空闲时高：正常。** 系统没事件，worker 退避到 500 ms，比例高并不是缺陷。
- **积压量接近 0 时高：预期。** 不需要把空轮询比例降到 0。
- **积压持续上升时仍高：需要细看。** 可能有 worker 锁冲突、事件被 retry/dead 状态阻塞，或查询范围没有找到可处理事件。检查日志和死信，不要只调低轮询间隔。

## 4. 重要限制

- `http_server_requests_seconds_*` 只有在 Spring Boot Web/Micrometer 自动配置正常时才出现。
- Timer 的 `_bucket` 需要开启 histogram。配置片段已经启用 HTTP、Outbox 批次和 worker 轮询定时器的百分位直方图。
- Hikari 指标只有使用 HikariCP 且数据源自动配置暴露正常时才出现。
- `process_cpu_usage` 是 JVM 进程 CPU，而不是整台电脑 CPU。
- Prometheus 默认按 15 秒 scrape，因此小于 15 秒的尖峰可能只看到部分走势。对压测定位通常够用；若要更高频监控，可以改为 5 秒，但会增加抓取与存储压力。
- V2.1 的 pending/dead/oldest 三个 Gauge 通过 Mapper 读取数据库；每次 Prometheus scrape 会触发这些 Gauge 取值。保持 15 秒抓取间隔，不要为了画图改成 1 秒；后续若发现采集查询本身成为负担，可以再把这几个 Gauge 改成定时刷新缓存。
