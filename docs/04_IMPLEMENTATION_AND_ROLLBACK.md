# 实施清单与回滚步骤

## 一、先备份并创建分支

```powershell
git status --short
git switch -c feature/community-like-v22-observability
```

确认当前工作区干净，且代码已处于你刚更新好的 V2.1 状态。不要直接在生产分支合并测试补丁。

## 二、复制 Java 文件

将 `src-patch` 目录里同路径文件复制到后端工程：

| 本包文件 | 覆盖 / 新增位置 |
|---|---|
| `job/CommunityLikeOutboxScheduler.java` | 覆盖当前的旧固定频率调度器 |
| `job/AdaptivePollDelayPolicy.java` | 新增 |
| `config/CommunityLikeSchedulingConfig.java` | 覆盖现有调度线程池配置 |
| `service/impl/CommunityLikeOutboxPollMetrics.java` | 新增 |
| `test/.../AdaptivePollDelayPolicyTest.java` | 新增单元测试 |

**不要覆盖 `CommunityLikeOutboxProcessor`、`CommunityLikeOutboxMapper`、事件实体或服务类。** 当前增量只应替换调度层并添加指标。

## 三、合并配置

将 `config/application-observability-snippet.yml` 合并到现有 YAML，保留原来 `target-batch-size`、`max-events-per-target`、`max-retries` 等值。将旧 `poll-delay-ms` 从新调度方案中移除，或至少确认不会误以为它仍控制等待时间。

注意：scheduler worker 数和 `communityLikeTaskScheduler` 的线程池大小由同一个 `workers` 值生成，应保持一致。不要一边设 8 个 worker，却另设一个只允许 4 个线程的调度池。

## 四、构建与测试

在后端项目根目录执行：

```powershell
mvn -Dtest=AdaptivePollDelayPolicyTest test
mvn clean test
```

然后启动应用，确认启动日志中出现类似：`Community like Outbox adaptive scheduler started: workers=4...`。

检查指标：

```powershell
(Invoke-WebRequest "http://localhost:8123/api/actuator/prometheus" -UseBasicParsing).Content | Select-String "community_like_outbox_poll"
```

如果你已有更大范围的测试，应先运行项目原有测试，再执行 10 RPS 的 smoke test。

## 五、启动监控，再跑压测

按 `01_GRAFANA_SETUP.md` 启动 Prometheus + Grafana；确保 `/targets` 中 `picture-backend` 为 UP。按 `02_AB_EXPERIMENTS.md` 跑单热点、多目标、混合热点。

## 六、回滚

如果调度改造后存在异常，建议按顺序回滚：

1. 恢复原 `CommunityLikeOutboxScheduler.java` 和 `CommunityLikeSchedulingConfig.java`。
2. 若应用不再引用，再删除新增 `AdaptivePollDelayPolicy.java`、`CommunityLikeOutboxPollMetrics.java` 和测试。
3. 删除新增 `community.like.outbox.*-poll-delay-ms` 与 `workers` 配置；保留 Actuator 原配置（如你仍需要 Grafana）。
4. Grafana/Prometheus 可独立关闭：在 `config` 目录执行 `docker compose down`，不会改变后端业务数据。

不要回滚或清理 Outbox 事件表来处理监控配置问题。
