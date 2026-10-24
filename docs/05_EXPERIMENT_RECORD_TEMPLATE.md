# 对照实验记录模板

每个实验单独复制本表到 Markdown 或 Excel；必须记录实际结果，不要只记录目标 RATE。

| 字段 | 记录 |
|---|---|
| 日期 / 开始时间 |  |
| 代码分支 / commit SHA |  |
| 场景 | A single / B multi / C mixed |
| 目标类型 | POST / PICTURE |
| 目标数量 |  |
| RATE / DURATION |  |
| HOT_RATIO（C） |  |
| PRE_VUS / MAX_VUS |  |
| 压测机 CPU / 内存峰值 |  |
| 应用机 CPU / JVM / GC |  |
| DB CPU / 锁等待 / 慢 SQL |  |
| k6 http_reqs.rate |  |
| k6 iterations.rate |  |
| dropped_iterations |  |
| HTTP 失败率 / 业务失败率 |  |
| http_req_duration P95 / P99 |  |
| Outbox pending（测试前 / 峰值 / 结束后） |  |
| oldest pending age（峰值） |  |
| Outbox processed events / s |  |
| dead-letter 数变化 |  |
| 结论 / 下一步 |  |

## 结果比较原则

- A/B/C 的 RATE、DURATION、VUs、应用与 DB 配置要一致。
- 如果 dropped_iterations 不为 0，把本轮结果标记为“目标到达率未完全实现”，不应直接和目标到达率全部实现的轮次比较吞吐。
- 如果一次测试后 Outbox 还在积压，要先等它恢复或者记录恢复时间；否则下一组实验会受到上一组未完成事件影响。
- 要判断性能改善，至少重复两次；本机资源噪声较大时可以重复三次，报告中位数并保留每次原始 summary JSON。
