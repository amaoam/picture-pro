# 教程 2：单热点、多目标、混合热点的 k6 对照实验

## 目标与实验规则

这三组实验只改变“请求打到哪些帖子/图片”，其它环境尽量保持一致。这样才能观察：

- 一个热点帖子是否成为计数更新、锁竞争或按目标顺序消费的限制因素；
- 把流量分散到多个目标后，整体吞吐是否提升；
- 混合热点能否代表“少数热门内容 + 长尾内容”的实际业务。

**单热点不是错误测试。** 它模拟一个极端热门的帖子，是对系统热点承受能力的压力测试。多目标则用于判断瓶颈是否只集中在热点行。

每次实验必须使用同一套代码、数据库数据、JVM 参数、连接池配置和 k6 机器。一次只改一个主要变量；先等 Outbox 待处理数恢复到接近 0，且死信为 0，再跑下一个场景。不要同时启动两个 k6 进程。

## 一、准备实验目标

先复制样例：`Copy-Item .\k6\targets.example.json .\k6\targets.local.json`。然后编辑新文件 `k6/targets.local.json`，内容类似：

```json
{
  "hotTargetId": "2112391798298521601",
  "targetIds": [
    "2112391798298521601",
    "2112391798298521602",
    "2112391798298521603",
    "2112391798298521604",
    "2112391798298521605"
  ]
}
```

上面除第一个 ID 外的描述性值只是占位符，**不能直接运行**；请全部替换成数据库中真实、存在且符合点赞接口权限要求的帖子 ID。所有目标必须是同一种类型（本例都是 POST），并且最好都是公共、可正常点赞的内容。不要直接对生产数据执行压测。

如果准备图片测试，设置 `$env:TARGET_TYPE = "PICTURE"`，并确保 IDs 都是可点赞的公共图片 ID。

## 二、准备压测账号

新脚本使用每个 VU 独占一个登录账号，避免多个 VU 共享同一个用户状态。`MAX_VUS=400` 时，账号文件至少需要 400 个有效账号。

如果已经有 `k6/users.local.json`，先确认账号数足够。没有的话，在项目根目录运行本包提供的脚本：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\New-LoadTestUsers.ps1 -Count 400
```

脚本会调用本地注册接口并生成 `k6/users.local.json`。它只能用于你自己的开发/测试环境；确认注册接口地址与请求字段和当前后端一致。若你已有 400 个有效账号，不要重复创建。账号创建应在压测前完成，不要把注册时间算入 API 测试。

## 三、设置公共环境变量

在项目根目录的 PowerShell 执行：

```powershell
$env:BASE_URL = "http://localhost:8123/api"
$env:TARGET_TYPE = "POST"
$env:USERS_FILE = ".\users.local.json"
$env:TARGETS_FILE = ".\targets.local.json"
$env:PRE_VUS = "200"
$env:MAX_VUS = "400"
$env:DURATION = "120s"
$env:SETUP_TIMEOUT = "5m"
```

`RATE`、`TARGET_MODE` 会按每次实验覆盖。这里使用 `200/400` 只是较稳妥的试跑起点，不能保证一定达到 1000 RPS；VU 需求会随接口耗时和 k6 机器资源变化。

## 四、实验 A：单热点目标

保持原先的目标 `2112391798298521601`，所有请求只触达这个目标：

```powershell
$env:TARGET_MODE = "single"
$env:RATE = "1000"
k6 run .\k6\community_like_ab.js --summary-export .\results\A-single-1000rps.json
```

建议依次执行：`RATE=100`、`300`、`500`、`800`、`1000`，每次运行单独设置结果文件名。初始熟悉阶段可先用 `DURATION=30s`；正式对比建议 120 秒。

## 五、实验 B：多目标分散

确认 `targetIds` 至少有 2 个有效 ID。每个 VU 固定分配到一个目标，尽量让各组操作逻辑稳定：

```powershell
$env:TARGET_MODE = "multi"
$env:RATE = "1000"
k6 run .\k6\community_like_ab.js --summary-export .\results\B-multi-1000rps.json
```

再用同样的速率阶梯跑一遍。若目标有 20 篇，VU 会轮流分配到这些目标，而不是所有请求都写同一篇帖子。

## 六、实验 C：混合热点

保持相同目标列表，默认 80% 请求随机打向 `hotTargetId`，其余 20% 均匀随机打向其它目标：

```powershell
$env:TARGET_MODE = "mixed"
$env:HOT_RATIO = "0.8"
$env:RATE = "1000"
k6 run .\k6\community_like_ab.js --summary-export .\results\C-mixed80-1000rps.json
```

需要进一步对比时，可以将 `HOT_RATIO` 改成 `0.5`、`0.9`。注意：脚本在每一次请求中抽样，理论上接近 80/20，但短时采样不一定正好等于 80/20。

## 七、每次实验记录这些参数

最少记录：代码版本/提交 SHA、日期、场景（A/B/C）、RATE、DURATION、PRE_VUS、MAX_VUS、目标数量、HOT_RATIO、应用/数据库运行位置、接口 P95/P99、实际 `http_reqs.rate`、`iterations.rate`、`dropped_iterations`、HTTP 错误率、业务失败率、测试前/中/后 Outbox pending、最老事件年龄、消费事件速率、数据库连接池等待、数据库 CPU/锁等待。

不要只记 1000 RPS 这个目标值。k6 目标 rate 不等于成功完成请求的实际速率。

## 八、怎么判断结果

### 情况 1：单热点慢，多目标快很多

优先怀疑热点目标的行锁、目标级顺序消费或该目标对应的更新路径。接下来对比 `single` 下的 pending/oldest age 与消费速率；再检查 MySQL 锁等待。不能直接得出“加更多 worker 就能解决”，因为同一目标按顺序处理正是为了防止统计错误。

### 情况 2：单热点和多目标都慢，且连接池 pending 上升

优先查连接池等待、事务耗时、MySQL 慢 SQL。记录 active/max 和数据库实际连接数。先确认每个事务没有包含非必要远程调用、日志输出或长时间计算，再考虑是否调整连接池。

### 情况 3：单热点和多目标都慢，但连接池不满

看应用 CPU、线程、GC、SQL 执行时间以及数据库 CPU。检查 MyBatis SQL 日志有没有在性能测试期间逐条打印大量内容。也要检查 k6 自身 CPU/内存/网络是否饱和。

### 情况 4：API 稳定，但 Outbox 事件堆积增加

说明同步请求路径看起来还能接收流量，但统计消费者赶不上。检查消费速率、批处理 P95、空轮询比例、死信以及单目标顺序阻塞情况。

### 情况 5：k6 `dropped_iterations` 大于 0

先不要宣称后端吞吐达到目标。k6 固定到达率需要有足够空闲 VU；官方文档说明，不足时会出现 dropped iterations，但延迟恶化造成 VU 不够也会引发相同现象。可先提高预分配 VU/最大 VU并检查压测机资源；如果应用延迟已经持续升高，则不能只靠无限加 VU 掩盖问题。

## 九、目标流量阶梯与停止条件

推荐流程：

1. 10 RPS × 30 秒：验证账号、Cookie、路由、目标 ID。
2. 50 → 100 → 300 RPS：确认趋势和指标采集正常。
3. 500 → 800 RPS：观察延迟和 Outbox 是否持续积压。
4. 1000 RPS × 120 秒：只有前面阶段通过后再做。
5. 每次结束后继续看 Grafana 1–3 分钟，确认 Outbox backlog 回落；如果 pending 一直涨、最老事件持续增加、死信增长或错误率异常，停止继续加压，先定位原因。

保持阈值：`P95 < 500ms`、`P99 < 1000ms`、业务失败率 `< 1%`、`dropped_iterations = 0`，作为本项目当前的建议门槛。第一次未达标不意味着脚本坏了，它可能确实揭示当前版本到达了性能边界。

## 十、k6 常见错误

- `测试账号数小于 MAX_VUS`：增加测试账号，或先下调 `MAX_VUS`。账号数量与 VU 数须匹配。
- `登录响应未找到 SESSION Cookie`：确认实际 Cookie 名称，按浏览器开发者工具中登录响应修改 `SESSION_COOKIE_NAME`。
- business code 不为 0：验证用户有权访问目标、目标 ID 存在、接口路径与当前代码一致。
- `targets.local.json` 报 ID 无效：目标列表中仍有占位符，替换成真实数字 ID。
- `dropped_iterations`：逐步提高 `PRE_VUS` 和 `MAX_VUS` 并查看 k6 主机资源；不要直接把它当成单纯的服务器错误。

## 官方参考

- k6 constant-arrival-rate：<https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/constant-arrival-rate/>
- k6 arrival-rate VU allocation：<https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/arrival-rate-vu-allocation/>
