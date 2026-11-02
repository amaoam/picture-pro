1.运行本地redis 端口6379 
2.mysql开机自启 不用管
3.docker 中的prometheus和grafana
在 `config` 目录：

```powershell
docker compose config
docker compose up -d
docker compose ps
```
prometheus
浏览器打开：`http://localhost:9090/targets`

找到 `picture-backend`，检查 State 是否为 `UP`。
grafana
浏览器打开：`http://localhost:3000`。

使用 `.env` 中的用户名密码登录，默认用户名是 `admin`（可以在 `.env` 改）。本包已自动配置 Prometheus 数据源，并预置 `Community Like - Outbox & API Overview` 仪表盘。

进入 **Dashboards → Community Like → Community Like - Outbox & API Overview**。
关闭与重启
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

4.docker中redis 端口6380 启动
在deploy目录
docker compose -f .\docker-compose.redis.yml up -d
docker compose -f .\docker-compose.redis.yml ps
docker exec picture-like-v3-redis redis-cli PING


