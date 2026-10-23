# picture-pro 点赞高并发 V2
## 1. 实现目标

- 点赞/取消点赞关系仍由 MySQL 同步持久化，保留原有 `(postId,userId)`、`(pictureId,userId)` 唯一索引。
- 点赞关系真正新增/删除时，在同一个事务中写入 `like_event_outbox`。重复点赞/取消不会产生新事件。
- 后台 4 个轮询 worker 通过 `SELECT ... FOR UPDATE SKIP LOCKED` 并发领取事件。
- 每批按目标 ID 聚合增减量，一条 SQL 批量更新多个帖子/图片的 `likeCount`，避免每次点赞都更新同一条热点帖子/图片。
- 获赞统计按作者批量合并；经验奖励通过 `user_exp_record.eventKey` 永久幂等，点赞→取消→再点赞不重复获得同一目标的获赞经验。
- 新增 `picture.likeCount` 字段，图片状态接口不再用 `COUNT(*)` 计算点赞总数。
- 处理失败的事件退避重试；超过最大重试次数进入 `status=2`，需要告警、诊断和人工/运维修复，不能静默忽略。

## 2. 文件清单

### 新增源文件

-  `src/main/java/com/zmt/picturebackendpro/model/entity/CommunityLikeOutboxEvent.java`
- `src/main/java/com/zmt/picturebackendpro/mapper/CommunityLikeOutboxMapper.java`
- `src/main/java/com/zmt/picturebackendpro/service/CommunityLikeService.java`
- `src/main/java/com/zmt/picturebackendpro/service/impl/CommunityLikeServiceImpl.java`
- `src/main/java/com/zmt/picturebackendpro/service/impl/CommunityLikeOutboxProcessor.java`
- `src/main/java/com/zmt/picturebackendpro/service/impl/CommunityLikeOutboxFailureService.java`
- `src/main/java/com/zmt/picturebackendpro/job/CommunityLikeOutboxScheduler.java`
- `src/main/java/com/zmt/picturebackendpro/config/CommunityLikeSchedulingConfig.java`
- `src/main/java/com/zmt/picturebackendpro/exception/CommunityLikeBatchProcessingException.java`

### 完整替换文件

- `src/main/java/com/zmt/picturebackendpro/controller/CommunityInteractionController.java`：点赞 API 转交新 `CommunityLikeService`，评论 API 仍调用原 `CommunityInteractionService`；评论分页保留完整的 `Page<CommunityCommentVO>` 类型。
- `src/main/java/com/zmt/picturebackendpro/model/entity/Picture.java`：完整实体，新增 `likeCount`。
- `src/main/java/com/zmt/picturebackendpro/mapper/UserExpRecordMapper.java`：保留每日经验查询，增加批量幂等键查询。
- `src/main/java/com/zmt/picturebackendpro/service/CommunityExperienceService.java`：完整接口，新增 `processLikeEvents`。
- `src/main/java/com/zmt/picturebackendpro/service/impl/CommunityExperienceServiceImpl.java`：完整实现，批量维护作者获赞数/经验，并缓存等级阈值。

> 原 `CommunityInteractionServiceImpl` 仍包含旧的点赞实现。本包把 HTTP 点赞路由切到新独立的 `CommunityLikeService`，评论仍走现有互动服务，以尽量隔离评论改造。将代码合入本地工程时，请搜索是否有其他地方直接调用旧的 `CommunityInteractionService.likePost/unlikePost/likePublicPicture/unlikePublicPicture`；若有，应一并改为注入新 `CommunityLikeService`，或手动将旧方法改为委托方法，避免保留两套可能被误用的点赞写入口。

## 3. 数据库迁移

1. 备份数据库，先在开发库运行并验收。
2. 核对真实数据库名。`sql/like_outbox_migration.sql` 默认 `USE yu_picture;`，如果你的连接库不同需先修改。
3. 检查 `picture.likeCount` 是否已经存在；若不存在，执行迁移 SQL 中 `ALTER TABLE picture ...`。该 ALTER 不可重复执行。
4. 创建 `like_event_outbox`。
5. 在维护窗口回填 `picture.likeCount`、`community_post.likeCount`；如需要一并修复历史获赞档案，再执行 `sql/like_counter_backfill.sql` 中的 profile 更新语句。大表请按主键范围分批执行，避免长时间全表扫描/更新。不要在仍持续消费 Outbox 或有待处理事件时重置计数。
6. 历史 `receivedLikeCount` 校准只影响当前有效获赞数量，不会重算经验历史。
7. `sql/like_counter_reconciliation.sql` 提供差异查询与人工回填示例。

## 4. 配置

将 `application-properties-snippet.yml` 合并到现有 `application.yml` 的 `community:` 节点，不要覆盖现有 `community.experience` 配置：

- `poll-delay-ms` 默认 100
- `batch-size` 默认 200
- `max-retries` 默认 10

调优建议从 4 个 worker、200 条/批、100ms 轮询开始。峰值 1000+ 次/秒是目标，不是静态配置即可保证的吞吐量；最终需要用真实数据库压测，并观察最老未处理事件年龄。

## 5. 数据库和运行要求

- `CommunityLikeOutboxMapper` 使用 `FOR UPDATE SKIP LOCKED`，要求 MySQL 8.0+。如果当前实际是 MySQL 5.7，请不要直接部署，需要改成带租约/过期时间的领取方案。
- 所有点赞 Outbox 处理依赖 Spring 的事务代理，`CommunityLikeOutboxProcessor.processNextBatch()` 必须由另一个 Bean（调度器）调用，不要改成同类内部自调用。
- 本包依赖项目原有的 `CommunityPostMapper`、`CommunityPostLikeMapper`、`PictureMapper`、`PictureLikeMapper`、`UserCommunityProfileMapper`、实体和 VO。
- `Picture` 字段映射依赖现有 MyBatis-Plus 配置，库中 `picture.likeCount` 必须实际存在后才能启动并执行相关 SQL。

## 6. 重要一致性语义

- 点赞成功响应中的 `likedByCurrentUser` 通过关系表读取，按请求事务完成后立即反映关系。
- `likeCount` 和作者的 `receivedLikeCount` 由异步消费者更新，允许约 0.5–2 秒延迟。
- Outbox 的计数更新、经验流水与 profile 更新、事件 status=1 标记处于同一个消费事务。发生异常时这些改动整体回滚。
- `status=2` 是死信而非成功；必须监控并修复。对死信事件人工修复前，先查明失败原因并检查是否已通过其他方式补偿，避免重复计数。
- `user_exp_record.eventKey` 继续保证同一帖子/图片与同一点赞人的获赞经验只奖励一次；取消点赞不扣除既有经验，符合原有业务语义。
- 物理删除被 Outbox 引用的帖子/图片会让批次失败并进入重试/死信；本项目应保持逻辑删除，或在物理清理前确保相关 Outbox 已处理。

## 7. 上线/验收清单

1. 同一用户并发请求点赞 50 次，点赞关系只有 1 条，只有 1 个 `delta=+1` 事件。
2. 同一用户重复取消点赞，只有首次删除真实关系时产生 `delta=-1`。
3. 点赞事务在关系 INSERT 后模拟 Outbox INSERT 失败，确认点赞关系也回滚。
4. 多个 worker 并发消费，确认单个事件不会被两个 worker 同时处理。
5. 对相同目标一批 +1/-1 混合事件，验证 `likeCount` 只按合计 delta 变化。
6. 消费事务在更新计数后故意抛异常，确认计数、经验流水与事件完成状态全部回滚。
7. 点赞、取消、再次点赞：`likeCount` 和 `receivedLikeCount` 恢复正确，但获赞经验不会重复发放。
8. 把消费者短暂停掉并持续压测，再恢复消费者，确认积压可被排空。
9. 模拟坏事件，观察 retryCount/nextRetryTime；达到重试上限后确认事件成为 status=2，并触发运维告警/人工处理。
10. 比较 `picture.likeCount` / `community_post.likeCount` 与关系表真实计数；对 1000+ 次/秒做持续压测，而不仅测短时峰值。


