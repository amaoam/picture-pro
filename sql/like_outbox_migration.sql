-- ============================================================
-- Picture Pro Like V2 migration
-- Default schema: yu_picture (请先核对 application-local.yml 的实际数据库名)
-- 建议先在开发库/备份副本执行，并确认数据库版本为 MySQL 8.0+。
-- 本迁移中的 ALTER TABLE 不是可重复执行的：执行前检查 picture.likeCount 是否已存在。
-- ============================================================

USE yu_picture;

-- 1. 公共图片点赞冗余计数。帖子表 community_post 已有 likeCount，不需要重复增加。
ALTER TABLE picture
    ADD COLUMN likeCount BIGINT NOT NULL DEFAULT 0 COMMENT '公共图库图片点赞总数';

-- 2. Outbox 事务事件表。点赞关系与事件在同一数据库事务中写入。
CREATE TABLE IF NOT EXISTS like_event_outbox
(
    id            BIGINT       NOT NULL COMMENT 'MyBatis-Plus ASSIGN_ID',
    targetType    VARCHAR(16)  NOT NULL COMMENT '目标类型：POST / PICTURE',
    targetId      BIGINT       NOT NULL COMMENT '帖子 ID 或图片 ID',
    actorUserId   BIGINT       NOT NULL COMMENT '触发点赞变更的用户 ID',
    ownerUserId   BIGINT       NULL COMMENT '作者/所有者 ID 快照，未知时为空',
    delta         TINYINT      NOT NULL COMMENT '+1 点赞关系新增，-1 点赞关系删除',
    status        TINYINT      NOT NULL DEFAULT 0 COMMENT '0待处理 1已完成 2死信/需人工处理',
    retryCount    INT          NOT NULL DEFAULT 0 COMMENT '失败重试次数',
    nextRetryTime DATETIME     NULL COMMENT '下次可重试时间',
    lastError     VARCHAR(500) NULL COMMENT '最近一次处理失败原因',
    createTime    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '事件创建时间',
    processedTime DATETIME     NULL COMMENT '事件处理完成时间',
    PRIMARY KEY (id),
    KEY idx_status_id (status, id),
    KEY idx_status_retry_id (status, nextRetryTime, id),
    KEY idx_target (targetType, targetId, id),
    KEY idx_owner_id (ownerUserId, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='点赞计数事务 Outbox';

-- 3. 一次性回填：图片点赞数。数据量很大时请按图片 ID 范围分批执行，避免长事务和大锁。
-- UPDATE picture p
-- LEFT JOIN (
--     SELECT pictureId, COUNT(*) AS cnt
--     FROM picture_like
--     GROUP BY pictureId
-- ) x ON x.pictureId = p.id
-- SET p.likeCount = COALESCE(x.cnt, 0);

-- 4. 建议同时校准帖子点赞计数，尤其是旧版本经历过并发点赞后。
-- UPDATE community_post p
-- LEFT JOIN (
--     SELECT postId, COUNT(*) AS cnt
--     FROM community_post_like
--     GROUP BY postId
-- ) x ON x.postId = p.id
-- SET p.likeCount = COALESCE(x.cnt, 0);

-- 5. 可选：校准用户当前收到的有效点赞数（帖子 + 公共图片）。这不会重算或撤销历史经验值。
-- UPDATE user_community_profile profile
-- LEFT JOIN (
--     SELECT ownerUserId, SUM(cnt) AS cnt
--     FROM (
--         SELECT p.userId AS ownerUserId, COUNT(*) AS cnt
--         FROM community_post_like l
--         JOIN community_post p ON p.id = l.postId
--         WHERE p.userId IS NOT NULL
--         GROUP BY p.userId
--         UNION ALL
--         SELECT pic.userId AS ownerUserId, COUNT(*) AS cnt
--         FROM picture_like l
--         JOIN picture pic ON pic.id = l.pictureId
--         WHERE pic.userId IS NOT NULL
--         GROUP BY pic.userId
--     ) all_likes
--     GROUP BY ownerUserId
-- ) totals ON totals.ownerUserId = profile.userId
-- SET profile.receivedLikeCount = COALESCE(totals.cnt, 0);

-- 6. 完成上线后应监控 status=2 的死信事件；不可直接删除。修复后可将对应事件重置为 status=0 再处理。
-- SELECT status, COUNT(*) FROM like_event_outbox GROUP BY status;
