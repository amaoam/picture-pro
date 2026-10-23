-- ============================================================
-- 一次性计数回填脚本（维护窗口使用）
-- 必须先停止旧版点赞服务，并且在新版 Outbox 消费者启动前执行。
-- 若已有 status=0 的 outbox 事件，先处理完再回填；否则全量计数与待处理 delta 可能重复计算。
-- 这些 UPDATE 会扫描大量关系数据。大表应按主键范围/业务分区分批执行，而不是一次性运行。
-- ============================================================

USE yu_picture;

-- 公共图片的当前点赞关系数
UPDATE picture p
LEFT JOIN (
    SELECT pictureId, COUNT(*) AS cnt
    FROM picture_like
    GROUP BY pictureId
) x ON x.pictureId = p.id
SET p.likeCount = COALESCE(x.cnt, 0);

-- 社区帖子的当前点赞关系数
UPDATE community_post p
LEFT JOIN (
    SELECT postId, COUNT(*) AS cnt
    FROM community_post_like
    GROUP BY postId
) x ON x.postId = p.id
SET p.likeCount = COALESCE(x.cnt, 0);

-- 用户当前收到的有效点赞数（帖子 + 公共图片）。这只修复统计值，不会重算经验。
UPDATE user_community_profile profile
LEFT JOIN (
    SELECT ownerUserId, SUM(cnt) AS cnt
    FROM (
        SELECT p.userId AS ownerUserId, COUNT(*) AS cnt
        FROM community_post_like l
        JOIN community_post p ON p.id = l.postId
        WHERE p.userId IS NOT NULL
        GROUP BY p.userId
        UNION ALL
        SELECT pic.userId AS ownerUserId, COUNT(*) AS cnt
        FROM picture_like l
        JOIN picture pic ON pic.id = l.pictureId
        WHERE pic.userId IS NOT NULL
        GROUP BY pic.userId
    ) all_likes
    GROUP BY ownerUserId
) totals ON totals.ownerUserId = profile.userId
SET profile.receivedLikeCount = COALESCE(totals.cnt, 0);
