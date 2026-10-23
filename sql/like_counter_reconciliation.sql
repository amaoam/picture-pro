-- 计数校准脚本：先在只读副本/低峰演练。不要在不了解数据量时直接全表执行。
-- 图片计数差异查询
SELECT p.id, p.likeCount AS cachedLikeCount, COALESCE(x.cnt, 0) AS realLikeCount
FROM picture p
LEFT JOIN (
    SELECT pictureId, COUNT(*) AS cnt
    FROM picture_like
    GROUP BY pictureId
) x ON x.pictureId = p.id
WHERE COALESCE(p.likeCount, 0) <> COALESCE(x.cnt, 0)
LIMIT 1000;

-- 帖子计数差异查询
SELECT p.id, p.likeCount AS cachedLikeCount, COALESCE(x.cnt, 0) AS realLikeCount
FROM community_post p
LEFT JOIN (
    SELECT postId, COUNT(*) AS cnt
    FROM community_post_like
    GROUP BY postId
) x ON x.postId = p.id
WHERE COALESCE(p.likeCount, 0) <> COALESCE(x.cnt, 0)
LIMIT 1000;

-- 小数据量或维护窗口中可用以下语句回填。大数据量请按主键范围分批更新。
-- UPDATE picture p
-- LEFT JOIN (SELECT pictureId, COUNT(*) AS cnt FROM picture_like GROUP BY pictureId) x
--   ON x.pictureId = p.id
-- SET p.likeCount = COALESCE(x.cnt, 0);
--
-- UPDATE community_post p
-- LEFT JOIN (SELECT postId, COUNT(*) AS cnt FROM community_post_like GROUP BY postId) x
--   ON x.postId = p.id
-- SET p.likeCount = COALESCE(x.cnt, 0);
