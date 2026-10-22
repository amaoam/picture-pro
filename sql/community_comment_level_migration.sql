-- 社区评论层级迁移（针对已经创建的 community_comment 表）
-- 先备份数据库并在开发库验证；本脚本只能对同一数据库执行一次。
-- 当前线上/已有业务规则禁止多层嵌套，因此旧数据 parentId 非空时视为一级评论的回复。

USE yu_picture;

ALTER TABLE community_comment
    ADD COLUMN level TINYINT NOT NULL DEFAULT 0 COMMENT '展示层级：0-一级评论 1-线程内回复' AFTER parentId,
    ADD COLUMN rootId BIGINT NULL COMMENT '所属一级评论 ID；一级评论为空' AFTER level;

-- 旧代码只允许回复一级评论，所以历史回复的 parentId 即为其 rootId。
UPDATE community_comment
SET level = CASE WHEN parentId IS NULL THEN 0 ELSE 1 END,
    rootId = CASE WHEN parentId IS NULL THEN NULL ELSE parentId END;

ALTER TABLE community_comment
    ADD KEY idx_post_level_comment_time (postId, isDelete, status, level, parentId, createTime, id),
    ADD KEY idx_root_reply_time (rootId, isDelete, status, level, createTime, id);
