-- V3.0 MySQL 消费幂等表。
-- 必须与关系变更、计数更新和经验流水在同一个 MySQL 事务内提交。
-- event_id 是 Redis 事件的 UUID；同一个 Stream 消息重投时，唯一键阻止重复应用。
CREATE TABLE IF NOT EXISTS community_like_event_consume (
    event_id       VARCHAR(64)  NOT NULL COMMENT 'Redis Lua 生成的唯一事件 ID',
    target_type    VARCHAR(16)  NOT NULL COMMENT 'POST / PICTURE',
    target_id      BIGINT       NOT NULL,
    actor_user_id  BIGINT       NOT NULL,
    owner_user_id  BIGINT       NULL,
    desired_state  VARCHAR(8)   NOT NULL COMMENT 'LIKE / UNLIKE',
    delta          TINYINT      NOT NULL COMMENT '+1 / -1',
    pair_version   BIGINT       NOT NULL COMMENT '同一用户-目标状态变化版本',
    created_at     DATETIME(3)  NOT NULL,
    processed_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (event_id),
    KEY idx_like_consume_target_time (target_type, target_id, processed_at),
    KEY idx_like_consume_processed_at (processed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Redis Stream 点赞事件的 MySQL 幂等消费记录';

-- 执行前需确认真实项目的 like_count 字段已经存在且是可更新的整数列。
-- 本迁移不自动添加社区帖子/图片关系表索引，避免与现有索引重复。
