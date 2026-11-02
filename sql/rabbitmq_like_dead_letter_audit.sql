-- RabbitMQ 点赞死信审计表。
-- 执行一次即可；该表只保存已确认发布到 DLQ 的消息审计副本，不自动重放消息。
CREATE TABLE IF NOT EXISTS community_like_dead_letter_audit (
    message_id          VARCHAR(128) NOT NULL COMMENT 'RabbitMQ message_id；缺失时由消息体 SHA-256 生成',
    event_id            VARCHAR(128) NULL COMMENT '点赞事件 ID；坏 JSON 消息允许为空',
    request_id          VARCHAR(128) NULL COMMENT '原始点赞请求 ID',
    target_type         VARCHAR(16) NULL COMMENT 'POST / PICTURE',
    target_id           VARCHAR(64) NULL COMMENT '帖子或图片 ID，按字符串保存以避免 BIGINT 精度问题',
    user_id             VARCHAR(64) NULL COMMENT '点赞用户 ID',
    desired_state       VARCHAR(16) NULL COMMENT 'LIKE / UNLIKE',
    delta               VARCHAR(8) NULL COMMENT '+1 / -1',
    retry_count         INT NOT NULL DEFAULT 0 COMMENT '应用层已累计的失败次数',
    reason_code         VARCHAR(128) NOT NULL COMMENT 'MAX_RETRIES_EXCEEDED / BROKER_DEAD_LETTER 等',
    error_class         VARCHAR(255) NULL COMMENT '异常类型',
    error_message       TEXT NULL COMMENT '异常摘要，不存完整堆栈',
    source_queue        VARCHAR(255) NOT NULL COMMENT '原始点赞队列',
    dead_queue          VARCHAR(255) NOT NULL COMMENT '死信队列',
    dead_lettered_at_ms BIGINT NULL COMMENT '发布到死信队列时的 epoch milliseconds',
    payload_text        LONGTEXT NOT NULL COMMENT '原始消息 UTF-8 文本，便于人工阅读',
    payload_base64      LONGTEXT NOT NULL COMMENT '原始消息 Base64，保证坏消息也能无损留存',
    headers_json        LONGTEXT NULL COMMENT 'RabbitMQ headers，包含 x-death 等诊断元数据',
    delivery_count      INT NOT NULL DEFAULT 1 COMMENT '死信审计消费者收到该 message_id 的次数',
    first_seen_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    last_seen_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    review_status       VARCHAR(32) NOT NULL DEFAULT 'PENDING_REVIEW' COMMENT '人工处理状态；当前不自动重放',
    PRIMARY KEY (message_id),
    KEY idx_like_dead_event_id (event_id),
    KEY idx_like_dead_status_time (review_status, first_seen_at),
    KEY idx_like_dead_target (target_type, target_id),
    KEY idx_like_dead_time (dead_lettered_at_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='RabbitMQ community like dead-letter audit records';
