package com.zmt.picturebackendpro.service.redis;

import lombok.Data;
import lombok.Getter;

/**
 * Stream 中的状态变化事件。
 * 所有 ID 先以字符串形式从 Redis 读取，避免 JSON/JavaScript 把 BIGINT ID 精度截断。
 */
@Data
public class RedisLikeEvent {
    private String eventId;
    private String requestId;
    private String targetType;
    private String targetId;
    private String userId;
    private String ownerUserId;
    private String desiredState;
    private String delta;
    private String pairVersion;
    private String createdAtMs;

    public long targetIdAsLong() { return Long.parseLong(targetId); }
    public long userIdAsLong() { return Long.parseLong(userId); }
    public Long ownerUserIdAsLongOrNull() {
        return ownerUserId == null || ownerUserId.trim().isEmpty() ? null : Long.valueOf(ownerUserId);
    }
    public int deltaAsInt() { return Integer.parseInt(delta); }
    public long pairVersionAsLong() { return Long.parseLong(pairVersion); }
    public long createdAtAsLong() { return Long.parseLong(createdAtMs); }
}
