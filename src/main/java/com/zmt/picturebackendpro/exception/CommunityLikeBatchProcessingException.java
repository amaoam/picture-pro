package com.zmt.picturebackendpro.exception;

import java.util.Collections;
import java.util.List;

/** 批处理失败时把本批事件 ID 带出事务，供另一个 REQUIRES_NEW 事务记录重试信息。 */
public class CommunityLikeBatchProcessingException extends RuntimeException {
    private final List<Long> eventIds;

    public CommunityLikeBatchProcessingException(List<Long> eventIds, Throwable cause) {
        super("点赞 Outbox 批处理失败", cause);
        this.eventIds = eventIds == null ? Collections.emptyList() : List.copyOf(eventIds);
    }

    public List<Long> getEventIds() {
        return eventIds;
    }
}
