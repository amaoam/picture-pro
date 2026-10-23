package com.zmt.picturebackendpro.service.impl;

import com.zmt.picturebackendpro.mapper.CommunityLikeOutboxMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.List;

/** 必须使用独立事务记录失败；原批处理事务已经回滚。 */
@Service
public class CommunityLikeOutboxFailureService {

    @Value("${community.like.outbox.max-retries:10}")
    private int configuredMaxRetries;

    @Resource private CommunityLikeOutboxMapper outboxMapper;

    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void recordFailure(List<Long> eventIds, String errorMessage) {
        if (eventIds == null || eventIds.isEmpty()) {
            return;
        }
        int maxRetries = Math.max(1, configuredMaxRetries);
        String message = StringUtils.hasText(errorMessage) ? errorMessage : "unknown error";
        if (message.length() > 500) {
            message = message.substring(0, 500);
        }
        outboxMapper.recordBatchFailure(eventIds, message, maxRetries);
    }
}
