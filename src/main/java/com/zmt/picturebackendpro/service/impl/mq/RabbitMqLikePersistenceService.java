package com.zmt.picturebackendpro.service.impl.mq;

import com.zmt.picturebackendpro.service.redis.RedisLikeEvent;
import com.zmt.picturebackendpro.service.redis.RedisLikePersistenceService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * RabbitMQ 模式复用项目已有的批量 MySQL 持久化算法（事件 Ledger、关系变更、计数聚合、经验副作用）。
 * 覆盖方法使 Spring 事务代理明确包住 super 调用；Redis Stream 旧消费者在 rabbitmq 模式下不会启动。
 */
@Service
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqLikePersistenceService extends RedisLikePersistenceService {
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void persistBatch(List<RedisLikeEvent> events) {
        super.persistBatch(events);
    }
}
