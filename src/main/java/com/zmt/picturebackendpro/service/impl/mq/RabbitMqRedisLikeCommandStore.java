package com.zmt.picturebackendpro.service.impl.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zmt.picturebackendpro.config.LikeRedisProperties;
import com.zmt.picturebackendpro.service.redis.LikeRedisKeyFactory;
import com.zmt.picturebackendpro.service.redis.RedisLikeCommandStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/** RabbitMQ 模式复用现有 Lua 命令门面；其 Stream 只作为 Redis 侧的可靠待投递日志。 */
@Component
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqRedisLikeCommandStore extends RedisLikeCommandStore {
    public RabbitMqRedisLikeCommandStore(@Qualifier("likeStringRedisTemplate") StringRedisTemplate redisTemplate,
                                         ObjectMapper objectMapper,
                                         LikeRedisKeyFactory keyFactory,
                                         LikeRedisProperties properties,
                                         MeterRegistry meterRegistry) {
        super(redisTemplate, objectMapper, keyFactory, properties, meterRegistry);
    }
}
