package com.zmt.picturebackendpro.config;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/** 防止未导入 MySQL 点赞基线就误启 RabbitMQ 模式。 */
@Component
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqLikeStartupGuard implements InitializingBean {
    private final StringRedisTemplate redisTemplate;
    private final LikeRedisProperties redisProperties;

    public RabbitMqLikeStartupGuard(@Qualifier("likeStringRedisTemplate") StringRedisTemplate redisTemplate,
                                   LikeRedisProperties redisProperties) {
        this.redisTemplate = redisTemplate;
        this.redisProperties = redisProperties;
    }

    @Override
    public void afterPropertiesSet() {
        String key = redisProperties.getKeyPrefix() + ":meta:ready";
        String value = redisTemplate.opsForValue().get(key);
        if (!"1".equals(value)) {
            throw new IllegalStateException("RabbitMQ 点赞模式未完成 Redis 基线导入。请先运行 bootstrap_rabbitmq_like_state.py，确认数据后再启动；缺少标记 " + key);
        }
    }
}
