package com.zmt.picturebackendpro.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * Fail-fast guard：Redis 当前态必须先从 MySQL 关系表完成 warm-up。
 * 否则应用可能把既有点赞误读为未点赞，并从错误基线开始递增计数。
 */
@Component
@ConditionalOnProperty(name = "community.like.mode", havingValue = "redis-stream")
public class RedisLikeStartupGuard implements InitializingBean {

    @Resource(name = "likeStringRedisTemplate") private StringRedisTemplate redisTemplate;
    @Resource private LikeRedisProperties properties;

    @Override
    public void afterPropertiesSet() {
        String readyKey = properties.getKeyPrefix() + ":meta:ready";
        Boolean ready = redisTemplate.hasKey(readyKey);
        if (!Boolean.TRUE.equals(ready)) {
            throw new IllegalStateException("Redis V3 点赞数据尚未初始化。请先暂停点赞写流量、导出关系表并运行 bootstrap_redis_like_state.py；缺少 key: " + readyKey);
        }
    }
}
