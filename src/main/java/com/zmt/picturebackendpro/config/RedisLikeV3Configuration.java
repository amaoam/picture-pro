package com.zmt.picturebackendpro.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * V3 点赞模块使用独立 Redis 连接。
 * 原有 spring.redis.* 自动配置仍然保留，继续供 Spring Session 与其它既有业务使用。
 */
@Configuration
@EnableConfigurationProperties(LikeRedisProperties.class)
public class RedisLikeV3Configuration {

    @Bean(name = "likeRedisConnectionManager")
    @ConditionalOnProperty(name = "community.like.mode", havingValue = "redis-stream")
    public LikeRedisConnectionManager likeRedisConnectionManager(LikeRedisProperties properties) {
        return new LikeRedisConnectionManager(properties);
    }

    @Bean(name = "likeStringRedisTemplate")
    @ConditionalOnProperty(name = "community.like.mode", havingValue = "redis-stream")
    public StringRedisTemplate likeStringRedisTemplate(
            @Qualifier("likeRedisConnectionManager") LikeRedisConnectionManager manager) {
        // 显式绑定点赞专用连接；Stream field 与 Lua key/value 均采用 StringRedisSerializer。
        return new StringRedisTemplate(manager.getConnectionFactory());
    }
}
