package com.zmt.picturebackendpro.config;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.util.StringUtils;

import java.time.Duration;

/**
 * 独立管理点赞模块的 Lettuce 连接工厂。
 * 特意不把 LettuceConnectionFactory 本身注册成 Spring Bean，避免 Spring Boot 因为检测到自定义
 * RedisConnectionFactory 而关闭全局 Redis 自动配置，进而意外改变 Spring Session 使用的连接。
 */
public class LikeRedisConnectionManager implements InitializingBean, DisposableBean {

    private final LettuceConnectionFactory delegate;

    public LikeRedisConnectionManager(LikeRedisProperties properties) {
        if (!StringUtils.hasText(properties.getHost())) {
            throw new IllegalStateException("community.like.redis.host 不能为空");
        }
        if (properties.getPort() < 1 || properties.getPort() > 65535) {
            throw new IllegalStateException("community.like.redis.port 必须在 1-65535 之间");
        }
        if (properties.getDatabase() < 0 || properties.getCommandTimeoutMs() < 1) {
            throw new IllegalStateException("community.like.redis.database/command-timeout-ms 配置不合法");
        }

        RedisStandaloneConfiguration server = new RedisStandaloneConfiguration(
                properties.getHost(), properties.getPort());
        server.setDatabase(properties.getDatabase());
        if (StringUtils.hasText(properties.getPassword())) {
            server.setPassword(RedisPassword.of(properties.getPassword()));
        }

        LettuceClientConfiguration client = LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(properties.getCommandTimeoutMs()))
                .shutdownTimeout(Duration.ofMillis(100))
                .build();
        this.delegate = new LettuceConnectionFactory(server, client);
    }

    @Override
    public void afterPropertiesSet() {
        // 因为 delegate 不是单独注册的 Spring Bean，必须手动驱动其生命周期。
        delegate.afterPropertiesSet();
    }

    /** 提供给点赞专用 StringRedisTemplate，不向 Spring 的全局 Redis 自动配置暴露此工厂 Bean。 */
    public LettuceConnectionFactory getConnectionFactory() {
        return delegate;
    }

    @Override
    public void destroy() {
        delegate.destroy();
    }
}
