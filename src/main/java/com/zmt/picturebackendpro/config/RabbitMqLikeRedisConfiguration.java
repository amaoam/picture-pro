package com.zmt.picturebackendpro.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * RabbitMQ 版点赞模块的 Redis 专用连接配置。
 * <p>
 * 设计目的：
 * 1. 点赞模块使用独立的 Redis 连接（独立 Bean 名 {@code likeStringRedisTemplate}），
 *    与 Spring Session 等全局组件使用的 {@code StringRedisTemplate} 隔离，
 *    避免点赞高并发流量影响会话、缓存等关键链路；
 * 2. 通过 {@link ConditionalOnProperty} 限定只在 {@code community.like.mode=rabbitmq} 时装配，
 *    与旧 Redis Stream 版本互斥，避免 Bean 冲突；
 * 3. 连接管理器 {@link LikeRedisConnectionManager} 由 {@link LikeRedisProperties} 驱动，
 *    可在配置中独立指定 host/port/密码/连接池参数，实现点赞 Redis 与业务 Redis 的物理或逻辑分离。
 * <p>
 * 注意：本类只负责"点赞专用 Redis"的装配，不改变 Spring Session 的全局 Redis 连接配置。
 */
@Configuration
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqLikeRedisConfiguration {

    /**
     * 创建点赞模块专用的 Redis 连接管理器。
     * <p>
     * 使用显式 Bean 名 {@code likeRedisConnectionManager}，便于在其他组件中通过
     * {@link Qualifier} 精确注入，避免与全局连接管理器混淆。
     * <p>
     * 连接参数（host、port、password、database、连接池等）全部来自
     * {@link LikeRedisProperties}，由配置文件 {@code community.like.redis.*} 提供，
     * 从而与全局 Redis 配置解耦。
     *
     * @param properties 点赞 Redis 配置属性
     * @return 点赞专用 Redis 连接管理器
     */
    @Bean(name = "likeRedisConnectionManager")
    public LikeRedisConnectionManager rabbitMqLikeRedisConnectionManager(LikeRedisProperties properties) {
        return new LikeRedisConnectionManager(properties);
    }

    /**
     * 创建点赞模块专用的 {@link StringRedisTemplate}。
     * <p>
     * 关键点：
     * 1. Bean 名固定为 {@code likeStringRedisTemplate}，业务代码通过
     *    {@code @Resource(name = "likeStringRedisTemplate")} 或
     *    {@code @Qualifier("likeStringRedisTemplate")} 注入，与全局模板隔离；
     * 2. 连接工厂来自 {@code likeRedisConnectionManager}，确保使用点赞专用连接池；
     * 3. 使用 {@link StringRedisTemplate}（而非通用 RedisTemplate）是因为点赞场景
     *    只操作字符串与数字（计数、集合成员、requestId 等），String 序列化更直观、
     *    Lua 脚本交互更简单，也避免了 JDK 序列化带来的兼容性问题。
     * <p>
     * 该模板主要用于：
     * - 点赞/取消点赞的 Lua 原子命令；
     * - 查询点赞状态；
     * - 登记对账目标（SADD 到分片 Set）。
     *
     * @param manager 点赞专用 Redis 连接管理器
     * @return 点赞专用 StringRedisTemplate
     */
    @Bean(name = "likeStringRedisTemplate")
    public StringRedisTemplate rabbitMqLikeStringRedisTemplate(
            @Qualifier("likeRedisConnectionManager") LikeRedisConnectionManager manager) {
        return new StringRedisTemplate(manager.getConnectionFactory());
    }
}