package com.zmt.picturebackendpro.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * V3 点赞 Redis/Stream 参数。
 * 所有分片、prefix 和消费组必须在迁移脚本及应用间保持一致。
 */
@ConfigurationProperties(prefix = "community.like.redis")
@Data
public class LikeRedisProperties {

    // 点赞使用独立 Redis 连接，避免为了测试 6380 而改变 Spring Session 的全局 spring.redis 连接。
    private String host = "127.0.0.1";
    private int port = 6380;
    private int database = 0;
    private String password = "";
    private long commandTimeoutMs = 2000;
    private String keyPrefix = "picture:like:v3";
    private int shardCount = 16;
    private String consumerGroup = "community-like-persist-v3";
    private int batchSize = 100;
    private long readBlockMs = 100;
    private long idleMinDelayMs = 10;
    private long idleMaxDelayMs = 250;
    private long errorMinDelayMs = 100;
    private long errorMaxDelayMs = 2000;
    private long idempotencyTtlSeconds = 86400;
    private int repeatedErrorWarningThreshold = 5;

}
