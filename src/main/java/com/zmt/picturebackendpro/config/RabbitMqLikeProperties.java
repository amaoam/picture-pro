package com.zmt.picturebackendpro.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** RabbitMQ V1 点赞链路参数。 */
@Data
@ConfigurationProperties(prefix = "community.like.rabbitmq")
public class RabbitMqLikeProperties {
    private String exchange = "community.like.exchange.v1";
    private String queue = "community.like.commands.v1";
    private String routingKey = "community.like.command.v1";

    private String deadExchange = "community.like.dead.exchange.v1";
    private String deadQueue = "community.like.commands.dead.v1";
    private String deadRoutingKey = "community.like.dead.v1";

    private String relayGroup = "community-like-rabbitmq-relay-v1";
    /** 第一版单实例固定 consumer 名称，服务重启后可重新读取其 Pending 消息。 */
    private String relayConsumer = "community-like-rabbitmq-relay-0";
    private int relayBatchSize = 100;
    private long relayPollDelayMs = 10;
    private long publisherConfirmTimeoutMs = 5000;

    private int consumerBatchSize = 100;
    private long consumerReceiveTimeoutMs = 50;
    private int consumerPrefetch = 100;
    /** 高频点赞请求的目标元数据短缓存，降低每个请求同步查询 MySQL 的次数。 */
    private long metadataCacheTtlMs = 3000;
    private long metadataCacheMaxSize = 100000;
    /** 第几次失败后进入死信队列；计数保存在 Redis，应用重启后仍存在。 */
    private int maxRetries = 8;
    private long retryBaseDelayMs = 1000;

    private long reconcileInitialDelayMs = 120000;
    private long reconcileIntervalMs = 300000;
    private long quietPeriodMs = 30000;
}
