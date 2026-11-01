package com.zmt.picturebackendpro.service.impl.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zmt.picturebackendpro.config.RabbitMqLikeProperties;
import com.zmt.picturebackendpro.service.redis.RedisLikeEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * RabbitMQ 点赞事件的统一可靠发布入口（RabbitMQ 模式）。
 * <p>
 * 可靠投递三道防线：
 * <ol>
 *   <li><b>持久化消息</b>：{@link MessageDeliveryMode#PERSISTENT}，Broker 落盘后返回确认；</li>
 *   <li><b>Publisher Confirm</b>：等待 Broker ACK，NACK 或超时则抛异常，由调用方决定重试；</li>
 *   <li><b>Mandatory Return 检查</b>：结合 {@code mandatory} 与 publisher returns，
 *       区分"Broker 已接收"与"消息实际路由到了队列"，防止消息被交换器丢弃却误判成功。</li>
 * </ol>
 * <p>
 * 使用场景：
 * <ul>
 *   <li>{@link #publishEvents(List)}：Stream relay 把一批 Redis Stream 事件发布到主队列；</li>
 *   <li>{@link #publishRaw}：消费者把达到最大重试次数的事件原样复制到死信队列；</li>
 *   <li>{@link #publishRetryRaw}：消费者遇到临时失败时，把消息投递到延迟重试队列，
 *       由 TTL + DLX 在延迟到期后自动回到主队列，避免阻塞 listener 线程。</li>
 * </ul>
 * 只有本类方法<b>正常返回</b>（即已获得 Confirm 且无 Returned）时，
 * 调用方才可以 ACK 原始消息 / 标记 Stream 事件已投递。
 * <p>
 * 通过 {@link ConditionalOnProperty} 限定只在 {@code community.like.mode=rabbitmq} 时装配。
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqLikePublisher {

    /** Spring AMQP 模板：负责实际发送消息与 Confirm 回调注册 */
    @Resource private RabbitTemplate rabbitTemplate;

    /** JSON 序列化器：把 Redis Stream 事件对象序列化为消息体 */
    @Resource private ObjectMapper objectMapper;

    /** 点赞 MQ 配置：主交换机、路由键、重试交换机/路由键、Confirm 超时时间等 */
    @Resource private RabbitMqLikeProperties properties;

    /**
     * 批量发布事件（主队列）。
     * <p>
     * 性能优化：先<b>依次发送</b>所有消息（不等待确认），再<b>统一等待</b>各自的 Confirm。
     * 相比"发送一条 → 等待确认 → 发送下一条"，可显著降低逐条等待确认的串行成本，
     * 使一个 Stream 批次的发布延迟近似等于单条确认延迟而非 N 倍。
     * <p>
     * 失败语义：只要任意一条消息 Confirm 失败（NACK / 超时 / Returned），
     * 即抛出异常；调用方不得 ACK 对应 Stream 批次，应整体重试。
     * 已成功发送的消息可能重复投递，由消费端基于 eventId 的幂等机制兜底。
     *
     * @param events 待发布事件（来自 Redis Stream 的一个批次）
     * @throws Exception 序列化失败、发送失败、Confirm 超时或 NACK、消息不可路由
     */
    public void publishEvents(List<RedisLikeEvent> events) throws Exception {
        List<PendingConfirm> pending = new ArrayList<>(events.size());

        // 第一遍：全部发送，仅登记 CorrelationData，不等待确认
        for (RedisLikeEvent event : events) {
            byte[] body = objectMapper.writeValueAsBytes(event);
            // correlationId 加入随机 UUID，保证同一 eventId 多次发布时 Correlation 不冲突
            CorrelationData correlation = new CorrelationData(event.getEventId() + "-" + UUID.randomUUID());
            // retryCount 从 0 开始；主队列发布不设置 TTL
            rabbitTemplate.send(properties.getExchange(), properties.getRoutingKey(),
                    buildMessage(body, event.getEventId(), 0, null), correlation);
            pending.add(new PendingConfirm(event.getEventId(), correlation));
        }

        // 第二遍：逐条等待确认；任一条失败即抛异常，交由调用方整体重试
        for (PendingConfirm item : pending) {
            awaitConfirm(item.correlation, item.messageId);
        }
    }

    /**
     * 发布原始消息（用于死信复制）。
     * <p>
     * 消费者在事件达到最大重试次数后，调用本方法把<b>原始消息体</b>复制到死信队列；
     * 只有本方法正常返回（已获得 Confirm 且无 Returned），调用方才 ACK 原消息。
     * 这样保证"死信副本落盘成功"与"原消息 ACK"之间不会出现丢失窗口。
     * <p>
     * 注意：本方法不重新序列化事件，而是直接转发 {@code body}，
     * 以保留原始消息内容（即使消息体损坏或版本不兼容，也能原样留存以便排查）。
     *
     * @param exchange   目标交换机（通常是死信交换机）
     * @param routingKey 目标路由键（通常是死信路由键）
     * @param body       原始消息体
     * @param messageId  稳定消息 ID（用于日志与 Confirm 关联，可为空）
     * @param retryCount 已重试次数（写入消息头，供死信排查）
     * @throws Exception 发送失败、Confirm 超时或 NACK、消息不可路由
     */
    public void publishRaw(String exchange, String routingKey, byte[] body, String messageId, int retryCount)
            throws Exception {
        // 死信复制不设置 TTL，expirationMs 传 null
        publishRawInternal(exchange, routingKey, body, messageId, retryCount, null);
    }

    /**
     * 临时失败时投递到延迟重试队列。
     * <p>
     * 消费者只有在 Confirm 成功后才能 ACK 原消息。
     * Retry Queue 上的消息设置了 TTL，到期后通过队列配置的 DLX 自动回到主队列，
     * 从而避免在唯一的 listener 线程里 {@code sleep} 阻塞消费。
     * <p>
     * 与 {@link #publishRaw} 的区别：目标交换机/路由键固定使用配置里的
     * {@code retryExchange}/{@code retryRoutingKey}，并且为消息设置 expiration（TTL）。
     * <p>
     * TTL 下限 100ms，防止调用方传入 0 或负数导致消息立即过期、退化成忙循环。
     *
     * @param body       原始消息体
     * @param messageId  稳定消息 ID（可为空，用于 Confirm 关联与日志）
     * @param retryCount 已重试次数（写入消息头，供消费端判断是否达到上限）
     * @param delayMs    延迟时间（毫秒），作为消息 TTL
     * @throws Exception 发送失败、Confirm 超时或 NACK、消息不可路由
     */
    public void publishRetryRaw(byte[] body, String messageId, int retryCount, long delayMs) throws Exception {
        long ttl = Math.max(100L, delayMs);
        publishRawInternal(properties.getRetryExchange(), properties.getRetryRoutingKey(),
                body, messageId, retryCount, ttl);
    }

    /**
     * 原始消息发送的统一实现，供死信复制与延迟重试复用。
     * <p>
     * 流程：构建 CorrelationData → 构建消息（可选 TTL）→ 发送 → 等待 Confirm 并校验可路由。
     * <p>
     * CorrelationData 的 id 中附加随机 UUID：
     * <ul>
     *   <li>messageId 可能为空（损坏消息），用 {@code unknown} 兜底；</li>
     *   <li>同一 messageId 可能被多次发送（重试），UUID 保证每次 Confirm 关联唯一。</li>
     * </ul>
     *
     * @param exchange     目标交换机
     * @param routingKey   目标路由键
     * @param body         原始消息体
     * @param messageId    稳定消息 ID（可为空）
     * @param retryCount   重试次数
     * @param expirationMs 消息 TTL（毫秒），为 null 表示不设置（不死信/不延迟）
     * @throws Exception 发送失败、Confirm 超时或 NACK、消息不可路由
     */
    private void publishRawInternal(String exchange, String routingKey, byte[] body,
                                    String messageId, int retryCount, Long expirationMs) throws Exception {
        String confirmId = (messageId == null ? "unknown" : messageId) + "-" + UUID.randomUUID();
        CorrelationData correlation = new CorrelationData(confirmId);
        rabbitTemplate.send(exchange, routingKey,
                buildMessage(body, messageId, retryCount, expirationMs), correlation);
        awaitConfirm(correlation, messageId);
    }

    /**
     * 构建消息（统一设置属性）。
     * <p>
     * 关键属性：
     * <ul>
     *   <li>{@code content-type=application/json}、{@code encoding=UTF-8}：便于消费端解析与排查；</li>
     *   <li>{@link MessageDeliveryMode#PERSISTENT}：消息持久化，Broker 重启不丢失；</li>
     *   <li>{@code messageId}：稳定 ID，供消费端幂等与重试计数使用；</li>
     *   <li>{@code x-like-retry-count}：自定义头，记录重试次数，便于死信排查；</li>
     *   <li>{@code expiration}：消息 TTL，仅延迟重试队列需要；为 null 时不设置。</li>
     * </ul>
     *
     * @param body         消息体
     * @param messageId    稳定消息 ID（可为空）
     * @param retryCount   重试次数
     * @param expirationMs 消息 TTL（毫秒），可为 null
     * @return 构建好的 AMQP 消息
     */
    private Message buildMessage(byte[] body, String messageId, int retryCount, Long expirationMs) {
        MessageProperties p = new MessageProperties();
        p.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        p.setContentEncoding("UTF-8");
        p.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        // messageId 为空字符串或全空白时不设置，避免消费端误判
        if (messageId != null && !messageId.trim().isEmpty()) p.setMessageId(messageId);
        p.setHeader("x-like-retry-count", retryCount);
        // 仅延迟重试场景设置 TTL；普通发送与死信复制不设置
        if (expirationMs != null) p.setExpiration(String.valueOf(expirationMs));
        return new Message(body, p);
    }

    /**
     * 等待单条消息的 Broker Confirm，并校验可路由性。
     * <p>
     * 双重校验：
     * <ol>
     *   <li><b>Confirm</b>：Broker 是否 ACK。NACK 或超时抛异常，调用方不得 ACK 原消息；</li>
     *   <li><b>Returned</b>：{@code mandatory=true} 时，若消息无法路由到任何队列，
     *       Broker 会通过 basic.return 返回；此时 Confirm 仍可能为 ACK（Broker 已接收），
     *       但消息实际已丢失。必须检查 {@link CorrelationData#getReturned()}，
     *       非空则视为失败。</li>
     * </ol>
     * 超时下限 100ms（防止配置为 0 导致立即超时）。
     *
     * @param correlation 本次发送的关联数据
     * @param messageId   稳定消息 ID（用于异常信息）
     * @throws Exception Confirm 超时、NACK 或消息不可路由
     */
    private void awaitConfirm(CorrelationData correlation, String messageId) throws Exception {
        CorrelationData.Confirm confirm = correlation.getFuture().get(
                Math.max(100L, properties.getPublisherConfirmTimeoutMs()), TimeUnit.MILLISECONDS);
        if (!confirm.isAck()) {
            throw new IllegalStateException("RabbitMQ NACK: eventId=" + messageId + ", cause=" + confirm.getReason());
        }
        // mandatory + publisher returns 用于区分“Broker 接收”与“实际路由到队列”。
        if (correlation.getReturned() != null) {
            throw new IllegalStateException("RabbitMQ 消息不可路由：eventId=" + messageId
                    + ", replyText=" + correlation.getReturned().getReplyText());
        }
    }

    /**
     * 待确认消息的登记项。
     * 保存 messageId（用于异常日志）与 CorrelationData（用于等待 Confirm），
     * 以便先批量发送、后统一等待确认。
     */
    private static final class PendingConfirm {
        /** 消息 ID（批量发布场景下为 Redis Stream 的 eventId） */
        private final String messageId;
        /** 本次发送的 Confirm 关联数据 */
        private final CorrelationData correlation;

        private PendingConfirm(String messageId, CorrelationData correlation) {
            this.messageId = messageId;
            this.correlation = correlation;
        }
    }
}