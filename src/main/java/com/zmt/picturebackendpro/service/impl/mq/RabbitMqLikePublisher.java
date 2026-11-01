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
 *   <li><b>Publisher Confirm</b>：等待 Broker ACK，NACK 则抛异常，由调用方决定重试；</li>
 *   <li><b>Mandatory Return 检查</b>：结合 {@code mandatory} 与 publisher returns，
 *       区分"Broker 已接收"与"消息实际路由到了队列"，防止消息被交换器丢弃却误判成功。</li>
 * </ol>
 * <p>
 * 使用场景：
 * <ul>
 *   <li>{@link #publishEvents(List)}：Stream relay 把一批 Redis Stream 事件发布到主队列；</li>
 *   <li>{@link #publishRaw}：消费者把达到最大重试次数的事件原样复制到死信队列。</li>
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
    /** JSON 序列化器：把事件对象序列化为消息体 */
    @Resource private ObjectMapper objectMapper;
    /** 点赞 MQ 配置：交换机、路由键、Confirm 超时时间等 */
    @Resource private RabbitMqLikeProperties properties;

    /**
     * 批量发布事件（主队列）。
     * <p>
     * 性能优化：先<b>依次发送</b>所有消息（不等待确认），再<b>统一等待</b>各自的 Confirm。
     * 相比"发送一条→等待确认→发送下一条"，可显著降低逐条等待确认的串行成本，
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
            Message message = buildMessage(body, event.getEventId(), 0);
            // correlationId 加入随机 UUID，保证同一 eventId 多次发布时 Correlation 不冲突
            CorrelationData correlation = new CorrelationData(event.getEventId() + "-" + UUID.randomUUID());
            rabbitTemplate.send(properties.getExchange(), properties.getRoutingKey(), message, correlation);
            pending.add(new PendingConfirm(event.getEventId(), correlation));
        }
        // 第二遍：逐条等待确认；任一条失败即抛异常，交由调用方整体重试
        for (PendingConfirm item : pending) {
            awaitConfirm(item.correlation, item.eventId);
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
     * @param exchange    目标交换机（通常是死信交换机）
     * @param routingKey  目标路由键（通常是死信路由键）
     * @param body        原始消息体
     * @param messageId   稳定消息 ID（用于日志与 Confirm 关联）
     * @param retryCount  已重试次数（写入消息头，供死信排查）
     * @throws Exception 发送失败、Confirm 超时或 NACK、消息不可路由
     */
    public void publishRaw(String exchange, String routingKey, byte[] body, String messageId, int retryCount)
            throws Exception {
        // messageId 可能为空（损坏消息），用 unknown 兜底，并加 UUID 保证 Confirm 关联唯一
        String confirmId = (messageId == null ? "unknown" : messageId) + "-" + UUID.randomUUID();
        CorrelationData correlation = new CorrelationData(confirmId);
        rabbitTemplate.send(exchange, routingKey, buildMessage(body, messageId, retryCount), correlation);
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
     *   <li>{@code x-like-retry-count}：自定义头，记录重试次数，便于死信排查。</li>
     * </ul>
     *
     * @param body       消息体
     * @param messageId  稳定消息 ID（可为空）
     * @param retryCount 重试次数
     * @return 构建好的 AMQP 消息
     */
    private Message buildMessage(byte[] body, String messageId, int retryCount) {
        MessageProperties p = new MessageProperties();
        p.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        p.setContentEncoding("UTF-8");
        p.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        if (messageId != null && !messageId.trim().isEmpty()) p.setMessageId(messageId);
        p.setHeader("x-like-retry-count", retryCount);
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
     * 保存 eventId（用于异常日志）与 CorrelationData（用于等待 Confirm），
     * 以便先批量发送、后统一等待确认。
     */
    private static final class PendingConfirm {
        private final String eventId;
        private final CorrelationData correlation;
        private PendingConfirm(String eventId, CorrelationData correlation) {
            this.eventId = eventId;
            this.correlation = correlation;
        }
    }
}