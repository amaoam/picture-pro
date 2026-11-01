package com.zmt.picturebackendpro.service.impl.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.zmt.picturebackendpro.config.LikeRedisProperties;
import com.zmt.picturebackendpro.config.RabbitMqLikeProperties;
import com.zmt.picturebackendpro.service.redis.RedisLikeEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * RabbitMQ 点赞事件批量消费者。
 * <p>
 * 核心处理模型（两层降级）：
 * <ol>
 *   <li><b>批量事务</b>：一批消息在一个 MySQL 事务内持久化，成功则一次性 ACK 整批，
 *       吞吐高、开销小；</li>
 *   <li><b>逐条事务降级</b>：批量事务失败（已整体回滚）后，按原顺序退化为逐条事务处理，
 *       隔离出"坏消息"，并保证成功的前缀尽快 ACK。</li>
 * </ol>
 * <p>
 * 顺序保证：遇到失败事件时，只 NACK 该事件及其后的未确认消息（{@code multiple=true}），
 * 让失败事件重新入队并排在前面，避免后续事件"越过"失败事件导致状态错乱
 * （例如 LIKE 与 UNLIKE 顺序颠倒）。
 * <p>
 * 重试与死信：
 * <ul>
 *   <li>每次失败在 Redis Hash 中累加重试次数（按稳定 messageId）；</li>
 *   <li>未达上限：<b>投递到延迟重试队列</b>并等待 Publisher Confirm，成功后 ACK 原消息，
 *       由 Retry Queue 的 TTL + DLX 在到期后自动回到主队列。这样<b>不会 sleep 阻塞
 *       listener 线程</b>，延迟由 MQ 承担；</li>
 *   <li>达到上限：先可靠发布到死信队列（等待 Publisher Confirm），成功后才 ACK 原消息，
 *       避免死信副本丢失。</li>
 * </ul>
 * <p>
 * pairVersion 持久化保护允许消息在不同重试轮次中乱序到达。
 * <p>
 * 通过 {@link ConditionalOnProperty} 限定只在 {@code community.like.mode=rabbitmq} 时启用。
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqLikeBatchConsumer {

    /** JSON 反序列化器，用于把消息 body 解析为 {@link RedisLikeEvent} */
    @Resource private ObjectMapper objectMapper;

    /** 持久化服务：负责把事件批量/单条写入 MySQL（方法上有事务注解） */
    @Resource private RabbitMqLikePersistenceService persistenceService;

    /** 发布器：负责主队列发布、延迟重试发布、死信复制（均等待 Confirm） */
    @Resource private RabbitMqLikePublisher publisher;

    /** 点赞 MQ 配置：队列名、最大重试次数、重试基础延迟、死信交换机等 */
    @Resource private RabbitMqLikeProperties properties;

    /** 运行时指标：批次大小、处理耗时、失败次数、重试次数、死信数等 */
    @Resource private RabbitMqLikeRuntimeMetrics metrics;

    /** 点赞专用 Redis 模板（按 Bean 名注入，与全局模板隔离） */
    @Resource(name = "likeStringRedisTemplate") private StringRedisTemplate redisTemplate;

    /** 点赞 Redis 配置：提供 key 前缀，用于拼装重试计数 key */
    @Resource private LikeRedisProperties redisProperties;

    /**
     * 批量消费入口。
     * <p>
     * 由 {@code communityLikeBatchListenerContainerFactory} 配置的批量监听容器调用，
     * 一次性拿到一批 {@link Message}，与底层 {@link Channel} 用于手动 ACK/NACK。
     * <p>
     * ACK 语义：使用 {@code basicAck(lastTag, true)} 一次性确认整批（multiple=true），
     * 因此必须保证批量事务已成功提交后才能 ACK，否则会丢消息。
     *
     * @param messages 本批消息（按投递顺序）
     * @param channel  AMQP 通道，用于 ack/nack
     */
    @RabbitListener(queues = "${community.like.rabbitmq.queue:community.like.commands.v1}",
            containerFactory = "communityLikeBatchListenerContainerFactory")
    public void consume(List<Message> messages, Channel channel) throws IOException {
        if (messages == null || messages.isEmpty()) return;
        long started = System.nanoTime();
        int size = messages.size();
        // 取最后一条消息的 deliveryTag，用于一次性 ACK/NACK 整批
        long lastTag = messages.get(size - 1).getMessageProperties().getDeliveryTag();
        metrics.beginBatch(size);
        try {
            List<RedisLikeEvent> events = decodeBatch(messages);
            // persistWithMetrics 内部调用事务代理；正常返回代表整批 MySQL 事务已经提交。
            persistWithMetrics(events);
            // 整批成功：一次性 ACK 整批
            channel.basicAck(lastTag, true);
            // 清理每条事件的重试计数，避免后续同 eventId 重投时误判为已重试
            for (RedisLikeEvent event : events) clearRetryCount(event.getEventId());
            metrics.recordProcessed(size, System.nanoTime() - started);
        } catch (Exception batchFailure) {
            metrics.recordFailure();
            log.warn("RabbitMQ like batch transaction failed; switching to ordered per-message fallback. size={}",
                    size, batchFailure);
            // 批量事务失败已整体回滚。逐条事务可隔离坏消息，且遇到失败时不让后续事件越过它。
            processBatchFallback(messages, channel, lastTag, started);
        } finally {
            metrics.endBatch(size);
        }
    }

    /**
     * 批量反序列化并做一致性校验。
     * <p>
     * 校验规则：若消息头 messageId 存在，则必须与 body 中的 eventId 一致，
     * 防止"消息头与消息体不匹配"导致的幂等失效或对账错误。
     * <p>
     * 注意：本方法抛异常会触发上层批量事务失败并进入逐条降级处理。
     *
     * @param messages 本批原始消息
     * @return 解析后的事件列表（与输入顺序一致）
     */
    private List<RedisLikeEvent> decodeBatch(List<Message> messages) throws IOException {
        List<RedisLikeEvent> events = new ArrayList<>(messages.size());
        for (Message message : messages) {
            RedisLikeEvent event = objectMapper.readValue(message.getBody(), RedisLikeEvent.class);
            String headerId = message.getMessageProperties().getMessageId();
            if (headerId != null && !headerId.trim().isEmpty()
                    && event.getEventId() != null && !headerId.equals(event.getEventId())) {
                throw new IllegalArgumentException("RabbitMQ messageId 与 body.eventId 不一致：" + headerId);
            }
            events.add(event);
        }
        return events;
    }

    /**
     * 批量失败后的逐条事务降级处理。
     * <p>
     * 处理策略：
     * <ul>
     *   <li>按原顺序逐条处理；成功则立即 ACK 单条（multiple=false）；</li>
     *   <li>遇到失败事件：累加重试计数：
     *     <ul>
     *       <li>未达上限：投递到<b>延迟重试队列</b>并等待 Confirm，成功后 ACK 原消息。
     *           不 sleep，延迟由 Retry Queue 的 TTL + DLX 承担；</li>
     *       <li>达到上限：先发布到死信队列并等待 Confirm，成功后 ACK 原消息；</li>
     *     </ul>
     *   </li>
     *   <li>任何"投递未确认"或"重试计数器不可用"的情况，都 NACK 该事件及其后的未确认消息
     *       （{@code multiple=true}），确保顺序且不丢消息。</li>
     * </ul>
     * <p>
     * 已 ACK 的前缀不会因后续 NACK 而回滚，这依赖 {@code multiple=true} 只作用于
     * 当前 deliveryTag 及其之前的未确认消息这一 AMQP 语义。
     *
     * @param messages 本批消息
     * @param channel  AMQP 通道
     * @param lastTag  本批最后一条消息的 deliveryTag（用于 NACK 未确认后缀）
     * @param started  批次开始时间（纳秒），用于统计
     */
    private void processBatchFallback(List<Message> messages, Channel channel, long lastTag, long started)
            throws IOException {
        int persisted = 0;
        for (Message message : messages) {
            long deliveryTag = message.getMessageProperties().getDeliveryTag();
            String stableId = stableMessageId(message);
            try {
                RedisLikeEvent event = objectMapper.readValue(message.getBody(), RedisLikeEvent.class);
                if (event.getEventId() == null || event.getEventId().trim().isEmpty()) {
                    throw new IllegalArgumentException("点赞消息缺少 eventId");
                }
                String headerId = message.getMessageProperties().getMessageId();
                // 兼容没有 messageId header 的历史消息：解析成功后用 body.eventId 作为重试键。
                if (headerId == null || headerId.trim().isEmpty()) stableId = event.getEventId();
                // 逐条处理时同样校验头体一致，避免绕过批量阶段的校验
                if (headerId != null && !headerId.equals(event.getEventId())) {
                    throw new IllegalArgumentException("RabbitMQ messageId 与 body.eventId 不一致：" + stableId);
                }
                persistWithMetrics(Collections.singletonList(event));
                channel.basicAck(deliveryTag, false);
                clearRetryCount(event.getEventId());
                persisted++;
            } catch (Exception eventFailure) {
                long attempts;
                try {
                    attempts = incrementRetryCount(stableId);
                } catch (RuntimeException retryStoreFailure) {
                    // 重试计数不可用（Redis 故障）：不能判断是否该进死信，保守重回未确认后缀。
                    log.error("Redis retry counter unavailable; requeueing unacked suffix", retryStoreFailure);
                    channel.basicNack(lastTag, true, true);
                    metrics.recordProcessed(persisted, System.nanoTime() - started);
                    return;
                }

                int maxAttempts = Math.max(1, properties.getMaxRetries());
                if (attempts >= maxAttempts) {
                    try {
                        // 发布死信副本并等待 Publisher Confirm；成功后才 ACK 原消息。
                        publisher.publishRaw(properties.getDeadExchange(), properties.getDeadRoutingKey(),
                                message.getBody(), stableId, (int) Math.min(Integer.MAX_VALUE, attempts));
                        channel.basicAck(deliveryTag, false);
                        clearRetryCount(stableId);
                        metrics.recordDeadLettered();
                        log.error("Like event moved to dead-letter queue after {} failures: eventId={}",
                                attempts, stableId, eventFailure);
                        continue;
                    } catch (Exception deadLetterFailure) {
                        // 死信副本未确认：不能 ACK 原消息，否则可能永久丢失事件。
                        log.error("Dead-letter publish was not confirmed; requeueing unacked suffix. eventId={}",
                                stableId, deadLetterFailure);
                        channel.basicNack(lastTag, true, true);
                        metrics.recordProcessed(persisted, System.nanoTime() - started);
                        return;
                    }
                }

                metrics.recordRetryAttempt();
                try {
                    // 固定短延迟，避免指数 TTL 在单个 retry queue 中形成长消息阻塞。
                    // 延迟由 Retry Queue 的 TTL 承担，listener 线程不 sleep。
                    long delayMs = Math.max(100L, properties.getRetryBaseDelayMs());
                    publisher.publishRetryRaw(message.getBody(), stableId,
                            (int) Math.min(Integer.MAX_VALUE, attempts), delayMs);
                    // 只有 retry queue 收到 Confirm 后才能确认原消息。
                    channel.basicAck(deliveryTag, false);
                    log.warn("Like event sent to delayed retry queue: eventId={}, attempt={}/{}, delayMs={}",
                            stableId, attempts, maxAttempts, delayMs, eventFailure);
                } catch (Exception retryPublishFailure) {
                    // 延迟重试消息未确认：不能 ACK 原消息，重新入队未确认后缀。
                    log.error("Retry publish was not confirmed; requeueing unacked suffix. eventId={}",
                            stableId, retryPublishFailure);
                    channel.basicNack(lastTag, true, true);
                    metrics.recordProcessed(persisted, System.nanoTime() - started);
                    return;
                }
            }
        }
        metrics.recordProcessed(persisted, System.nanoTime() - started);
    }

    /**
     * 执行持久化并单独统计 MySQL 耗时。
     * <p>
     * 将 MySQL 持久化耗时与整个 Rabbit listener callback 耗时分开统计，
     * 便于定位瓶颈（DB 慢 vs MQ/序列化慢）。
     * <p>
     * 异常处理：持久化抛 RuntimeException 时记录 {@code recordPersistenceFailure}，
     * 然后原样抛出，由上层（批量或逐条）决定降级策略。
     * <p>
     * {@code finally} 中记录耗时，确保失败路径也能统计到持久化开销。
     *
     * @param events 待持久化的事件（批量或单条）
     */
    private void persistWithMetrics(List<RedisLikeEvent> events) {
        long started = System.nanoTime();
        try {
            persistenceService.persistBatch(events);
        } catch (RuntimeException exception) {
            metrics.recordPersistenceFailure();
            throw exception;
        } finally {
            metrics.recordPersistence(System.nanoTime() - started);
        }
    }

    /**
     * 生成稳定的消息标识，用于重试计数 key。
     * <p>
     * 优先使用消息头 messageId；若缺失（例如消息 JSON 损坏且未设置 messageId），
     * 则对消息体做 SHA-256 摘要，保证同一条消息在重投时使用相同的计数键，
     * 避免因缺少 ID 导致重试次数无法累加、无限重投。
     * <p>
     * 极端情况下（JVM 不支持 SHA-256，实际不可能）退化为基于消息体的命名 UUID。
     *
     * @param message 原始消息
     * @return 稳定的消息标识（以 {@code malformed-} 前缀区分无 messageId 场景）
     */
    private String stableMessageId(Message message) {
        String messageId = message.getMessageProperties().getMessageId();
        if (messageId != null && !messageId.trim().isEmpty()) return messageId;
        // 即使消息 JSON 损坏且没有 messageId，也让同一条消息在重投时使用稳定的失败计数键。
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(message.getBody());
            StringBuilder out = new StringBuilder("malformed-");
            for (byte b : digest) out.append(String.format("%02x", b & 0xff));
            return out.toString();
        } catch (NoSuchAlgorithmException impossible) {
            return "malformed-" + UUID.nameUUIDFromBytes(message.getBody());
        }
    }

    /**
     * 原子递增事件的重试次数（Redis Hash）。
     * <p>
     * key 结构：{@code {keyPrefix}:mq:retry-counts}，field 为稳定消息 ID。
     * 使用 HINCRBY 保证并发消费时的计数原子性。
     * <p>
     * 注意：返回值 {@code null} 时兜底为 1，避免 Redis 客户端异常返回空值导致 NPE。
     *
     * @param messageId 稳定消息 ID
     * @return 本次递增后的计数（至少为 1）
     */
    private long incrementRetryCount(String messageId) {
        String key = redisProperties.getKeyPrefix() + ":mq:retry-counts";
        Long count = redisTemplate.opsForHash().increment(key, messageId, 1L);
        return count == null ? 1L : count;
    }

    /**
     * 清理已完成事件的重试计数。
     * <p>
     * 失败容错：清理失败只影响"后续同 eventId 重投时的重试预算"，
     * 不应因为清理异常而把已提交并 ACK 的事件重新判为失败，
     * 因此这里只记录 warn 日志，不向上抛出。
     *
     * @param messageId 稳定消息 ID（可为空，空则跳过）
     */
    private void clearRetryCount(String messageId) {
        if (messageId == null || messageId.trim().isEmpty()) return;
        try {
            redisTemplate.opsForHash().delete(redisProperties.getKeyPrefix() + ":mq:retry-counts", messageId);
        } catch (RuntimeException exception) {
            // 清理失败不应把已提交并 ACK 的事件重新判为失败。
            log.warn("Could not clear completed like retry counter: eventId={}", messageId, exception);
        }
    }
}