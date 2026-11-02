package com.zmt.picturebackendpro.service.impl.mq;

import com.rabbitmq.client.Channel;
import com.zmt.picturebackendpro.config.RabbitMqLikeProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 死信队列独立消费者：把死信消息写入 MySQL 审计表，供后续检索与人工处理。
 *
 * <p>职责与处理模型：</p>
 * <ol>
 *   <li>独立监听死信队列（{@code community.like.commands.dead.v1}），
 *       使用专属容器工厂 {@code communityLikeDeadLetterListenerContainerFactory}，
 *       与主点赞消费者线程隔离，避免审计逻辑拖慢主链路；</li>
 *   <li>调用 {@link RabbitMqLikeDeadLetterAuditService#persist(Message)} 把消息原文、
 *       诊断头与解析字段写入审计表；</li>
 *   <li>写库成功后先 ACK，再记录指标与结构化日志；</li>
 *   <li>写库失败时记录失败指标、输出告警日志、短暂 sleep 后 NACK 并重新入队，
 *       让消息留在 RabbitMQ 中等待数据库恢复后重试。</li>
 * </ol>
 *
 * <p>ACK 语义：</p>
 * <ul>
 *   <li>采用手动 ACK；必须保证 MySQL 写入成功后才 ACK，否则会丢失死信审计记录；</li>
 *   <li>若 MySQL 写入成功但 ACK 失败，消息可能重复到达。审计表以 {@code message_id} 为唯一键，
 *       通过 UPSERT 幂等合并并累加 {@code delivery_count}，因此重复投递不会产生重复行。</li>
 * </ul>
 *
 * <p>通过 {@link ConditionalOnProperty} 限定只在 {@code community.like.mode=rabbitmq} 时装配。</p>
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqLikeDeadLetterConsumer {

    /** 死信审计服务：负责把死信消息解析并持久化到 community_like_dead_letter_audit 表 */
    private final RabbitMqLikeDeadLetterAuditService auditService;

    /** 运行时指标：记录死信审计成功/失败次数等 */
    private final RabbitMqLikeRuntimeMetrics metrics;

    /** 点赞 MQ 配置：提供死信队列名与审计失败重试延迟等参数 */
    private final RabbitMqLikeProperties properties;

    /**
     * 构造器注入依赖。
     *
     * <p>使用构造器注入而非字段注入，便于测试替换，也能保证依赖不可变。</p>
     *
     * @param auditService 死信审计服务
     * @param metrics      运行时指标
     * @param properties   点赞 MQ 配置
     */
    public RabbitMqLikeDeadLetterConsumer(RabbitMqLikeDeadLetterAuditService auditService,
                                          RabbitMqLikeRuntimeMetrics metrics,
                                          RabbitMqLikeProperties properties) {
        this.auditService = auditService;
        this.metrics = metrics;
        this.properties = properties;
    }

    /**
     * 死信消费入口。
     *
     * <p>由 {@code communityLikeDeadLetterListenerContainerFactory} 调用，
     * 该工厂配置为手动 ACK、单消费者、prefetch=1、{@code defaultRequeueRejected=true}。</p>
     *
     * <p>成功路径：</p>
     * <ol>
     *   <li>调用审计服务写库；</li>
     *   <li>写库成功后 ACK 单条消息；</li>
     *   <li>记录审计成功指标；</li>
     *   <li>输出结构化审计日志（只含定位字段，不含完整消息正文）。</li>
     * </ol>
     *
     * <p>失败路径：</p>
     * <ol>
     *   <li>记录审计失败指标；</li>
     *   <li>输出告警日志，标记 {@code action=REQUEUE}；</li>
     *   <li>短暂 sleep 后 NACK 并重新入队，让消息留在 DLQ 中等待数据库恢复。</li>
     * </ol>
     *
     * @param message 死信原始消息
     * @param channel AMQP 通道，用于手动 ACK/NACK
     * @throws IOException ACK/NACK 底层 IO 异常
     */
    @RabbitListener(queues = "${community.like.rabbitmq.dead-queue:community.like.commands.dead.v1}",
            containerFactory = "communityLikeDeadLetterListenerContainerFactory")
    public void consume(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        // 提前取原始 messageId，仅用于失败日志；审计服务内部会做缺失/超长兜底
        String rawMessageId = message.getMessageProperties().getMessageId();
        try {
            RabbitMqLikeDeadLetterAuditService.AuditSummary summary = auditService.persist(message);
            // 先写 MySQL，再 ACK；若 ACK 失败，消息可能重复到达，表的唯一键会幂等合并
            channel.basicAck(deliveryTag, false);
            metrics.recordDeadLetterAuditPersisted();
            // 结构化日志：包含定位字段与审计表名，便于日志检索与告警规则匹配
            log.error("LIKE_DEAD_LETTER_AUDITED messageId={} eventId={} targetType={} targetId={} " +
                            "reason={} retryCount={} auditTable=community_like_dead_letter_audit",
                    summary.getMessageId(), summary.getEventId(), summary.getTargetType(), summary.getTargetId(),
                    summary.getReasonCode(), summary.getRetryCount());
        } catch (Exception auditFailure) {
            // 写库失败：不能 ACK，否则死信审计记录会永久丢失
            metrics.recordDeadLetterAuditFailure();
            log.error("LIKE_DEAD_LETTER_AUDIT_PERSIST_FAILED messageId={} queue={} action=REQUEUE",
                    rawMessageId, properties.getDeadQueue(), auditFailure);
            // 短暂退避，避免 MySQL 故障时立即重新入队形成紧密忙循环
            sleepBeforeRequeue();
            // MySQL 暂时不可用时，DLQ 消息仍留在 RabbitMQ；数据库恢复后审计消费者会继续处理
            channel.basicNack(deliveryTag, false, true);
        }
    }

    /**
     * 重新入队前的短暂退避。
     *
     * <p>当 MySQL 不可用导致审计持续失败时，若立即 NACK 重新入队，会形成
     * “投递 → 失败 → 立即重新入队”的紧密忙循环，持续刷日志并占用消费者线程。
     * 这里 sleep 一个可配置的最小时长（下限 1000ms），给数据库恢复留出时间窗口。</p>
     *
     * <p>中断处理：捕获 {@link InterruptedException} 后恢复线程中断标志
     * （{@link Thread#interrupt()}），不吞掉中断信号，符合线程中断的最佳实践。</p>
     */
    private void sleepBeforeRequeue() {
        try {
            // 下限 1000ms，防止配置为 0 或负值导致退避失效
            Thread.sleep(Math.max(1000L, properties.getDeadLetterAuditRetryDelayMs()));
        } catch (InterruptedException interrupted) {
            // 恢复中断标志，交由上层线程池决定是否终止
            Thread.currentThread().interrupt();
        }
    }
}