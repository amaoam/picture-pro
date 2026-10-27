package com.zmt.picturebackendpro.service.redis;

import com.zmt.picturebackendpro.config.LikeRedisProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Redis 点赞事件流消费者 (V3.0 架构)
 * 核心设计：
 * 1. 每个 Redis Stream 分片（Shard）配备一个固定的 Worker 线程，单线程按 Stream ID 顺序消费，保证消息顺序性。
 * 2. 严格遵循“先持久化、后 ACK、再清理”的策略，结合事件日志表（Event Ledger）确保消息不丢失且幂等。
 * 3. 实现 SmartLifecycle 接口，随 Spring 容器生命周期平滑启停。
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "community.like.mode", havingValue = "redis-stream")
public class RedisLikeStreamConsumer implements SmartLifecycle {

    @Resource(name = "likeStringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private LikeRedisProperties properties;

    @Resource
    private LikeRedisKeyFactory keyFactory;

    @Resource
    private RedisLikePersistenceService persistenceService;

    @Resource
    private MeterRegistry meterRegistry;

    /** 运行状态标志，防止重复启动或关闭 */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** 消费线程池，线程数等于分片数（Shard Count） */
    private volatile ExecutorService workerPool;

    /**
     * 启动生命周期：
     * 1. 校验配置参数。
     * 2. 在所有 Redis Stream 分片上预先创建消费组（Consumer Group）。
     * 3. 初始化固定大小的线程池，启动各个 Shard 的消费循环。
     * 4. 注册 Micrometer 监控指标（Pending 消息数、Stream 总长度）。
     */
    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        validateProperties();

        // 所有 consumer group 在接收流量前创建；从 0-0 开始，确保历史事件不会被遗漏。
        for (int shard = 0; shard < keyFactory.getShardCount(); shard++) {
            ensureConsumerGroup(keyFactory.streamKey(shard));
        }

        // 自定义线程工厂，为每个分片 Worker 命名，便于排查日志
        ThreadFactory threadFactory = new ThreadFactory() {
            private int sequence = 0;
            @Override
            public synchronized Thread newThread(Runnable task) {
                Thread thread = new Thread(task, "like-v3-stream-" + sequence++);
                thread.setDaemon(false);
                return thread;
            }
        };

        // 创建固定大小的线程池，分片数 = 线程数
        workerPool = Executors.newFixedThreadPool(keyFactory.getShardCount(), threadFactory);
        for (int shard = 0; shard < keyFactory.getShardCount(); shard++) {
            final int assignedShard = shard;
            workerPool.submit(() -> consumeShard(assignedShard));
        }

        // 注册指标：统计所有 Redis Stream 中尚未 ACK 的挂起消息总数 (Pending Messages)
        Gauge.builder(
                        "community.like.redis.stream.pending",
                        this,
                        consumer -> consumer.pendingMessageCount()
                )
                .description("Total pending Redis Stream messages not yet acknowledged")
                .register(meterRegistry);

        // 注册指标：统计所有 Redis Stream 中的物理消息总数 (Entries)
        Gauge.builder(
                        "community.like.redis.stream.entries",
                        this,
                        consumer -> consumer.streamEntryCount()
                )
                .description("Total physical entries remaining across V3 Redis Streams")
                .register(meterRegistry);

        log.info("Redis like Stream consumer started: shards={}, group={}, batchSize={}",
                keyFactory.getShardCount(), properties.getConsumerGroup(), properties.getBatchSize());
    }

    /**
     * 核心消费逻辑（单分片）：
     * 循环拉取、持久化、ACK 确认、清理，并在发生异常时进行指数退避重试。
     * DB 失败时保留 pending 状态并暂停重试，防止乱序越过失败事件。
     */
    private void consumeShard(int shard) {
        String streamKey = keyFactory.streamKey(shard);
        // 固定 consumer 名称，确保服务重启后能重新读取属于该 consumer 的未完成 pending 消息。
        // 注：当前版本要求单实例部署，多副本部署前需增加分布式租约/fencing机制。
        String consumerName = String.format("like-v3-consumer-%02d", shard);
        Consumer consumer = Consumer.from(properties.getConsumerGroup(), consumerName);
        StreamOperations<String, Object, Object> streamOps = redisTemplate.opsForStream();

        long idleDelay = Math.max(1L, properties.getIdleMinDelayMs());
        int consecutiveFailures = 0;

        while (running.get()) {
            try {
                // 1. 优先读取当前 consumer 的 Pending 消息（即之前读取但未 ACK 的消息）。
                // 如果 DB 提交成功但 ACK 丢失，重启或重试时可以通过幂等表跳过重复。
                List<MapRecord<String, Object, Object>> records = streamOps.read(
                        consumer,
                        StreamReadOptions.empty().count(properties.getBatchSize()),
                        StreamOffset.create(streamKey, ReadOffset.from("0-0")));

                // 2. 如果没有自己的 Pending 消息，则读取 Group 中尚未投递的新消息。
                // 使用 BLOCK 阻塞读，避免轮询导致 CPU 飙升。
                if (records == null || records.isEmpty()) {
                    records = streamOps.read(
                            consumer,
                            StreamReadOptions.empty()
                                    .count(properties.getBatchSize())
                                    .block(Duration.ofMillis(Math.max(1L, properties.getReadBlockMs()))),
                            StreamOffset.create(streamKey, ReadOffset.lastConsumed()));
                }

                // 3. 若依然没有消息，执行空闲退避策略（避免高频空轮询）
                if (records == null || records.isEmpty()) {
                    sleepQuietly(idleDelay);
                    long minIdle = Math.max(1L, properties.getIdleMinDelayMs());
                    long maxIdle = Math.max(minIdle, properties.getIdleMaxDelayMs());
                    // 动态增加 idleDelay，直到达到 maxIdle 上限
                    idleDelay = Math.min(maxIdle, idleDelay > maxIdle / 2 ? maxIdle : Math.max(minIdle, idleDelay * 2));
                    continue;
                }

                long startedAt = System.nanoTime();
                List<RedisLikeEvent> events = new ArrayList<>(records.size());
                for (MapRecord<String, Object, Object> record : records) {
                    events.add(toEvent(record));
                }

                // 4. 批量持久化到 MySQL（该方法应通过 Spring 事务代理执行，正常返回代表事务已提交）
                persistenceService.persistBatch(events);

                // 5. 数据库事务提交成功后，发送 XACK 确认消费完成。
                // 若 ACK 失败会导致消息重新投递，但 Event Ledger 保证幂等防重。
                RecordId[] recordIds = records.stream().map(MapRecord::getId).toArray(RecordId[]::new);
                streamOps.acknowledge(streamKey, properties.getConsumerGroup(), recordIds);

                // 6. 清理已完成的 Stream 记录（XDEL），控制 Redis 内存增长。
                // 即使 XDEL 失败，消息也已经成功 ACK，此处仅记录告警指标，不影响主流程。
                try {
                    Long deleted = streamOps.delete(streamKey, recordIds);
                    if (deleted == null || deleted < recordIds.length) {
                        Counter.builder("community.like.redis.stream.cleanup.incomplete")
                                .description("Acknowledged Stream entries that could not all be deleted")
                                .register(meterRegistry).increment();
                        log.warn("Some acknowledged Stream entries remain stored: stream={}, expected={}, deleted={}",
                                streamKey, recordIds.length, deleted);
                    }
                } catch (RuntimeException cleanupException) {
                    Counter.builder("community.like.redis.stream.cleanup.failures")
                            .description("Failed to delete already-acknowledged Redis Stream entries")
                            .register(meterRegistry).increment();
                    log.warn("MySQL events were committed and Stream ACK sent, but XDEL cleanup failed: stream={}",
                            streamKey, cleanupException);
                }

                // 7. 记录性能指标和延迟埋点
                Timer.builder("community.like.redis.stream.consume.batch.duration")
                        .description("Time for MySQL persistence plus Redis Stream ACK")
                        .register(meterRegistry)
                        .record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);

                Counter.builder("community.like.redis.stream.consume.messages")
                        .description("Redis Stream messages successfully persisted and acknowledged")
                        .register(meterRegistry).increment(records.size());

                // 计算端到端延迟（从 Lua 生成时间到消费完成），标签不含高基数字段防止内存泄漏
                long now = System.currentTimeMillis();
                for (RedisLikeEvent event : events) {
                    long lag = Math.max(0L, now - event.createdAtAsLong());
                    Timer.builder("community.like.redis.event.persistence.lag")
                            .description("Redis event createdAt to DB persistence/ack completion")
                            .register(meterRegistry)
                            .record(lag, TimeUnit.MILLISECONDS);
                }

                // 消费成功，重置退避延迟与失败计数
                idleDelay = Math.max(1L, properties.getIdleMinDelayMs());
                consecutiveFailures = 0;

            } catch (InterruptedException interrupted) {
                // 收到中断信号，优雅退出
                Thread.currentThread().interrupt();
                return;
            } catch (Exception exception) {
                consecutiveFailures++;

                Counter.builder("community.like.redis.stream.consume.failures")
                        .description("Redis Stream consumer batch failures")
                        .register(meterRegistry).increment();

                long delay = exponentialErrorDelay(consecutiveFailures);

                // 按照阈值打印错误或警告日志，避免日志泛滥
                if (consecutiveFailures == 1
                        || consecutiveFailures % Math.max(1, properties.getRepeatedErrorWarningThreshold()) == 0) {
                    log.error("Redis like Stream shard failed; messages remain pending. shard={}, stream={}, consumer={}, attempts={}, retryDelayMs={}",
                            shard, streamKey, consumerName, consecutiveFailures, delay, exception);
                } else {
                    log.warn("Redis like Stream shard will retry pending messages. shard={}, attempts={}, retryDelayMs={}",
                            shard, consecutiveFailures, delay);
                }

                try {
                    sleepQuietly(delay);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /**
     * 将 Redis 的 MapRecord 映射转换成实体对象 RedisLikeEvent
     */
    private RedisLikeEvent toEvent(MapRecord<String, Object, Object> record) {
        Map<Object, Object> values = record.getValue();
        RedisLikeEvent event = new RedisLikeEvent();
        event.setEventId(required(values, "eventId"));
        event.setRequestId(required(values, "requestId"));
        event.setTargetType(required(values, "targetType"));
        event.setTargetId(required(values, "targetId"));
        event.setUserId(required(values, "userId"));
        event.setOwnerUserId(valueOrEmpty(values, "ownerUserId"));
        event.setDesiredState(required(values, "desiredState"));
        event.setDelta(required(values, "delta"));
        event.setPairVersion(required(values, "pairVersion"));
        event.setCreatedAtMs(required(values, "createdAtMs"));
        return event;
    }

    /** 校验并获取必填字段 */
    private String required(Map<Object, Object> values, String key) {
        Object value = values.get(key);
        if (value == null || value.toString().trim().isEmpty()) {
            throw new IllegalArgumentException("Redis Stream event missing field: " + key);
        }
        return value.toString();
    }

    /** 获取可选字段，为空时返回空字符串 */
    private String valueOrEmpty(Map<Object, Object> values, String key) {
        Object value = values.get(key);
        return value == null ? "" : value.toString();
    }

    /** 计算指数退避重试延迟时间 */
    private long exponentialErrorDelay(int attempts) {
        long minimum = Math.max(1L, properties.getErrorMinDelayMs());
        long maximum = Math.max(minimum, properties.getErrorMaxDelayMs());
        int exponent = Math.min(20, Math.max(0, attempts - 1));
        long multiplier = 1L << exponent;
        if (minimum > maximum / multiplier) {
            return maximum;
        }
        return Math.min(maximum, minimum * multiplier);
    }

    /** 安全睡眠线程（响应中断异常） */
    private void sleepQuietly(long millis) throws InterruptedException {
        if (millis > 0) {
            Thread.sleep(millis);
        }
    }

    /**
     * 确保消费者组存在。如果已存在（BUSYGROUP 错误），则安全忽略。
     */
    private void ensureConsumerGroup(String streamKey) {
        try {
            redisTemplate.opsForStream().createGroup(streamKey, ReadOffset.from("0-0"), properties.getConsumerGroup());
        } catch (DataAccessException exception) {
            // 重启时 group 已存在属于正常情况，其它 Redis 错误必须向上抛出，阻止应用误以为消费已成功启动。
            String message = exception.getMostSpecificCause() == null
                    ? exception.getMessage() : exception.getMostSpecificCause().getMessage();
            if (message == null || !message.toUpperCase().contains("BUSYGROUP")) {
                throw exception;
            }
        }
    }

    /** 统计所有分片中待处理（Pending）的消息总数 */
    private long pendingMessageCount() {
        long total = 0L;

        for (int shard = 0; shard < keyFactory.getShardCount(); shard++) {
            try {
                org.springframework.data.redis.connection.stream.PendingMessagesSummary summary =
                        redisTemplate.opsForStream().pending(
                                keyFactory.streamKey(shard),
                                properties.getConsumerGroup()
                        );

                if (summary != null) {
                    total += summary.getTotalPendingMessages();
                }
            } catch (RuntimeException exception) {
                log.debug("Could not query Redis stream pending count for shard {}", shard, exception);
            }
        }

        return total;
    }

    /** 统计所有分片中 Stream 的物理消息总长度 */
    private long streamEntryCount() {
        long total = 0L;

        for (int shard = 0; shard < keyFactory.getShardCount(); shard++) {
            try {
                Long size = redisTemplate.opsForStream().size(keyFactory.streamKey(shard));

                if (size != null) {
                    total += size;
                }
            } catch (RuntimeException exception) {
                log.debug("Could not query Redis stream length for shard {}", shard, exception);
            }
        }

        return total;
    }

    /** 校验配置参数是否合法 */
    private void validateProperties() {
        if (properties.getShardCount() < 1 || properties.getShardCount() > 256) {
            throw new IllegalStateException("community.like.redis.shard-count 必须在 1-256 之间");
        }
        if (properties.getBatchSize() < 1 || properties.getBatchSize() > 1000) {
            throw new IllegalStateException("community.like.redis.batch-size 必须在 1-1000 之间");
        }
        if (properties.getConsumerGroup() == null || properties.getConsumerGroup().trim().isEmpty()) {
            throw new IllegalStateException("community.like.redis.consumer-group 不能为空");
        }
    }

    /**
     * 优雅停机：
     * 关闭线程池，最多等待 5 秒，超时强制中断。
     */
    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        ExecutorService pool = workerPool;
        if (pool != null) {
            pool.shutdownNow();
            try {
                if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("Timed out waiting for Redis like consumers to stop");
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override public boolean isRunning() { return running.get(); }
    @Override public boolean isAutoStartup() { return true; }

    /** 设置最高的销毁/停止优先级，确保在应用关闭时最后停止消费者，尽量处理完剩余数据 */
    @Override public int getPhase() { return Integer.MAX_VALUE - 100; }
}