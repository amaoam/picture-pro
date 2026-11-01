package com.zmt.picturebackendpro.service.impl.mq;

import com.zmt.picturebackendpro.config.LikeRedisProperties;
import com.zmt.picturebackendpro.config.RabbitMqLikeProperties;
import com.zmt.picturebackendpro.service.redis.LikeRedisKeyFactory;
import com.zmt.picturebackendpro.service.redis.RedisLikeEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Redis Stream → RabbitMQ 转发器 (Relay)。
 * <p>
 * 核心设计思想：
 * 1. 业务侧（点赞）通过 Lua 脚本将状态修改和事件写入 Redis Stream，保证原子性。
 * 2. 本类作为后台守护线程，不断拉取 Stream 中的事件。
 * 3. 将事件投递到 RabbitMQ，并等待 Broker 的发布确认 (Publisher Confirms)。
 * 4. 只有 RabbitMQ 确认收到后，才通过 Lua 脚本原子地执行 XACK + XDEL，清理 Redis Stream 中的记录。
 * <p>
 * 容错机制：
 * - 若 RabbitMQ 投递失败或网络超时，Stream 记录不会被删除，后续会重试（至少一次投递）。
 * - 下游 RabbitMQ 消费者需通过 eventId 实现幂等去重。
 * - 第一版采用单实例、固定 consumer 名称，以便重启后能通过 Pending Entries List (PEL) 读取到自己未处理完的记录。
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqLikeStreamRelay implements org.springframework.context.SmartLifecycle {
    @Resource(name = "likeStringRedisTemplate") private StringRedisTemplate redisTemplate;
    @Resource private LikeRedisProperties redisProperties;
    @Resource private RabbitMqLikeProperties mqProperties;
    @Resource private LikeRedisKeyFactory keyFactory;
    @Resource private RabbitMqLikePublisher publisher;
    @Resource private MeterRegistry meterRegistry;

    // 控制后台转发线程生命周期的开关
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ExecutorService executor;

    // 监控指标：成功转发的消息数、转发失败次数
    private Counter publishedMessages;
    private Counter relayFailures;
    private volatile long lastPublishedAtMs;

    /**
     * Lua 脚本：创建消费者组。
     * 使用 MKSTREAM 选项，如果 Stream 不存在则自动创建。
     * 从 '0-0' 开始消费（即从最早的消息开始）。
     */
    private final DefaultRedisScript<String> createGroupScript = new DefaultRedisScript<String>() {{
        setScriptText("redis.call('XGROUP', 'CREATE', KEYS[1], ARGV[1], '0-0', 'MKSTREAM'); return 'OK'");
        setResultType(String.class);
    }};

    /**
     * Lua 脚本：对每条记录执行 XACK 与 XDEL 的组合脚本。
     * 避免两次命令之间崩溃留下不可恢复的中间状态。
     * 逻辑：只有被该消费组成功 ACK 的记录才会被 XDEL 删除；
     * 若意外出现 owner/状态不符，未 ACK 的记录会保留在 Stream 中等待重试。
     */
    private final DefaultRedisScript<Long> ackAndDeleteScript = new DefaultRedisScript<Long>() {{
        setScriptText("local acked = 0; " +
                "for i = 2, #ARGV do " +
                "  local n = redis.call('XACK', KEYS[1], ARGV[1], ARGV[i]); " +
                "  if n == 1 then redis.call('XDEL', KEYS[1], ARGV[i]); acked = acked + 1; end; " +
                "end; " +
                "return acked");
        setResultType(Long.class);
    }};

    @Override
    public void start() {
        // 防止重复启动
        if (!running.compareAndSet(false, true)) return;

        // 1. 参数校验
        if (mqProperties.getRelayBatchSize() < 1 || mqProperties.getRelayBatchSize() > 1000) {
            running.set(false);
            throw new IllegalStateException("community.like.rabbitmq.relay-batch-size 必须在 1-1000 之间");
        }

        // 2. 为所有分片 (shard) 初始化 Redis Stream 消费者组
        for (int shard = 0; shard < keyFactory.getShardCount(); shard++) {
            ensureGroup(keyFactory.streamKey(shard));
        }

        // 3. 注册 Micrometer 监控指标
        publishedMessages = Counter.builder("community.like.rabbitmq.relay.published")
                .description("Redis Stream events confirmed by RabbitMQ")
                .register(meterRegistry);
        relayFailures = Counter.builder("community.like.rabbitmq.relay.failures")
                .description("Redis-to-RabbitMQ relay batch failures")
                .register(meterRegistry);
        Gauge.builder("community.like.rabbitmq.relay.stream.entries", this, RabbitMqLikeStreamRelay::streamEntryCount)
                .description("Physical entries waiting in Redis relay streams")
                .register(meterRegistry);
        Gauge.builder("community.like.rabbitmq.relay.stream.pending", this, RabbitMqLikeStreamRelay::pendingMessageCount)
                .description("Redis relay stream pending messages")
                .register(meterRegistry);

        // 4. 创建单线程执行器并启动后台轮询任务
        // 使用单线程保证同一时间只有一个批次在处理，简化 ACK 逻辑，避免复杂并发
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "rabbitmq-like-stream-relay");
            thread.setDaemon(false); // 设为非守护线程，防止 JVM 提前退出导致消息丢失
            return thread;
        });
        executor.submit(this::runLoop);

        log.info("RabbitMQ like relay started: shards={}, group={}, batchSize={}",
                keyFactory.getShardCount(), mqProperties.getRelayGroup(), mqProperties.getRelayBatchSize());
    }

    /**
     * 核心轮询循环逻辑
     */
    private void runLoop() {
        while (running.get()) {
            boolean found = false; // 标记本次轮询是否读到了数据
            // 遍历所有分片
            for (int shard = 0; shard < keyFactory.getShardCount() && running.get(); shard++) {
                try {
                    // 第一步：优先读取当前消费者组下未被 ACK 的历史 Pending 消息（防止重启后消息丢失）
                    List<MapRecord<String, Object, Object>> records = readBatch(shard, ReadOffset.from("0-0"));

                    // 第二步：如果没有 Pending 消息，则读取最新的未消费消息 (>)
                    if (records == null || records.isEmpty()) {
                        records = readBatch(shard, ReadOffset.lastConsumed());
                    }

                    if (records == null || records.isEmpty()) continue;

                    found = true;
                    List<RedisLikeEvent> events = new ArrayList<>(records.size());
                    for (MapRecord<String, Object, Object> record : records) {
                        events.add(toEvent(record));
                    }

                    // 第三步：将事件发布到 RabbitMQ。
                    // 注意：这里 publisher.publishEvents 内部通常会利用 CorrelationData 等待 Broker 确认。
                    // 如果发生异常（Broker 拒收/网络问题），会抛出异常，跳过后面的 ACK 逻辑。
                    // 这保证了“至少一次投递” (At-Least-Once)，下游需通过 eventId 去重。
                    publisher.publishEvents(events);

                    // 第四步：RabbitMQ 发布成功后，构建 Lua 脚本参数
                    List<String> arguments = new ArrayList<>();
                    arguments.add(mqProperties.getRelayGroup());
                    for (MapRecord<String, Object, Object> record : records) {
                        arguments.add(record.getId().getValue());
                    }

                    // 第五步：执行 XACK + XDEL 脚本清理 Stream 记录
                    Long acknowledged = redisTemplate.execute(ackAndDeleteScript,
                            Collections.singletonList(keyFactory.streamKey(shard)), arguments.toArray());

                    // 校验清理数量是否与读取数量一致，不一致说明出现并发问题或数据异常
                    if (acknowledged == null || acknowledged != records.size()) {
                        throw new IllegalStateException("Stream ack/delete 数量不匹配，预期=" + records.size()
                                + ", 实际=" + acknowledged);
                    }

                    // 更新监控指标和心跳时间
                    publishedMessages.increment(records.size());
                    lastPublishedAtMs = System.currentTimeMillis();

                } catch (Exception e) {
                    // 任何异常（网络抖动、MQ 宕机等）都会进入这里。
                    // 此时不执行 ACK，消息保留在 Stream 中，下次循环会重新读取（即重试）。
                    relayFailures.increment();
                    log.error("Redis-to-RabbitMQ relay failed; event remains recoverable in Stream. shard={}", shard, e);
                    sleep(250L); // 发生异常时短暂休眠，避免疯狂重试打满 CPU
                }
            }
            // 如果所有分片都没有读到数据，说明当前空闲，根据配置进行长轮询休眠
            if (!found) sleep(Math.max(1L, mqProperties.getRelayPollDelayMs()));
        }
    }

    /**
     * 从 Redis Stream 中批量读取消息
     * @param shard 分片索引
     * @param offset 读取偏移量（0-0 代表从头读 Pending，> 代表读最新未分配）
     */
    private List<MapRecord<String, Object, Object>> readBatch(int shard, ReadOffset offset) {
        String stream = keyFactory.streamKey(shard);
        Consumer consumer = Consumer.from(mqProperties.getRelayGroup(), mqProperties.getRelayConsumer());
        StreamOperations<String, Object, Object> ops = redisTemplate.opsForStream();
        return ops.read(consumer,
                StreamReadOptions.empty().count(mqProperties.getRelayBatchSize()),
                StreamOffset.create(stream, offset));
    }

    /**
     * 将 Redis Stream 的 Record 映射转换为 Java 对象
     */
    private RedisLikeEvent toEvent(MapRecord<String, Object, Object> record) {
        Map<Object, Object> values = record.getValue();
        RedisLikeEvent e = new RedisLikeEvent();
        e.setEventId(required(values, "eventId"));
        e.setRequestId(required(values, "requestId"));
        e.setTargetType(required(values, "targetType"));
        e.setTargetId(required(values, "targetId"));
        e.setUserId(required(values, "userId"));
        e.setOwnerUserId(valueOrEmpty(values, "ownerUserId"));
        e.setDesiredState(required(values, "desiredState"));
        e.setDelta(required(values, "delta"));
        e.setPairVersion(required(values, "pairVersion"));
        e.setCreatedAtMs(required(values, "createdAtMs"));
        return e;
    }

    private String required(Map<Object, Object> values, String key) {
        Object value = values.get(key);
        if (value == null || value.toString().trim().isEmpty()) {
            throw new IllegalArgumentException("Redis Stream event missing field: " + key);
        }
        return value.toString();
    }

    private String valueOrEmpty(Map<Object, Object> values, String key) {
        Object value = values.get(key);
        return value == null ? "" : value.toString();
    }

    /**
     * 确保消费者组存在，若已存在则忽略 "BUSYGROUP" 异常
     */
    private void ensureGroup(String stream) {
        try {
            redisTemplate.execute(createGroupScript, Collections.singletonList(stream), mqProperties.getRelayGroup());
        } catch (DataAccessException e) {
            Throwable root = e.getMostSpecificCause();
            String message = root == null ? e.getMessage() : root.getMessage();
            // 捕获 BUSYGROUP 错误：表示组名已存在，属于正常情况，无需抛出
            if (message == null || !message.toUpperCase().contains("BUSYGROUP")) throw e;
        }
    }

    /**
     * 检查 Stream 是否已经排空（用于优雅停机时的探活）
     */
    public boolean isDrained() {
        try {
            for (int shard = 0; shard < keyFactory.getShardCount(); shard++) {
                // 如果还有物理消息
                Long size = redisTemplate.opsForStream().size(keyFactory.streamKey(shard));
                if (size != null && size > 0) return false;
                // 如果还有 Pending 消息
                org.springframework.data.redis.connection.stream.PendingMessagesSummary p =
                        redisTemplate.opsForStream().pending(keyFactory.streamKey(shard), mqProperties.getRelayGroup());
                if (p != null && p.getTotalPendingMessages() > 0) return false;
            }
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    public long lastPublishedAtMs() { return lastPublishedAtMs; }

    /**
     * 监控指标：统计所有分片的 Stream 物理消息总数
     */
    private long streamEntryCount() {
        long total = 0L;
        for (int shard = 0; shard < keyFactory.getShardCount(); shard++) {
            try {
                Long size = redisTemplate.opsForStream().size(keyFactory.streamKey(shard));
                if (size != null) total += size;
            } catch (RuntimeException e) { return -1L; }
        }
        return total;
    }

    /**
     * 监控指标：统计所有分片的 Pending (未 ACK) 消息总数
     */
    private long pendingMessageCount() {
        long total = 0L;
        for (int shard = 0; shard < keyFactory.getShardCount(); shard++) {
            try {
                org.springframework.data.redis.connection.stream.PendingMessagesSummary p =
                        redisTemplate.opsForStream().pending(keyFactory.streamKey(shard), mqProperties.getRelayGroup());
                if (p != null) total += p.getTotalPendingMessages();
            } catch (RuntimeException e) { return -1L; }
        }
        return total;
    }

    private void sleep(long milliseconds) {
        try { Thread.sleep(milliseconds); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); running.set(false); }
    }

    @Override public void stop() {
        if (!running.compareAndSet(true, false)) return;
        ExecutorService pool = executor;
        if (pool != null) {
            pool.shutdownNow();
            try { pool.awaitTermination(5, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }
    @Override public void stop(Runnable callback) { stop(); callback.run(); }
    @Override public boolean isRunning() { return running.get(); }
    @Override public boolean isAutoStartup() { return true; }
    // 确保该组件在 Spring 容器生命周期较晚时启动，较早时关闭，保证业务执行时它已经准备好
    @Override public int getPhase() { return Integer.MAX_VALUE - 100; }
}