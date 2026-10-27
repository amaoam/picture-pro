package com.zmt.picturebackendpro.service.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zmt.picturebackendpro.config.LikeRedisProperties;
import com.zmt.picturebackendpro.exception.BusinessException;
import com.zmt.picturebackendpro.exception.ErrorCode;
import lombok.extern.slf4j.Slf4j;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

/**
 * Redis Lua 命令门面（Facade）
 * 核心职责：所有点赞写入和状态查询操作必须经由该类收敛，严禁在业务层将点赞拆分为多次独立的 Redis 调用。
 * 借助 Redis 服务端执行 Lua 脚本的原子性，保障点赞状态、计数器、幂等防重、Stream 事件投递的一致性。
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "community.like.mode", havingValue = "redis-stream")
public class RedisLikeCommandStore {

    /** 专用的 StringRedisTemplate 实例，用于操作 Redis 集群或单机 */
    private final StringRedisTemplate redisTemplate;

    /** Jackson 对象映射器，用于将 Lua 脚本返回的 JSON 字符串反序列化为实体类 */
    private final ObjectMapper objectMapper;

    /** Redis Key 生成工厂，确保集群模式下所有相关 Key 落在同一个 Slot（Hash Tag） */
    private final LikeRedisKeyFactory keyFactory;

    /** 点赞模块的 Redis 配置属性（如幂等 TTL、键前缀等） */
    private final LikeRedisProperties properties;

    /** Micrometer 监控注册表，用于多维度上报 Redis 命令执行结果与异常计数 */
    private final MeterRegistry meterRegistry;

    /** 核心点赞/取消点赞的 Lua 脚本对象 */
    private final DefaultRedisScript<String> commandScript;

    /** 状态与计数器一致性校验的 Lua 脚本对象 */
    private final DefaultRedisScript<String> statusScript;

    /**
     * 构造函数：注入各项核心依赖，并自动加载类路径下的 Lua 脚本文件。
     *
     * @param redisTemplate 指定的 StringRedisTemplate Bean
     * @param objectMapper  Jackson 映射器
     * @param keyFactory    Key 统一工厂
     * @param properties    点赞配置属性
     * @param meterRegistry 监控指标注册表
     */
    public RedisLikeCommandStore(@Qualifier("likeStringRedisTemplate") StringRedisTemplate redisTemplate,
                                 ObjectMapper objectMapper,
                                 LikeRedisKeyFactory keyFactory,
                                 LikeRedisProperties properties,
                                 MeterRegistry meterRegistry) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.keyFactory = keyFactory;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        // 加载位于 classpath 下的 Lua 脚本
        this.commandScript = loadScript("redis/like-command.lua");
        this.statusScript = loadScript("redis/like-status.lua");
    }

    /**
     * 执行明确的 LIKE（点赞）或 UNLIKE（取消点赞）命令。
     * 架构设计：Java 端负责生成全局唯一的事件 ID（UUID）与请求上下文，
     * Redis Lua 脚本负责原子性校验、状态更新、计数维护、幂等防重以及 Stream 写入。
     *
     * @param targetType   目标类型（如 POST、PICTURE）
     * @param targetId     目标对象的 ID
     * @param userId       操作用户的 ID
     * @param ownerUserId  目标所有者 ID（用于通知等，可为 null）
     * @param desiredState 期望的目标状态（"LIKE" 或 "UNLIKE"）
     * @param requestId    客户端请求唯一标识（用于幂等过滤）
     * @return RedisLikeCommandResult 包含执行结果、变更状态及最新计数的封装对象
     */
    public RedisLikeCommandResult execute(String targetType, Long targetId, Long userId,
                                          Long ownerUserId, String desiredState, String requestId) {
        // 在 Java 端生成当前操作的全局事件 ID，供下游消费者（如 MySQL 账本）去重
        String eventId = UUID.randomUUID().toString();
        // 计算目标对象所属的 Stream 分片
        int shard = keyFactory.shardFor(targetType, targetId);
        String response;

        try {
            // 执行 Redis Lua 脚本：
            // KEYS 列表包括：1.用户点赞集合、2.点赞计数器、3.Stream分片、4.幂等键、5.版本控制Hash
            // ARGV 列表包括：用户ID、期望状态、事件ID、请求ID、目标类型、目标ID、所有者ID、时间戳、幂等TTL
            response = redisTemplate.execute(commandScript,
                    Arrays.asList(
                            keyFactory.likedUsersKey(targetType, targetId),
                            keyFactory.likeCountKey(targetType, targetId),
                            keyFactory.streamKey(shard),
                            keyFactory.idempotencyKey(targetType, targetId, requestId),
                            keyFactory.pairVersionsKey(targetType, targetId)),
                    String.valueOf(userId), desiredState, eventId, requestId,
                    targetType, String.valueOf(targetId), ownerUserId == null ? "" : String.valueOf(ownerUserId),
                    String.valueOf(System.currentTimeMillis()), String.valueOf(properties.getIdempotencyTtlSeconds()));
        } catch (RuntimeException exception) {
            // 捕获 Redis 底层通信异常（如连接超时、网络中断），记录 Redis 错误指标并向上抛出
            Counter.builder("community.like.redis.commands")
                    .tag("outcome", "redis_error")
                    .register(meterRegistry).increment();
            throw exception;
        }

        // 解析 Lua 返回的 JSON 响应结果
        RedisLikeCommandResult result = parseResult(response);

        // 若 Lua 内部返回 outcome == 'error'，说明触发了业务校验异常或状态不一致
        if ("error".equals(result.getOutcome())) {
            // 严禁 Redis 写入失败后静默降级到 MySQL，以防双写状态分叉
            String message = "REQUEST_ID_REUSED".equals(result.getError())
                    ? "X-Request-Id 已用于另一条点赞命令，请为新操作生成新的请求 ID"
                    : "Redis 点赞状态异常：" + result.getError();

            Counter.builder("community.like.redis.commands")
                    .tag("outcome", "business_error")
                    .register(meterRegistry).increment();
            throw new BusinessException(ErrorCode.OPERATION_ERROR, message);
        }

        // 记录正常的业务执行结果埋点（如 changed、noop、duplicate 等）
        Counter.builder("community.like.redis.commands")
                .tag("outcome", result.getOutcome() == null ? "unknown" : result.getOutcome())
                .register(meterRegistry).increment();
        return result;
    }

    /**
     * 原子读取当前用户的点赞状态与目标的实时计数。
     * 若检测到 Redis Set 集合基数与计数器不一致（数据损坏），则触发 Fail-Fast 失败关闭。
     *
     * @param targetType 目标类型
     * @param targetId   目标 ID
     * @param userId     用户 ID
     * @return RedisLikeCommandResult 状态查询结果
     */
    public RedisLikeCommandResult status(String targetType, Long targetId, Long userId) {
        String response = redisTemplate.execute(statusScript,
                Arrays.asList(keyFactory.likedUsersKey(targetType, targetId),
                        keyFactory.likeCountKey(targetType, targetId)),
                String.valueOf(userId));

        RedisLikeCommandResult result = parseResult(response);

        // 检查状态查询中途是否发生不变量破坏（如计数器与用户集合不匹配）
        if ("error".equals(result.getOutcome())) {
            Counter.builder("community.like.redis.commands")
                    .tag("outcome", "status_invariant_error")
                    .register(meterRegistry).increment();
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "Redis 点赞状态异常：" + result.getError());
        }
        return result;
    }

    /**
     * 从类路径（Classpath）加载 Lua 脚本文件，并指定返回类型为 String。
     *
     * @param classpathLocation 脚本在 resources 下的相对路径
     * @return 封装后的 DefaultRedisScript 对象
     */
    private DefaultRedisScript<String> loadScript(String classpathLocation) {
        DefaultRedisScript<String> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource(classpathLocation)));
        script.setResultType(String.class);
        return script;
    }

    /**
     * 将 Lua 脚本返回的 JSON 响应字符串反序列化为 `RedisLikeCommandResult` 对象。
     *
     * @param response Lua 执行返回的原始字符串
     * @return 解析后的结果实体
     */
    private RedisLikeCommandResult parseResult(String response) {
        if (response == null || response.trim().isEmpty()) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "Redis 点赞服务未返回结果");
        }
        try {
            return objectMapper.readValue(response, RedisLikeCommandResult.class);
        } catch (IOException exception) {
            log.error("Unable to parse Redis like Lua response: {}", response, exception);
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "解析 Redis 点赞结果失败");
        }
    }
}