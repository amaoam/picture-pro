package com.zmt.picturebackendpro.service.redis;

import com.zmt.picturebackendpro.config.LikeRedisProperties;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.zip.CRC32;

/**
 * 统一生成 V3 版本的所有 Redis key 的工厂类。
 * 核心职责：利用相同的 Redis Hash Tag（如 `{like-XX}`），确保在同一业务逻辑（如 Lua 脚本）
 * 中涉及的所有 Key 能够准确无误地落在同一个 Redis Cluster Slot 中，避免跨 Slot 操作报错。
 */
@Component
public class LikeRedisKeyFactory {

    /** 点赞 Redis 相关配置属性（包含键前缀、分片数等） */
    private final LikeRedisProperties properties;

    public LikeRedisKeyFactory(LikeRedisProperties properties) {
        this.properties = properties;
    }

    /**
     * 根据目标类型和目标 ID 计算分片索引（Shard）。
     * 同一 target 始终映射到固定分片，保证该目标的状态事件在一个 Stream 中保持严格顺序。
     *
     * @param targetType 目标类型（如 POST、PICTURE）
     * @param targetId   目标对象 ID
     * @return 分片索引
     */
    public int shardFor(String targetType, Long targetId) {
        // 拼接成标准来源字符串，利用 CRC32 进行散列计算
        String source = normalizeType(targetType) + ":" + targetId;
        CRC32 crc32 = new CRC32();
        crc32.update(source.getBytes(StandardCharsets.UTF_8));
        return (int) (crc32.getValue() % getShardCount());
    }

    /**
     * 获取指定目标的基础 Key 前缀。
     *
     * @param targetType 目标类型
     * @param targetId   目标 ID
     * @return 目标基础 Key 路径
     */
    public String targetBase(String targetType, Long targetId) {
        int shard = shardFor(targetType, targetId);
        return shardPrefix(shard) + ":target:" + normalizeType(targetType) + ":" + targetId;
    }

    /**
     * 获取记录已点赞用户的 Redis Set 键。
     *
     * @param targetType 目标类型
     * @param targetId   目标 ID
     * @return 用户集合 Key
     */
    public String likedUsersKey(String targetType, Long targetId) {
        return targetBase(targetType, targetId) + ":users";
    }

    /**
     * 获取记录目标点赞总数的 Redis 键。
     *
     * @param targetType 目标类型
     * @param targetId   目标 ID
     * @return 点赞计数 Key
     */
    public String likeCountKey(String targetType, Long targetId) {
        return targetBase(targetType, targetId) + ":count";
    }

    /**
     * 获取记录版本号对齐的 Redis 键（用于防并发冲突或状态对齐）。
     *
     * @param targetType 目标类型
     * @param targetId   目标 ID
     * @return 版本键
     */
    public String pairVersionsKey(String targetType, Long targetId) {
        return targetBase(targetType, targetId) + ":versions";
    }

    /**
     * 获取指定分片的 Redis Stream 键。
     *
     * @param shard 分片索引
     * @return Stream 键
     */
    public String streamKey(int shard) {
        return shardPrefix(shard) + ":events";
    }

    /**
     * 生成请求幂等校验的 Redis 键。
     * 注意：requestId 作为 key 的一部分会先做 SHA-256 摘要，防止任意恶意/奇怪的 Header 导致 Key 膨胀或格式问题；
     * 同时按目标分片构建，保证它与 Lua 脚本里使用的其它 Keys 严格落在同一个 Redis Cluster Slot 中。
     *
     * @param targetType 目标类型
     * @param targetId   目标 ID
     * @param requestId  客户端请求唯一标识
     * @return 幂等控制 Key
     */
    public String idempotencyKey(String targetType, Long targetId, String requestId) {
        return shardPrefix(shardFor(targetType, targetId)) + ":idem:"
                + normalizeType(targetType) + ":" + targetId + ":" + sha256(requestId);
    }

    /**
     * 生成包含 Redis Cluster Hash Tag 的分片前缀。
     * 例如：`prefix:{like-01}`，大括号内的 `like-01` 决定了集群的 Slot 归属。
     *
     * @param shard 分片索引
     * @return 带 Hash Tag 的分片前缀
     */
    public String shardPrefix(int shard) {
        if (shard < 0 || shard >= getShardCount()) {
            throw new IllegalArgumentException("shard 超出配置范围: " + shard);
        }
        return properties.getKeyPrefix() + ":{like-" + String.format(Locale.ROOT, "%02d", shard) + "}";
    }

    /**
     * 获取配置的分片总数，并校验其合法性（必须在 1-256 之间）。
     *
     * @return 分片总数
     */
    public int getShardCount() {
        int count = properties.getShardCount();
        if (count < 1 || count > 256) {
            throw new IllegalStateException("community.like.redis.shard-count 必须在 1-256 之间");
        }
        return count;
    }

    /**
     * 对 targetType 进行校验与标准化处理（转为大写）。
     * 目前仅支持 POST（帖子）和 PICTURE（图片）。
     *
     * @param targetType 原始目标类型
     * @return 标准化后的目标类型
     */
    private String normalizeType(String targetType) {
        if (targetType == null) {
            throw new IllegalArgumentException("targetType 不能为空");
        }
        String normalized = targetType.trim().toUpperCase(Locale.ROOT);
        if (!"POST".equals(normalized) && !"PICTURE".equals(normalized)) {
            throw new IllegalArgumentException("不支持的 targetType: " + targetType);
        }
        return normalized;
    }

    /**
     * 对输入的字符串进行 SHA-256 摘要计算，并转换为 16 进制小写字符串。
     *
     * @param value 原始字符串（如 requestId）
     * @return SHA-256 哈希字符串
     */
    private String sha256(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            // 每个 byte 对应 2 个十六进制字符，故 StringBuilder 初始容量设为 bytes.length * 2
            StringBuilder builder = new StringBuilder(bytes.length * 2);
            for (byte item : bytes) {
                builder.append(String.format(Locale.ROOT, "%02x", item & 0xff));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("JDK 不支持 SHA-256", impossible);
        }
    }
}