package com.zmt.picturebackendpro.service.impl.mq;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zmt.picturebackendpro.config.RabbitMqLikeProperties;
import com.zmt.picturebackendpro.exception.BusinessException;
import com.zmt.picturebackendpro.exception.ErrorCode;
import com.zmt.picturebackendpro.mapper.CommunityPostMapper;
import com.zmt.picturebackendpro.mapper.PictureMapper;
import com.zmt.picturebackendpro.model.entity.CommunityPost;
import com.zmt.picturebackendpro.model.entity.User;
import com.zmt.picturebackendpro.model.entity.Picture;
import com.zmt.picturebackendpro.model.vo.community.CommunityLikeStatusVO;
import com.zmt.picturebackendpro.service.CommunityLikeService;
import com.zmt.picturebackendpro.service.redis.LikeRedisKeyFactory;
import com.zmt.picturebackendpro.service.redis.RedisLikeCommandResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.time.Duration;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * RabbitMQ 点赞版本的 API 服务。
 * <p>
 * 设计要点：
 * 1. 与旧 Redis Stream 版本通过 {@code community.like.mode=rabbitmq} 条件装配隔离，二者互斥；
 * 2. Redis 负责高频的点赞状态变更（计数、用户点赞集合），保证接口低延迟；
 * 3. Caffeine 短缓存用于减少每次点赞都查询 MySQL 获取目标元数据（作者、发布状态等）的开销；
 * 4. 真正的 MQ 消息投递（Stream relay）与数据库最终落库由独立组件异步完成，本类只负责"写 Redis + 返回状态"；
 * 5. 所有写操作都要求幂等 requestId，用于 Redis 命令去重。
 * <p>
 * 使用 {@link Primary} 确保在 rabbitmq 模式下优先注入本实现。
 */
@Slf4j
@Service
@Primary
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqCommunityLikeServiceImpl implements CommunityLikeService {

    /** 帖子状态：1 = 已发布（与数据库字段约定一致） */
    private static final int POST_PUBLISHED = 1;
    /** 图片审核状态：1 = 审核通过 */
    private static final int REVIEW_PASSED = 1;

    /**
     * 合法的 requestId 正则：允许字母、数字、点、下划线、冒号、连字符，长度 1-128。
     * 用于防止外部传入的 X-Request-Id 携带非法字符影响 Redis key 或日志。
     */
    private static final Pattern REQUEST_ID_PATTERN = Pattern.compile("^[A-Za-z0-9._:-]{1,128}$");

    /** 社区帖子 Mapper，用于校验帖子是否已发布 */
    @Resource private CommunityPostMapper communityPostMapper;
    /** 图片 Mapper，用于校验图片是否公共可见 */
    @Resource private PictureMapper pictureMapper;
    /** Redis key 工厂，负责按类型/ID 计算分片及生成规范化 key */
    @Resource private LikeRedisKeyFactory keyFactory;
    /** Redis 点赞命令存储：封装 Lua 脚本，实现点赞/取消点赞/状态查询的原子操作 */
    @Resource private RabbitMqRedisLikeCommandStore commandStore;
    /** 点赞活动包装器：统一处理请求上下文（如日志 traceId、限流等） */
    @Resource private RabbitMqLikeActivity activity;
    /** 专门用于点赞模块的 Redis 模板（按 bean 名注入，避免与其他 StringRedisTemplate 冲突） */
    @Resource(name = "likeStringRedisTemplate") private StringRedisTemplate redisTemplate;

    /**
     * 帖子元数据缓存：postId -> (id, ownerUserId)。
     * 减少高频点赞时反复查 MySQL 的开销；TTL 与最大容量由配置控制。
     */
    private final Cache<Long, PostMetadata> postCache;

    /**
     * 图片元数据缓存：pictureId -> (id, ownerUserId)。
     * 图片的可见性/审核状态变化较慢，适合短缓存。
     */
    private final Cache<Long, PictureMetadata> pictureCache;

    /**
     * 已登记到 Redis 的"对账目标"集合缓存，key 形如 "POST:123"。
     * 用于避免每次请求都执行一次 Redis SADD（热目标只登记一次）。
     * 使用 expireAfterAccess=1h，长时间未访问的目标会被淘汰，下次访问重新登记。
     */
    private final Cache<String, Boolean> indexedTargets;

    /**
     * 构造器注入配置，初始化三个 Caffeine 缓存。
     * TTL 和最大容量做了兜底下限（500ms / 1000），防止配置误填导致缓存失效或内存膨胀。
     */
    public RabbitMqCommunityLikeServiceImpl(RabbitMqLikeProperties properties) {
        long ttlMs = Math.max(500L, properties.getMetadataCacheTtlMs());
        long maximumSize = Math.max(1000L, properties.getMetadataCacheMaxSize());
        this.postCache = Caffeine.newBuilder().maximumSize(maximumSize)
                .expireAfterWrite(Duration.ofMillis(ttlMs)).recordStats().build();
        this.pictureCache = Caffeine.newBuilder().maximumSize(maximumSize)
                .expireAfterWrite(Duration.ofMillis(ttlMs)).recordStats().build();
        this.indexedTargets = Caffeine.newBuilder().maximumSize(maximumSize)
                .expireAfterAccess(Duration.ofHours(1)).build();
    }

    // ============================ 帖子点赞 API ============================

    /**
     * 帖子点赞（不传 requestId，自动生成 UUID）。
     * 适合内部调用或简单场景；HTTP 接口应优先使用带 requestId 的重载以实现幂等。
     */
    @Override
    public CommunityLikeStatusVO likePost(Long postId, User user) {
        return likePost(postId, user, UUID.randomUUID().toString());
    }

    /**
     * 帖子点赞（带 requestId，支持幂等）。
     * 流程：
     * 1. 在 activity 上下文中执行（统一请求包装）；
     * 2. 校验登录；
     * 3. 校验帖子已发布（走 Caffeine 缓存）；
     * 4. 禁止给自己点赞；
     * 5. 登记对账目标到 Redis Set（供后续对账任务扫描）；
     * 6. 执行 Redis 点赞命令并返回最新状态。
     */
    @Override
    public CommunityLikeStatusVO likePost(Long postId, User user, String requestId) {
        return activity.withRequest(() -> {
            requireLogin(user);
            PostMetadata post = requirePublishedPost(postId);
            if (user.getId().equals(post.ownerUserId)) {
                throw new BusinessException(ErrorCode.OPERATION_ERROR, "不能给自己的帖子点赞");
            }
            indexTarget("POST", post.id);
            return execute("POST", post.id, user.getId(), post.ownerUserId, "LIKE", normalizeRequestId(requestId));
        });
    }

    /** 帖子取消点赞（自动生成 requestId）。 */
    @Override
    public CommunityLikeStatusVO unlikePost(Long postId, User user) {
        return unlikePost(postId, user, UUID.randomUUID().toString());
    }

    /**
     * 帖子取消点赞（带 requestId）。
     * 注意：取消点赞不校验"是否本人帖子"，因为用户不可能给自己点赞成功，取消也无意义；
     * 但依然需要校验帖子存在，以便拿到 ownerUserId 做 Redis 命令。
     */
    @Override
    public CommunityLikeStatusVO unlikePost(Long postId, User user, String requestId) {
        return activity.withRequest(() -> {
            requireLogin(user);
            PostMetadata post = requirePublishedPost(postId);
            indexTarget("POST", post.id);
            return execute("POST", post.id, user.getId(), post.ownerUserId, "UNLIKE", normalizeRequestId(requestId));
        });
    }

    /**
     * 查询当前用户对帖子的点赞状态。
     * 只读操作，不写 Redis 命令，但仍需登记对账目标（保证该目标被后续对账任务覆盖）。
     */
    @Override
    public CommunityLikeStatusVO getPostLikeStatus(Long postId, User user) {
        return activity.withRequest(() -> {
            requireLogin(user);
            PostMetadata post = requirePublishedPost(postId);
            indexTarget("POST", post.id);
            return readStatus("POST", post.id, user.getId());
        });
    }

    // ============================ 公共图片点赞 API ============================

    /** 公共图片点赞（自动生成 requestId）。 */
    @Override
    public CommunityLikeStatusVO likePublicPicture(Long pictureId, User user) {
        return likePublicPicture(pictureId, user, UUID.randomUUID().toString());
    }

    /**
     * 公共图片点赞（带 requestId）。
     * 与帖子点赞类似，但额外规则：
     * 1. 图片必须是公共可见（spaceId 为空、审核通过、未删除）；
     * 2. 图片 ownerUserId 可能为 null（历史数据），此时跳过"不能给自己点赞"校验。
     */
    @Override
    public CommunityLikeStatusVO likePublicPicture(Long pictureId, User user, String requestId) {
        return activity.withRequest(() -> {
            requireLogin(user);
            PictureMetadata picture = requirePublicPicture(pictureId);
            if (picture.ownerUserId != null && picture.ownerUserId.equals(user.getId())) {
                throw new BusinessException(ErrorCode.OPERATION_ERROR, "不能给自己的图片点赞");
            }
            indexTarget("PICTURE", picture.id);
            return execute("PICTURE", picture.id, user.getId(), picture.ownerUserId,
                    "LIKE", normalizeRequestId(requestId));
        });
    }

    /** 公共图片取消点赞（自动生成 requestId）。 */
    @Override
    public CommunityLikeStatusVO unlikePublicPicture(Long pictureId, User user) {
        return unlikePublicPicture(pictureId, user, UUID.randomUUID().toString());
    }

    /** 公共图片取消点赞（带 requestId）。 */
    @Override
    public CommunityLikeStatusVO unlikePublicPicture(Long pictureId, User user, String requestId) {
        return activity.withRequest(() -> {
            requireLogin(user);
            PictureMetadata picture = requirePublicPicture(pictureId);
            indexTarget("PICTURE", picture.id);
            return execute("PICTURE", picture.id, user.getId(), picture.ownerUserId,
                    "UNLIKE", normalizeRequestId(requestId));
        });
    }

    /** 查询当前用户对公共图片的点赞状态。 */
    @Override
    public CommunityLikeStatusVO getPublicPictureLikeStatus(Long pictureId, User user) {
        return activity.withRequest(() -> {
            requireLogin(user);
            PictureMetadata picture = requirePublicPicture(pictureId);
            indexTarget("PICTURE", picture.id);
            return readStatus("PICTURE", picture.id, user.getId());
        });
    }

    // ============================ 元数据校验（带缓存） ============================

    /**
     * 校验并获取已发布帖子的元数据。
     * <p>
     * 缓存策略：Caffeine 的 {@code get(key, mappingFunction)} 是原子操作，缓存未命中时才查 MySQL。
     * 注意：若帖子被删除/下线，缓存最多 TTL 时间内仍可能返回旧数据，这是短缓存可接受的权衡；
     * 对点赞这种最终一致场景，短暂误判可通过后续对账修正。
     *
     * @throws BusinessException 参数非法或帖子不存在/未公开
     */
    private PostMetadata requirePublishedPost(Long postId) {
        if (postId == null || postId <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "帖子 ID 不合法");
        }
        return postCache.get(postId, id -> {
            // 只查"已发布 + 未删除"的帖子，避免对草稿/删除帖子点赞
            CommunityPost post = communityPostMapper.selectOne(new LambdaQueryWrapper<CommunityPost>()
                    .eq(CommunityPost::getId, id)
                    .eq(CommunityPost::getStatus, POST_PUBLISHED)
                    .eq(CommunityPost::getIsDelete, 0));
            if (post == null) {
                throw new BusinessException(ErrorCode.NOT_FOUND_ERROR, "帖子不存在或尚未公开");
            }
            return new PostMetadata(post.getId(), post.getUserId());
        });
    }

    /**
     * 校验并获取公共图片的元数据。
     * <p>
     * 公共可见条件：
     * - 图片存在；
     * - spaceId 为空（不属于私有空间）；
     * - 审核状态为通过；
     * - 未被删除。
     *
     * @throws BusinessException 参数非法或图片不可见
     */
    private PictureMetadata requirePublicPicture(Long pictureId) {
        if (pictureId == null || pictureId <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "图片 ID 不合法");
        }
        return pictureCache.get(pictureId, id -> {
            Picture picture = pictureMapper.selectById(id);
            if (picture == null || picture.getSpaceId() != null
                    || picture.getReviewStatus() == null || picture.getReviewStatus() != REVIEW_PASSED
                    || picture.getIsDelete() == null || picture.getIsDelete() != 0) {
                throw new BusinessException(ErrorCode.NOT_FOUND_ERROR, "公共图片不存在或不可见");
            }
            return new PictureMetadata(picture.getId(), picture.getUserId());
        });
    }

    // ============================ Redis 命令执行 ============================

    /**
     * 执行点赞/取消点赞命令。
     * <p>
     * 异常处理策略：
     * - {@link BusinessException} 属于业务预期内异常（如参数错、幂等冲突），原样抛出；
     * - 其他 {@link RuntimeException}（如 Redis 连接失败）统一包装为"服务暂时不可用"，并记录详细日志，
     *   避免把底层异常细节暴露给前端。
     *
     * @param type       目标类型（POST / PICTURE）
     * @param targetId   目标 ID
     * @param userId     操作者用户 ID
     * @param ownerUserId 目标作者 ID（用于 Redis 中记录"作者收到的点赞数"等）
     * @param state      LIKE / UNLIKE
     * @param requestId  幂等请求 ID
     */
    private CommunityLikeStatusVO execute(String type, Long targetId, Long userId, Long ownerUserId,
                                          String state, String requestId) {
        try {
            RedisLikeCommandResult result = commandStore.execute(type, targetId, userId, ownerUserId, state, requestId);
            return toVo(targetId, result);
        } catch (BusinessException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            log.error("RabbitMQ mode Redis like command failed: type={}, targetId={}, userId={}, state={}, requestId={}",
                    type, targetId, userId, state, requestId, exception);
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "点赞服务暂时不可用，请稍后重试");
        }
    }

    /**
     * 查询点赞状态（只读 Redis，不写命令）。
     * 异常处理策略同 {@link #execute}。
     */
    private CommunityLikeStatusVO readStatus(String type, Long targetId, Long userId) {
        try {
            return toVo(targetId, commandStore.status(type, targetId, userId));
        } catch (BusinessException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            log.error("RabbitMQ mode Redis like status failed: type={}, targetId={}, userId={}",
                    type, targetId, userId, exception);
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "点赞状态暂时不可用，请稍后重试");
        }
    }

    /**
     * 将 Redis 命令结果转换为前端 VO。
     * 对 null 值做兜底：likeCount 默认 0，liked 默认 false。
     */
    private CommunityLikeStatusVO toVo(Long targetId, RedisLikeCommandResult result) {
        CommunityLikeStatusVO vo = new CommunityLikeStatusVO();
        vo.setTargetId(targetId);
        vo.setLikeCount(result.getLikeCount() == null ? 0L : result.getLikeCount());
        vo.setLikedByCurrentUser(Boolean.TRUE.equals(result.getLiked()));
        return vo;
    }

    // ============================ 对账目标登记 ============================

    /**
     * 将目标登记到 Redis Set，供后续对账任务（reconciliation）扫描。
     * <p>
     * 优化点：使用本地 Caffeine {@code indexedTargets} 记录"当前实例已登记过的目标"，
     * 热目标后续请求不再额外执行一次 Redis SADD，显著降低 Redis 写压力。
     * <p>
     * key 结构：{shardPrefix}:targets，value 为 "POST:123" / "PICTURE:456"。
     * 分片由 {@link LikeRedisKeyFactory#shardFor} 计算，避免单个 Set 过大。
     * <p>
     * 若 SADD 失败（Redis 不可用），直接抛业务异常，因为对账目标缺失会导致数据最终不一致。
     */
    private void indexTarget(String targetType, Long targetId) {
        String token = targetType + ":" + targetId;
        // 目标第一次在当前实例中被访问时登记；热目标后续请求不再额外执行一次 Redis SADD。
        if (indexedTargets.getIfPresent(token) != null) return;
        int shard = keyFactory.shardFor(targetType, targetId);
        String key = keyFactory.shardPrefix(shard) + ":targets";
        try {
            redisTemplate.opsForSet().add(key, token);
            indexedTargets.put(token, Boolean.TRUE);
        } catch (RuntimeException exception) {
            log.error("Could not register like reconciliation target: type={}, targetId={}",
                    targetType, targetId, exception);
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "点赞服务暂时不可用，请稍后重试");
        }
    }

    // ============================ 工具方法 ============================

    /**
     * 规范化 requestId：
     * - 空白则自动生成 UUID；
     * - trim 后校验正则，非法字符直接拒绝，避免污染 Redis key / 日志。
     */
    private String normalizeRequestId(String requestId) {
        String resolved = StringUtils.hasText(requestId) ? requestId.trim() : UUID.randomUUID().toString();
        if (!REQUEST_ID_PATTERN.matcher(resolved).matches()) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR,
                    "X-Request-Id 只能包含字母、数字、点、下划线、冒号或连字符，长度 1-128");
        }
        return resolved;
    }

    /** 登录校验：user 为空或 id 为空即视为未登录。 */
    private void requireLogin(User user) {
        if (user == null || user.getId() == null) {
            throw new BusinessException(ErrorCode.NO_AUTH_ERROR);
        }
    }

    // ============================ 内部值对象 ============================

    /**
     * 帖子元数据（缓存值）。
     * 只保留点赞逻辑需要的字段，避免缓存整个实体造成内存浪费。
     */
    private static final class PostMetadata {
        private final Long id;
        private final Long ownerUserId;
        private PostMetadata(Long id, Long ownerUserId) { this.id = id; this.ownerUserId = ownerUserId; }
    }

    /**
     * 图片元数据（缓存值）。
     * ownerUserId 可能为 null（历史数据），调用方需做空判断。
     */
    private static final class PictureMetadata {
        private final Long id;
        private final Long ownerUserId;
        private PictureMetadata(Long id, Long ownerUserId) { this.id = id; this.ownerUserId = ownerUserId; }
    }
}