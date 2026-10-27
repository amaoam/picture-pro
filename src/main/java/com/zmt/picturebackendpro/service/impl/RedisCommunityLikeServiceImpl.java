package com.zmt.picturebackendpro.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.zmt.picturebackendpro.exception.BusinessException;
import com.zmt.picturebackendpro.exception.ErrorCode;
import com.zmt.picturebackendpro.mapper.CommunityPostMapper;
import com.zmt.picturebackendpro.mapper.PictureMapper;
import com.zmt.picturebackendpro.model.entity.CommunityPost;
import com.zmt.picturebackendpro.model.entity.Picture;
import com.zmt.picturebackendpro.model.entity.User;
import com.zmt.picturebackendpro.model.vo.community.CommunityLikeStatusVO;
import com.zmt.picturebackendpro.service.CommunityLikeService;
import com.zmt.picturebackendpro.service.redis.RedisLikeCommandResult;
import com.zmt.picturebackendpro.service.redis.RedisLikeCommandStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * V3 版本社区点赞服务实现类。
 * <p>
 * 设计架构：
 * 1. 实时点赞关系与计数完全由 Redis 承载（利用 Redis 的高性能和原子命令/Lua脚本）。
 * 2. MySQL 的持久化解耦，由 Redis Stream 消费者异步完成。
 * 3. 仅通过 MySQL 读取目标元数据（如帖子、图片）以进行公开状态校验及自赞防刷校验，切勿在此处调用旧的数据库点赞写方法。
 * </p>
 *
 * @author zmt
 */
@Slf4j
@Service
@Primary
@ConditionalOnProperty(name = "community.like.mode", havingValue = "redis-stream")
public class RedisCommunityLikeServiceImpl implements CommunityLikeService {

    /** 帖子已发布状态常量 */
    private static final int POST_PUBLISHED = 1;
    /** 图片审核通过状态常量 */
    private static final int REVIEW_PASSED = 1;
    /** 请求 ID (requestId) 正则校验模式：仅允许字母、数字、点、下划线、冒号或连字符，长度 1-128 */
    private static final Pattern REQUEST_ID_PATTERN = Pattern.compile("^[A-Za-z0-9._:-]{1,128}$");

    @Resource
    private CommunityPostMapper communityPostMapper;
    @Resource
    private PictureMapper pictureMapper;
    @Resource
    private RedisLikeCommandStore commandStore;

    /**
     * 点赞社区帖子（自动生成唯一请求 ID）
     *
     * @param postId    帖子 ID
     * @param loginUser 当前登录用户
     * @return 点赞状态视图对象（包含最新点赞数及当前用户是否点赞）
     */
    @Override
    public CommunityLikeStatusVO likePost(Long postId, User loginUser) {
        return likePost(postId, loginUser, UUID.randomUUID().toString());
    }

    /**
     * 点赞社区帖子（支持指定幂等请求 ID）
     *
     * @param postId    帖子 ID
     * @param loginUser 当前登录用户
     * @param requestId 请求追踪/幂等 ID
     * @return 点赞状态视图对象
     */
    @Override
    public CommunityLikeStatusVO likePost(Long postId, User loginUser, String requestId) {
        // 1. 校验用户登录状态
        requireLogin(loginUser);
        // 2. 校验帖子是否存在且处于公开/已发布状态
        CommunityPost post = requirePublishedPost(postId);
        // 3. 校验业务规则：不能给自己的帖子点赞
        if (loginUser.getId().equals(post.getUserId())) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "不能给自己的帖子点赞");
        }
        // 4. 执行 Redis 点赞命令
        return execute("POST", post.getId(), loginUser.getId(), post.getUserId(), "LIKE", normalizeRequestId(requestId));
    }

    /**
     * 取消点赞社区帖子（自动生成唯一请求 ID）
     *
     * @param postId    帖子 ID
     * @param loginUser 当前登录用户
     * @return 点赞状态视图对象
     */
    @Override
    public CommunityLikeStatusVO unlikePost(Long postId, User loginUser) {
        return unlikePost(postId, loginUser, UUID.randomUUID().toString());
    }

    /**
     * 取消点赞社区帖子（支持指定幂等请求 ID）
     *
     * @param postId    帖子 ID
     * @param loginUser 当前登录用户
     * @param requestId 请求追踪/幂等 ID
     * @return 点赞状态视图对象
     */
    @Override
    public CommunityLikeStatusVO unlikePost(Long postId, User loginUser, String requestId) {
        // 1. 校验用户登录状态
        requireLogin(loginUser);
        // 2. 校验帖子是否存在且已发布
        CommunityPost post = requirePublishedPost(postId);
        // 3. 执行 Redis 取消点赞命令
        return execute("POST", post.getId(), loginUser.getId(), post.getUserId(), "UNLIKE", normalizeRequestId(requestId));
    }

    /**
     * 获取当前用户对指定帖子的点赞状态
     *
     * @param postId    帖子 ID
     * @param loginUser 当前登录用户
     * @return 点赞状态视图对象
     */
    @Override
    public CommunityLikeStatusVO getPostLikeStatus(Long postId, User loginUser) {
        // 1. 校验登录
        requireLogin(loginUser);
        // 2. 校验帖子是否存在且已发布
        CommunityPost post = requirePublishedPost(postId);
        // 3. 读取点赞状态
        return readStatus("POST", post.getId(), loginUser.getId());
    }

    /**
     * 点赞公共图片（自动生成唯一请求 ID）
     *
     * @param pictureId 图片 ID
     * @param loginUser 当前登录用户
     * @return 点赞状态视图对象
     */
    @Override
    public CommunityLikeStatusVO likePublicPicture(Long pictureId, User loginUser) {
        return likePublicPicture(pictureId, loginUser, UUID.randomUUID().toString());
    }

    /**
     * 点赞公共图片（支持指定幂等请求 ID）
     *
     * @param pictureId 图片 ID
     * @param loginUser 当前登录用户
     * @param requestId 请求追踪/幂等 ID
     * @return 点赞状态视图对象
     */
    @Override
    public CommunityLikeStatusVO likePublicPicture(Long pictureId, User loginUser, String requestId) {
        // 1. 校验登录
        requireLogin(loginUser);
        // 2. 校验图片是否存在且为公开审核通过的公共图片
        Picture picture = requirePublicPicture(pictureId);
        // 3. 校验业务规则：不能给自己的图片点赞
        if (picture.getUserId() != null && picture.getUserId().equals(loginUser.getId())) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "不能给自己的图片点赞");
        }
        // 4. 执行 Redis 点赞命令
        return execute("PICTURE", picture.getId(), loginUser.getId(), picture.getUserId(), "LIKE",
                normalizeRequestId(requestId));
    }

    /**
     * 取消点赞公共图片（自动生成唯一请求 ID）
     *
     * @param pictureId 图片 ID
     * @param loginUser 当前登录用户
     * @return 点赞状态视图对象
     */
    @Override
    public CommunityLikeStatusVO unlikePublicPicture(Long pictureId, User loginUser) {
        return unlikePublicPicture(pictureId, loginUser, UUID.randomUUID().toString());
    }

    /**
     * 取消点赞公共图片（支持指定幂等请求 ID）
     *
     * @param pictureId 图片 ID
     * @param loginUser 当前登录用户
     * @param requestId 请求追踪/幂等 ID
     * @return 点赞状态视图对象
     */
    @Override
    public CommunityLikeStatusVO unlikePublicPicture(Long pictureId, User loginUser, String requestId) {
        // 1. 校验登录
        requireLogin(loginUser);
        // 2. 校验图片是否存在且合法
        Picture picture = requirePublicPicture(pictureId);
        // 3. 执行 Redis 取消点赞命令
        return execute("PICTURE", picture.getId(), loginUser.getId(), picture.getUserId(), "UNLIKE",
                normalizeRequestId(requestId));
    }

    /**
     * 获取当前用户对指定公共图片的点赞状态
     *
     * @param pictureId 图片 ID
     * @param loginUser 当前登录用户
     * @return 点赞状态视图对象
     */
    @Override
    public CommunityLikeStatusVO getPublicPictureLikeStatus(Long pictureId, User loginUser) {
        // 1. 校验登录
        requireLogin(loginUser);
        // 2. 校验图片合法性
        Picture picture = requirePublicPicture(pictureId);
        // 3. 读取点赞状态
        return readStatus("PICTURE", picture.getId(), loginUser.getId());
    }

    /**
     * 执行点赞/取消点赞的底层核心方法（调用 Redis 存储组件）
     *
     * @param targetType  目标类型（"POST" 或 "PICTURE"）
     * @param targetId    目标实体 ID
     * @param userId      操作用户 ID
     * @param ownerUserId 资源拥有者用户 ID
     * @param desiredState 期望状态（"LIKE" 或 "UNLIKE"）
     * @param requestId   请求追踪/幂等 ID
     * @return 点赞状态视图对象
     */
    private CommunityLikeStatusVO execute(String targetType, Long targetId, Long userId, Long ownerUserId,
                                          String desiredState, String requestId) {
        try {
            // 一次 Lua 脚本执行完成状态变更、计数更新和事件追加，避免分散 Redis 调用产生中间状态不一致。
            RedisLikeCommandResult result = commandStore.execute(
                    targetType, targetId, userId, ownerUserId, desiredState, requestId);
            return toVo(targetId, result);
        } catch (BusinessException exception) {
            // 业务异常直接往上抛出
            throw exception;
        } catch (RuntimeException exception) {
            // Redis 故障时采用 fail-closed（故障关闭）策略；绝不静默调用数据库版写接口，避免双源数据分叉。
            log.error("Redis like command failed: type={}, targetId={}, userId={}, state={}, requestId={}",
                    targetType, targetId, userId, desiredState, requestId, exception);
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "点赞服务暂时不可用，请稍后重试");
        }
    }

    /**
     * 读取点赞状态的底层核心方法
     *
     * @param targetType 目标类型（"POST" 或 "PICTURE"）
     * @param targetId   目标实体 ID
     * @param userId     当前用户 ID
     * @return 点赞状态视图对象
     */
    private CommunityLikeStatusVO readStatus(String targetType, Long targetId, Long userId) {
        try {
            // 使用 Lua 脚本同时读取 liked（是否点赞）和 count（点赞总数），避免多次 GET/SISMEMBER 读到不同时间点的错位状态。
            return toVo(targetId, commandStore.status(targetType, targetId, userId));
        } catch (BusinessException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            log.error("Redis like status read failed: type={}, targetId={}, userId={}", targetType, targetId, userId, exception);
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "点赞状态暂时不可用，请稍后重试");
        }
    }

    /**
     * 将 Redis 命令执行结果转换为前端 VO
     *
     * @param targetId 目标实体 ID
     * @param result   Redis 点赞命令执行结果领域对象
     * @return 点赞状态 VO
     */
    private CommunityLikeStatusVO toVo(Long targetId, RedisLikeCommandResult result) {
        CommunityLikeStatusVO vo = new CommunityLikeStatusVO();
        vo.setTargetId(targetId);
        // 如果计数为空则默认为 0
        vo.setLikeCount(result.getLikeCount() == null ? 0L : result.getLikeCount());
        // 判断当前用户是否已点赞
        vo.setLikedByCurrentUser(Boolean.TRUE.equals(result.getLiked()));
        return vo;
    }

    /**
     * 校验并获取已发布的社区帖子
     *
     * @param postId 帖子 ID
     * @return 社区帖子实体
     */
    private CommunityPost requirePublishedPost(Long postId) {
        if (postId == null || postId <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "帖子 ID 不合法");
        }
        // 从数据库查询满足“ID匹配”、“已发布”、“未删除”的帖子
        CommunityPost post = communityPostMapper.selectOne(new LambdaQueryWrapper<CommunityPost>()
                .eq(CommunityPost::getId, postId)
                .eq(CommunityPost::getStatus, POST_PUBLISHED)
                .eq(CommunityPost::getIsDelete, 0));
        if (post == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR, "帖子不存在或尚未公开");
        }
        return post;
    }

    /**
     * 校验并获取公开审核通过的图片
     *
     * @param pictureId 图片 ID
     * @return 图片实体
     */
    private Picture requirePublicPicture(Long pictureId) {
        if (pictureId == null || pictureId <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "图片 ID 不合法");
        }
        Picture picture = pictureMapper.selectById(pictureId);
        // 校验：图片必须存在、属于公共空间（spaceId 为空）、审核通过（reviewStatus == 1）、未被逻辑删除
        if (picture == null || picture.getSpaceId() != null
                || picture.getReviewStatus() == null || picture.getReviewStatus() != REVIEW_PASSED
                || picture.getIsDelete() == null || picture.getIsDelete() != 0) {
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR, "公共图片不存在或不可见");
        }
        return picture;
    }

    /**
     * 规范化并校验请求 ID (requestId)
     *
     * @param requestId 原始请求 ID
     * @return 规范化后的请求 ID
     */
    private String normalizeRequestId(String requestId) {
        // 如果未提供则随机生成 UUID，否则去除首尾空格
        String resolved = StringUtils.hasText(requestId) ? requestId.trim() : UUID.randomUUID().toString();
        // 正则校验格式，防止非法字符注入
        if (!REQUEST_ID_PATTERN.matcher(resolved).matches()) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR,
                    "X-Request-Id 只能包含字母、数字、点、下划线、冒号或连字符，长度 1-128");
        }
        return resolved;
    }

    /**
     * 校验用户是否已登录
     *
     * @同上用户对象
     */
    private void requireLogin(User user) {
        if (user == null || user.getId() == null) {
            throw new BusinessException(ErrorCode.NO_AUTH_ERROR);
        }
    }
}