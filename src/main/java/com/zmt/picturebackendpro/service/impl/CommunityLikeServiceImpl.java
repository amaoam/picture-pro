package com.zmt.picturebackendpro.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.zmt.picturebackendpro.exception.BusinessException;
import com.zmt.picturebackendpro.exception.ErrorCode;
import com.zmt.picturebackendpro.mapper.CommunityLikeOutboxMapper;
import com.zmt.picturebackendpro.mapper.CommunityPostLikeMapper;
import com.zmt.picturebackendpro.mapper.CommunityPostMapper;
import com.zmt.picturebackendpro.mapper.PictureLikeMapper;
import com.zmt.picturebackendpro.mapper.PictureMapper;
import com.zmt.picturebackendpro.model.entity.CommunityLikeOutboxEvent;
import com.zmt.picturebackendpro.model.entity.CommunityPost;
import com.zmt.picturebackendpro.model.entity.CommunityPostLike;
import com.zmt.picturebackendpro.model.entity.Picture;
import com.zmt.picturebackendpro.model.entity.PictureLike;
import com.zmt.picturebackendpro.model.entity.User;
import com.zmt.picturebackendpro.model.vo.community.CommunityLikeStatusVO;
import com.zmt.picturebackendpro.service.CommunityLikeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Date;

/**
 * 点赞关系仍然同步写 MySQL；只有关系真正新增/删除时才生成 Outbox 事件。
 * 统计计数由后台消费者批量更新，接口中的 likeCount 因而允许短暂滞后。
 */
@Slf4j
@Service
public class CommunityLikeServiceImpl implements CommunityLikeService {

    private static final int POST_PUBLISHED = 1;
    private static final int REVIEW_PASSED = 1;

    @Resource private CommunityPostMapper communityPostMapper;
    @Resource private CommunityPostLikeMapper communityPostLikeMapper;
    @Resource private PictureMapper pictureMapper;
    @Resource private PictureLikeMapper pictureLikeMapper;
    @Resource private CommunityLikeOutboxMapper outboxMapper;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CommunityLikeStatusVO likePost(Long postId, User loginUser) {
        //登录鉴权 和 帖子的检验与获取
        requireLogin(loginUser);
        CommunityPost post = requirePublishedPost(postId);
        if (loginUser.getId().equals(post.getUserId())) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "不能给自己的帖子点赞");
        }

        CommunityPostLike like = new CommunityPostLike();
        like.setPostId(postId);
        like.setUserId(loginUser.getId());
        like.setCreateTime(new Date());
        int inserted;
        try {
            // 不再先 SELECT 判断关系是否存在；唯一索引负责并发去重。
            inserted = communityPostLikeMapper.insert(like);
        } catch (DuplicateKeyException duplicate) {
            // 只捕获点赞关系表的唯一键冲突；Outbox 插入异常必须向外抛出并回滚整个事务。
            log.debug("Duplicate post like ignored: post={}, user={}", postId, loginUser.getId());
            return postStatus(post, loginUser.getId());
        }
        if (inserted == 1) {
            insertEvent(CommunityLikeOutboxEvent.TARGET_POST, postId, loginUser.getId(), post.getUserId(), 1);
        }
        return postStatus(post, loginUser.getId());
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CommunityLikeStatusVO unlikePost(Long postId, User loginUser) {
        requireLogin(loginUser);
        CommunityPost post = requirePublishedPost(postId);
        int deleted = communityPostLikeMapper.delete(new LambdaQueryWrapper<CommunityPostLike>()
                .eq(CommunityPostLike::getPostId, postId)
                .eq(CommunityPostLike::getUserId, loginUser.getId()));
        if (deleted > 0) {
            insertEvent(CommunityLikeOutboxEvent.TARGET_POST, postId, loginUser.getId(), post.getUserId(), -1);
        }
        return postStatus(post, loginUser.getId());
    }

    @Override
    public CommunityLikeStatusVO getPostLikeStatus(Long postId, User loginUser) {
        requireLogin(loginUser);
        return postStatus(requirePublishedPost(postId), loginUser.getId());
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CommunityLikeStatusVO likePublicPicture(Long pictureId, User loginUser) {
        requireLogin(loginUser);
        Picture picture = requirePublicPicture(pictureId);
        if (picture.getUserId() != null && picture.getUserId().equals(loginUser.getId())) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "不能给自己的图片点赞");
        }

        PictureLike like = new PictureLike();
        like.setPictureId(pictureId);
        like.setUserId(loginUser.getId());
        like.setCreateTime(new Date());
        int inserted;
        try {
            inserted = pictureLikeMapper.insert(like);
        } catch (DuplicateKeyException duplicate) {
            log.debug("Duplicate picture like ignored: picture={}, user={}", pictureId, loginUser.getId());
            return pictureStatus(picture, loginUser.getId());
        }
        if (inserted == 1) {
            insertEvent(CommunityLikeOutboxEvent.TARGET_PICTURE, pictureId,
                    loginUser.getId(), picture.getUserId(), 1);
        }
        return pictureStatus(picture, loginUser.getId());
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CommunityLikeStatusVO unlikePublicPicture(Long pictureId, User loginUser) {
        requireLogin(loginUser);
        Picture picture = requirePublicPicture(pictureId);
        int deleted = pictureLikeMapper.delete(new LambdaQueryWrapper<PictureLike>()
                .eq(PictureLike::getPictureId, pictureId)
                .eq(PictureLike::getUserId, loginUser.getId()));
        if (deleted > 0) {
            insertEvent(CommunityLikeOutboxEvent.TARGET_PICTURE, pictureId,
                    loginUser.getId(), picture.getUserId(), -1);
        }
        return pictureStatus(picture, loginUser.getId());
    }

    @Override
    public CommunityLikeStatusVO getPublicPictureLikeStatus(Long pictureId, User loginUser) {
        requireLogin(loginUser);
        return pictureStatus(requirePublicPicture(pictureId), loginUser.getId());
    }
    //插入事务消息表
    private void insertEvent(String targetType, Long targetId, Long actorUserId, Long ownerUserId, int delta) {
        CommunityLikeOutboxEvent event = new CommunityLikeOutboxEvent();
        event.setTargetType(targetType);
        event.setTargetId(targetId);
        event.setActorUserId(actorUserId);
        event.setOwnerUserId(ownerUserId);
        event.setDelta(delta);
        event.setStatus(CommunityLikeOutboxEvent.STATUS_PENDING);
        event.setRetryCount(0);
        event.setCreateTime(new Date());
        if (outboxMapper.insert(event) != 1) {
            // 让外层事务回滚，不能出现点赞关系已提交但没有计数事件的情况。
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "创建点赞计数事件失败");
        }
    }

    private CommunityLikeStatusVO postStatus(CommunityPost post, Long userId) {
        CommunityLikeStatusVO vo = new CommunityLikeStatusVO();
        vo.setTargetId(post.getId());
        vo.setLikeCount(post.getLikeCount() == null ? 0L : post.getLikeCount());
        vo.setLikedByCurrentUser(findPostLike(post.getId(), userId) != null);
        return vo;
    }

    private CommunityLikeStatusVO pictureStatus(Picture picture, Long userId) {
        CommunityLikeStatusVO vo = new CommunityLikeStatusVO();
        vo.setTargetId(picture.getId());
        // 不再 COUNT(*) 扫描 picture_like，直接读取 picture.likeCount。
        vo.setLikeCount(picture.getLikeCount() == null ? 0L : picture.getLikeCount());
        vo.setLikedByCurrentUser(findPictureLike(picture.getId(), userId) != null);
        return vo;
    }

    private CommunityPost requirePublishedPost(Long postId) {
        if (postId == null || postId <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "帖子 ID 不合法");
        }
        CommunityPost post = communityPostMapper.selectOne(new LambdaQueryWrapper<CommunityPost>()
                .eq(CommunityPost::getId, postId)
                .eq(CommunityPost::getStatus, POST_PUBLISHED)
                .eq(CommunityPost::getIsDelete, 0));
        if (post == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR, "帖子不存在或尚未公开");
        }
        return post;
    }

    private Picture requirePublicPicture(Long pictureId) {
        if (pictureId == null || pictureId <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "图片 ID 不合法");
        }
        Picture picture = pictureMapper.selectById(pictureId);
        if (picture == null || picture.getSpaceId() != null
                || picture.getReviewStatus() == null || picture.getReviewStatus() != REVIEW_PASSED
                || picture.getIsDelete() == null || picture.getIsDelete() != 0) {
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR, "公共图片不存在或不可见");
        }
        return picture;
    }

    private CommunityPostLike findPostLike(Long postId, Long userId) {
        return communityPostLikeMapper.selectOne(new LambdaQueryWrapper<CommunityPostLike>()
                .eq(CommunityPostLike::getPostId, postId)
                .eq(CommunityPostLike::getUserId, userId)
                .last("limit 1"));
    }

    private PictureLike findPictureLike(Long pictureId, Long userId) {
        return pictureLikeMapper.selectOne(new LambdaQueryWrapper<PictureLike>()
                .eq(PictureLike::getPictureId, pictureId)
                .eq(PictureLike::getUserId, userId)
                .last("limit 1"));
    }

    private void requireLogin(User loginUser) {
        if (loginUser == null || loginUser.getId() == null) {
            throw new BusinessException(ErrorCode.NO_AUTH_ERROR);
        }
    }
}
