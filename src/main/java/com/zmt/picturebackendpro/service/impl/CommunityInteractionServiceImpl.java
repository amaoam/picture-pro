package com.zmt.picturebackendpro.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.zmt.picturebackendpro.exception.BusinessException;
import com.zmt.picturebackendpro.exception.ErrorCode;
import com.zmt.picturebackendpro.mapper.*;
import com.zmt.picturebackendpro.model.dto.community.CommunityCommentQueryRequest;
import com.zmt.picturebackendpro.model.entity.*;
import com.zmt.picturebackendpro.model.vo.CommunityCommentVO;
import com.zmt.picturebackendpro.model.vo.CommunityLikeStatusVO;
import com.zmt.picturebackendpro.service.CommunityExperienceService;
import com.zmt.picturebackendpro.service.CommunityInteractionService;
import com.zmt.picturebackendpro.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class CommunityInteractionServiceImpl implements CommunityInteractionService {

    private static final int POST_PUBLISHED = 1;
    private static final int IMAGE_REVIEW_PASSED = 1;
    private static final int MAX_PAGE_SIZE = 50;
    private static final int COMMENT_NORMAL = 1;

    @Resource private CommunityPostMapper communityPostMapper;
    @Resource private CommunityPostLikeMapper communityPostLikeMapper;
    @Resource private CommunityCommentMapper communityCommentMapper;
    @Resource private PictureLikeMapper pictureLikeMapper;
    @Resource private PictureMapper pictureMapper;
    @Resource private UserCommunityProfileMapper profileMapper;
    @Resource private CommunityExperienceService experienceService;
    @Resource private UserService userService;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CommunityLikeStatusVO likePost(Long postId, User loginUser) {
        requireLogin(loginUser);
        //根据id获取帖子
        CommunityPost post = requirePublishedPost(postId);
        if (loginUser.getId().equals(post.getUserId())) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "不能给自己的帖子点赞");
        }
        //查询是否已经有点赞记录
        CommunityPostLike existing = findPostLike(postId, loginUser.getId());
        if (existing == null) {
            CommunityPostLike like = new CommunityPostLike();
            like.setPostId(postId);
            like.setUserId(loginUser.getId());
            like.setCreateTime(new Date());
            try {
                if (communityPostLikeMapper.insert(like) == 1) {
                    //todo 这里是采用更新数据库的方式 之后肯定要升级到高并发的场景
                    updatePostCounter(postId, "likeCount", 1);
                    //给帖子用户增加经验 
                    experienceService.recordPostLikeAdded(postId, loginUser.getId(), post.getUserId());
                }
            } catch (DuplicateKeyException duplicateKeyException) {
                // 并发重复点赞由唯一键兜底，不再次更新计数或经验。
                log.debug("Duplicate post like ignored: post={}, user={}", postId, loginUser.getId());
            }
        }
        //返回当前用户点赞视图
        return postStatus(postId, loginUser.getId());
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CommunityLikeStatusVO unlikePost(Long postId, User loginUser) {
        //确认登录
        requireLogin(loginUser);
        //获取正确的帖子
        CommunityPost post = requirePublishedPost(postId);
        //查询之前是否存在点赞记录
        CommunityPostLike existing = findPostLike(postId, loginUser.getId());
        if (existing != null) {
            //todo  同点赞业务 需要升级到高并发场景
            int deleted = communityPostLikeMapper.delete(new LambdaQueryWrapper<CommunityPostLike>()
                    .eq(CommunityPostLike::getId, existing.getId())
                    .eq(CommunityPostLike::getUserId, loginUser.getId()));
            if (deleted > 0) {
                updatePostCounter(postId, "likeCount", -1);
                //去除帖子点赞数（但是不扣除之前点赞带来的经验）
                experienceService.recordPostLikeRemoved(post.getUserId());
            }
        }
        //返回当前用户视图
        return postStatus(postId, loginUser.getId());
    }

    @Override
    public CommunityLikeStatusVO getPostLikeStatus(Long postId, User loginUser) {
        //校验登录和帖子状态
        requireLogin(loginUser);
        requirePublishedPost(postId);
        return postStatus(postId, loginUser.getId());
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CommunityLikeStatusVO likePublicPicture(Long pictureId, User loginUser) {
        //对用户登录和图片状态进行检查
        requireLogin(loginUser);
        Picture picture = requirePublicPicture(pictureId);
        if (picture.getUserId() != null && picture.getUserId().equals(loginUser.getId())) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "不能给自己的图片点赞");
        }
        //查看是否已经存在点赞记录
        PictureLike existing = findPictureLike(pictureId, loginUser.getId());
        if (existing == null) {
            PictureLike like = new PictureLike();
            like.setPictureId(pictureId);
            like.setUserId(loginUser.getId());
            like.setCreateTime(new Date());
            try {
                if (pictureLikeMapper.insert(like) == 1) {
                    //为图片所有者添加经验
                    experienceService.recordPictureLikeAdded(pictureId, loginUser.getId(), picture.getUserId());
                }
            } catch (DuplicateKeyException duplicateKeyException) {
                log.debug("Duplicate picture like ignored: picture={}, user={}", pictureId, loginUser.getId());
            }
        }
        return pictureStatus(pictureId, loginUser.getId());
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CommunityLikeStatusVO unlikePublicPicture(Long pictureId, User loginUser) {
        //对用户登录和图片状态进行检查
        requireLogin(loginUser);
        Picture picture = requirePublicPicture(pictureId);
        PictureLike existing = findPictureLike(pictureId, loginUser.getId());
        if (existing != null) {
            int deleted = pictureLikeMapper.delete(new LambdaQueryWrapper<PictureLike>()
                    .eq(PictureLike::getId, existing.getId())
                    .eq(PictureLike::getUserId, loginUser.getId()));
            if (deleted > 0) {
                //图片点赞数目减一
                experienceService.recordPictureLikeRemoved(picture.getUserId());
            }
        }
        return pictureStatus(pictureId, loginUser.getId());
    }

    @Override
    public CommunityLikeStatusVO getPublicPictureLikeStatus(Long pictureId, User loginUser) {
        requireLogin(loginUser);
        requirePublicPicture(pictureId);
        return pictureStatus(pictureId, loginUser.getId());
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Long createComment(Long postId, Long parentId, String content, User loginUser) {
        //登录鉴权和帖子状态检查
        requireLogin(loginUser);
        CommunityPost post = requirePublishedPost(postId);
        if (!StringUtils.hasText(content) || content.trim().length() > 2000) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "评论内容不能为空且不能超过 2000 个字符");
        }
        Long replyToUserId = null;
        if (parentId != null) {
            if (parentId <= 0) {
                throw new BusinessException(ErrorCode.PARAMS_ERROR, "父评论 ID 不合法");
            }
            CommunityComment parent = communityCommentMapper.selectOne(new LambdaQueryWrapper<CommunityComment>()
                    .eq(CommunityComment::getId, parentId)
                    .eq(CommunityComment::getPostId, postId)
                    .eq(CommunityComment::getIsDelete, 0)
                    .eq(CommunityComment::getStatus, COMMENT_NORMAL));
            if (parent == null) {
                throw new BusinessException(ErrorCode.PARAMS_ERROR, "父评论不存在或不可回复");
            }
            if (parent.getParentId() != null) {
                throw new BusinessException(ErrorCode.PARAMS_ERROR, "暂不支持超过一级的评论回复");
            }
            replyToUserId = parent.getUserId();
        }
        CommunityComment comment = new CommunityComment();
        comment.setPostId(postId);
        comment.setUserId(loginUser.getId());
        comment.setParentId(parentId);
        comment.setReplyToUserId(replyToUserId);
        comment.setContent(content.trim());
        comment.setStatus(COMMENT_NORMAL);
        comment.setCreateTime(new Date());
        comment.setUpdateTime(new Date());
        if (communityCommentMapper.insert(comment) != 1) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "发表评论失败");
        }
        //todo 更新帖子评论数量  依旧等待升级高并发
        updatePostCounter(postId, "commentCount", 1);
        experienceService.recordCommentCreated(comment.getId(), postId, loginUser.getId());
        return comment.getId();
    }

    @Override
    public Page<CommunityCommentVO> pageComments(Long postId, CommunityCommentQueryRequest request) {
        requirePublishedPost(postId);
        CommunityCommentQueryRequest safe = request == null ? new CommunityCommentQueryRequest() : request;
        long current = safe.getCurrent();
        long size = safe.getPageSize();
        Long parentId = safe.getParentId();
        if (current < 1 || size < 1 || size > MAX_PAGE_SIZE) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "评论分页参数不合法，每页最多 50 条");
        }
        Page<CommunityComment> commentPage = communityCommentMapper.selectPage(new Page<>(current, size),
                new LambdaQueryWrapper<CommunityComment>()
                        .eq(CommunityComment::getPostId, postId)
                        .eq(CommunityComment::getStatus, COMMENT_NORMAL)
                        .eq(CommunityComment::getIsDelete, 0)
                        .eq(CommunityComment::getParentId,parentId)
                        .orderByDesc(CommunityComment::getCreateTime)
                        .orderByDesc(CommunityComment::getId));
        List<CommunityCommentVO> records = fillCommentVOs(commentPage.getRecords());
        Page<CommunityCommentVO> result = new Page<>(commentPage.getCurrent(), commentPage.getSize(), commentPage.getTotal());
        result.setRecords(records);
        return result;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void deleteComment(Long commentId, User loginUser) {
        requireLogin(loginUser);
        if (commentId == null || commentId <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "评论 ID 不合法");
        }
        CommunityComment comment = communityCommentMapper.selectById(commentId);
        if (comment == null || comment.getIsDelete() == null || comment.getIsDelete() != 0) {
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR, "评论不存在");
        }
        if (!loginUser.getId().equals(comment.getUserId()) && !userService.isAdmin(loginUser)) {
            throw new BusinessException(ErrorCode.NO_AUTH_ERROR, "只能删除自己的评论");
        }
        //todo  这个判断也有问题
        if (comment.getParentId() == null) {
            Long replyCount = communityCommentMapper.selectCount(new LambdaQueryWrapper<CommunityComment>()
                    .eq(CommunityComment::getParentId, commentId)
                    .eq(CommunityComment::getStatus, COMMENT_NORMAL)
                    .eq(CommunityComment::getIsDelete, 0));
            if (replyCount != null && replyCount > 0) {
                throw new BusinessException(ErrorCode.OPERATION_ERROR, "该评论已有回复，请先删除回复");
            }
        }
        int updated = communityCommentMapper.update(null, new LambdaUpdateWrapper<CommunityComment>()
                .eq(CommunityComment::getId, commentId)
                .eq(CommunityComment::getIsDelete, 0)
                .set(CommunityComment::getIsDelete, 1)
                .set(CommunityComment::getUpdateTime, new Date()));
        if (updated > 0) {
            updatePostCounter(comment.getPostId(), "commentCount", -1);
            experienceService.recordCommentDeleted(comment.getId(), comment.getPostId(), comment.getUserId());
        }
    }

    private List<CommunityCommentVO> fillCommentVOs(List<CommunityComment> comments) {
        //给评论填充参数 主要是用户资料
        if (comments == null || comments.isEmpty()) {
            return Collections.emptyList();
        }
        Set<Long> userIds = new LinkedHashSet<>();
        for (CommunityComment comment : comments) {
            if (comment.getUserId() != null) userIds.add(comment.getUserId());
            if (comment.getReplyToUserId() != null) userIds.add(comment.getReplyToUserId());
        }
        Map<Long, User> userMap = userService.listByIds(userIds).stream()
                .collect(Collectors.toMap(User::getId, user -> user, (a, b) -> a));
        Map<Long, UserCommunityProfile> profileMap = (userIds.isEmpty()) ? new HashMap<>()
                : profileMapper.selectList(
                        new LambdaQueryWrapper<UserCommunityProfile>().in(UserCommunityProfile::getUserId, userIds))
                .stream().collect(Collectors.toMap(UserCommunityProfile::getUserId, profile -> profile, (a, b) -> a));
        List<CommunityCommentVO> result = new ArrayList<>();
        //todo 依旧for循环
        for (CommunityComment comment : comments) {
            CommunityCommentVO vo = new CommunityCommentVO();
            vo.setId(comment.getId());
            vo.setPostId(comment.getPostId());
            vo.setUserId(comment.getUserId());
            vo.setParentId(comment.getParentId());
            vo.setReplyToUserId(comment.getReplyToUserId());
            vo.setContent(comment.getContent());
            vo.setCreateTime(comment.getCreateTime());
            User author = userMap.get(comment.getUserId());
            if (author != null) {
                vo.setUserName(author.getUserName());
                vo.setUserAvatar(author.getUserAvatar());
            }
            User replyTo = userMap.get(comment.getReplyToUserId());
            if (replyTo != null) {
                vo.setReplyToUserName(replyTo.getUserName());
            }
            UserCommunityProfile profile = profileMap.get(comment.getUserId());
            vo.setUserLevel(profile == null || profile.getLevel() == null ? 1 : profile.getLevel());
            result.add(vo);
        }
        return result;
    }

    private void updatePostCounter(Long postId, String counter, int delta) {
        LambdaUpdateWrapper<CommunityPost> wrapper = new LambdaUpdateWrapper<CommunityPost>()
                .eq(CommunityPost::getId, postId)
                .eq(CommunityPost::getIsDelete, 0)
                .eq(CommunityPost::getStatus, POST_PUBLISHED);
        if ("likeCount".equals(counter)) {
            if (delta > 0) {
                wrapper.setSql("likeCount = COALESCE(likeCount, 0) + 1");
            } else {
                wrapper.setSql("likeCount = CASE WHEN COALESCE(likeCount, 0) > 0 THEN likeCount - 1 ELSE 0 END");
            }
        } else if ("commentCount".equals(counter)) {
            if (delta > 0) {
                wrapper.setSql("commentCount = COALESCE(commentCount, 0) + 1");
            } else {
                wrapper.setSql("commentCount = CASE WHEN COALESCE(commentCount, 0) > 0 THEN commentCount - 1 ELSE 0 END");
            }
        } else {
            throw new IllegalArgumentException("Unknown counter: " + counter);
        }
        if (communityPostMapper.update(null, wrapper) != 1) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "更新帖子互动计数失败");
        }
    }

    private CommunityLikeStatusVO postStatus(Long postId, Long userId) {
        CommunityPost post = requirePublishedPost(postId);
        CommunityLikeStatusVO vo = new CommunityLikeStatusVO();
        vo.setTargetId(postId);
        vo.setLikeCount(post.getLikeCount() == null ? 0L : post.getLikeCount());
        //查看用户是否点赞
        vo.setLikedByCurrentUser(findPostLike(postId, userId) != null);
        return vo;
    }

    private CommunityLikeStatusVO pictureStatus(Long pictureId, Long userId) {
        Long count = pictureLikeMapper.selectCount(new LambdaQueryWrapper<PictureLike>()
                .eq(PictureLike::getPictureId, pictureId));
        CommunityLikeStatusVO vo = new CommunityLikeStatusVO();
        vo.setTargetId(pictureId);
        vo.setLikeCount(count == null ? 0L : count);
        vo.setLikedByCurrentUser(findPictureLike(pictureId, userId) != null);
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
                || picture.getReviewStatus() == null || picture.getReviewStatus() != IMAGE_REVIEW_PASSED
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
