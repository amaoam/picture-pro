package com.zmt.picturebackendpro.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.zmt.picturebackendpro.exception.BusinessException;
import com.zmt.picturebackendpro.exception.ErrorCode;
import com.zmt.picturebackendpro.mapper.*;
import com.zmt.picturebackendpro.model.dto.community.CommunityPostAddRequest;
import com.zmt.picturebackendpro.model.dto.community.CommunityPostQueryRequest;
import com.zmt.picturebackendpro.model.entity.*;
import com.zmt.picturebackendpro.model.vo.community.CommunityImageVO;
import com.zmt.picturebackendpro.model.vo.community.CommunityPostVO;
import com.zmt.picturebackendpro.service.CommunityPostService;
import com.zmt.picturebackendpro.service.SpaceService;
import com.zmt.picturebackendpro.service.SpaceUserService;
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
public class CommunityPostServiceImpl implements CommunityPostService {

    private static final int POST_TYPE_SHARE = 1;
    private static final int POST_TYPE_HELP = 2;
    private static final int PUBLISH_PERSONAL = 1;
    private static final int PUBLISH_TEAM = 2;
    private static final int POST_PENDING = 0;
    private static final int POST_PUBLISHED = 1;
    private static final int POST_REJECTED = 2;
    private static final int POST_REMOVED = 3;
    private static final int IMAGE_REVIEW_PASSED = 1;
    private static final int MAX_PAGE_SIZE = 20;
    private static final int MAX_POST_IMAGES = 9;
    private static final int MAX_TAGS = 10;

    @Resource
    private CommunityPostMapper communityPostMapper;
    @Resource
    private CommunityPostImageMapper communityPostImageMapper;
    @Resource
    private CommunityPostTagMapper communityPostTagMapper;
    @Resource
    private CommunityImageMapper communityImageMapper;
    @Resource
    private CommunityCategoryMapper communityCategoryMapper;
    @Resource
    private CommunityTagMapper communityTagMapper;
    @Resource
    private UserCommunityProfileMapper userCommunityProfileMapper;
    @Resource
    private SpaceService spaceService;
    @Resource
    private SpaceUserService spaceUserService;
    @Resource
    private UserService userService;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Long createPost(CommunityPostAddRequest request, User loginUser) {
        //登录
        requireLogin(loginUser);
        //校验帖子参数
        validatePostRequest(request);
        CommunityCategory category = communityCategoryMapper.selectById(request.getCategoryId());
        if (category == null || category.getStatus() == null || category.getStatus() != 1) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "帖子分类不存在或已停用");
        }
        //填充帖子参数
        CommunityPost post = new CommunityPost();
        post.setUserId(loginUser.getId());
        post.setPostType(request.getPostType());
        post.setPublishType(request.getPublishType());
        post.setTitle(request.getTitle().trim());
        post.setContent(request.getContent().trim());
        post.setCategoryId(request.getCategoryId());
        post.setStatus(POST_PENDING);
        post.setLikeCount(0L);
        post.setCommentCount(0L);
        post.setPublishUserNameSnapshot(StringUtils.hasText(loginUser.getUserName())
                ? loginUser.getUserName().trim() : "用户" + loginUser.getId());
        post.setCreateTime(new Date());
        post.setEditTime(new Date());
        post.setUpdateTime(new Date());
        //填充帖子发布时的身份快照
        fillPublisherSnapshot(post, request, loginUser);
        //限制填充图片
        List<Long> imageIds = normalizeIds(request.getImageIds(), MAX_POST_IMAGES, "图片");
        if (request.getPostType() == POST_TYPE_SHARE && imageIds.isEmpty()) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "分享帖至少需要一张图片");
        }
        if (!imageIds.isEmpty()) {
            //验证传入的图片是否在社区中存在
            validateCommunityImages(imageIds, loginUser);
        }
        //限制帖子选择的标签
        List<Long> tagIds = normalizeIds(request.getTagIds(), MAX_TAGS, "标签");
        if (!tagIds.isEmpty()) {
            validateTags(tagIds);
        }
        
        if (communityPostMapper.insert(post) != 1) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "发布帖子失败");
        }
        //todo 这里好多for循环 之后看看要不要优化
        for (int i = 0; i < imageIds.size(); i++) {
            CommunityPostImage relation = new CommunityPostImage();
            relation.setPostId(post.getId());
            relation.setCommunityImageId(imageIds.get(i));
            relation.setSortOrder(i);
            relation.setCreateTime(new Date());
            if (communityPostImageMapper.insert(relation) != 1) {
                throw new BusinessException(ErrorCode.OPERATION_ERROR, "保存帖子图片关联失败");
            }
        }
        for (Long tagId : tagIds) {
            CommunityPostTag relation = new CommunityPostTag();
            relation.setPostId(post.getId());
            relation.setTagId(tagId);
            relation.setCreateTime(new Date());
            if (communityPostTagMapper.insert(relation) != 1) {
                throw new BusinessException(ErrorCode.OPERATION_ERROR, "保存帖子标签关联失败");
            }
        }
        // 注意：经验奖励放在后续 CommunityExperienceService 中处理；当前创建接口不直接改经验值。
        return post.getId();
    }

    @Override
    public Page<CommunityPostVO> pagePublicPosts(CommunityPostQueryRequest request) {
        return queryPosts(request, false);
    }

    @Override
    public Page<CommunityPostVO> pageAdminPosts(CommunityPostQueryRequest request, User loginUser) {
        requireLogin(loginUser);
        if (!userService.isAdmin(loginUser)) {
            throw new BusinessException(ErrorCode.NO_AUTH_ERROR, "仅管理员可以查看社区审核队列");
        }
        return queryPosts(request, true);
    }

    @Override
    public CommunityPostVO getPublicPost(Long postId) {
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
        return fillPostVOList(Collections.singletonList(post)).get(0);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void reviewPost(Long postId, Integer status, User loginUser) {
        requireLogin(loginUser);
        if (!userService.isAdmin(loginUser)) {
            throw new BusinessException(ErrorCode.NO_AUTH_ERROR, "仅管理员可以审核社区帖子");
        }
        if (postId == null || postId <= 0 || status == null
                || (status != POST_PUBLISHED && status != POST_REJECTED && status != POST_REMOVED)) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "帖子审核参数不合法");
        }
        CommunityPost post = communityPostMapper.selectById(postId);
        if (post == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR, "帖子不存在");
        }
        if (status == POST_PUBLISHED) {
            List<CommunityPostImage> imageRelations = communityPostImageMapper.selectList(
                    new LambdaQueryWrapper<CommunityPostImage>().eq(CommunityPostImage::getPostId, postId));
            if (post.getPostType() != null && post.getPostType() == POST_TYPE_SHARE && imageRelations.isEmpty()) {
                throw new BusinessException(ErrorCode.OPERATION_ERROR, "分享帖至少需要一张图片，不能审核发布");
            }
            if (!imageRelations.isEmpty()) {
                List<Long> ids = imageRelations.stream().map(CommunityPostImage::getCommunityImageId).collect(Collectors.toList());
                // 使用 LambdaQueryWrapper 替代已弃用的 selectBatchIds
                List<CommunityImage> images = communityImageMapper.selectList(
                        new LambdaQueryWrapper<CommunityImage>().in(CommunityImage::getId, ids));
                if (images.size() != ids.size() || images.stream().anyMatch(image -> image.getReviewStatus() == null
                        || image.getReviewStatus() != IMAGE_REVIEW_PASSED)) {
                    throw new BusinessException(ErrorCode.OPERATION_ERROR, "帖子中仍存在未通过审核的图片");
                }
            }
        }
        post.setStatus(status);
        post.setEditTime(new Date());
        post.setUpdateTime(new Date());
        if (communityPostMapper.updateById(post) != 1) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "帖子审核更新失败");
        }
    }

    @Override
    public List<CommunityCategory> listCategories() {
        return communityCategoryMapper.selectList(new LambdaQueryWrapper<CommunityCategory>()
                .eq(CommunityCategory::getStatus, 1)
                .orderByAsc(CommunityCategory::getSortOrder)
                .orderByAsc(CommunityCategory::getId));
    }

    @Override
    public List<CommunityTag> listTags(String keyword) {
        LambdaQueryWrapper<CommunityTag> wrapper = new LambdaQueryWrapper<CommunityTag>()
                .eq(CommunityTag::getStatus, 1)
                .orderByDesc(CommunityTag::getUseCount)
                .orderByAsc(CommunityTag::getTagName)
                .last("limit 100");
        if (StringUtils.hasText(keyword)) {
            wrapper.like(CommunityTag::getTagName, keyword.trim());
        }
        return communityTagMapper.selectList(wrapper);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CommunityTag createOrGetTag(String tagName, User loginUser) {
        requireLogin(loginUser);
        if (!StringUtils.hasText(tagName)) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "标签名不能为空");
        }
        String normalizedName = tagName.trim();
        if (normalizedName.length() > 32) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "标签名不能超过 32 个字符");
        }
        CommunityTag existing = communityTagMapper.selectOne(new LambdaQueryWrapper<CommunityTag>()
                .eq(CommunityTag::getTagName, normalizedName));
        if (existing != null) {
            if (existing.getStatus() == null || existing.getStatus() != 1) {
                throw new BusinessException(ErrorCode.OPERATION_ERROR, "该标签已停用");
            }
            return existing;
        }
        CommunityTag tag = new CommunityTag();
        tag.setTagName(normalizedName);
        tag.setUseCount(0L);
        tag.setStatus(1);
        tag.setCreateTime(new Date());
        tag.setUpdateTime(new Date());
        try {
            if (communityTagMapper.insert(tag) != 1) {
                throw new BusinessException(ErrorCode.OPERATION_ERROR, "创建标签失败");
            }
            return tag;
        } catch (DuplicateKeyException duplicateKeyException) {
            // 并发请求创建同名标签时，唯一索引负责最终裁决；读取已存在标签即可。
            CommunityTag concurrent = communityTagMapper.selectOne(new LambdaQueryWrapper<CommunityTag>()
                    .eq(CommunityTag::getTagName, normalizedName));
            if (concurrent != null && concurrent.getStatus() != null && concurrent.getStatus() == 1) {
                return concurrent;
            }
            throw duplicateKeyException;
        }
    }

    private Page<CommunityPostVO> queryPosts(CommunityPostQueryRequest request, boolean adminMode) {
        //一大堆校验填充参数
        CommunityPostQueryRequest safeRequest = request == null ? new CommunityPostQueryRequest() : request;
        long current = safeRequest.getCurrent();
        long size = safeRequest.getPageSize();
        if (current < 1 || size < 1 || size > MAX_PAGE_SIZE) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "分页参数不合法，每页最多 20 条");
        }
        LambdaQueryWrapper<CommunityPost> wrapper = new LambdaQueryWrapper<CommunityPost>()
                .eq(CommunityPost::getIsDelete, 0)
                .orderByDesc(CommunityPost::getCreateTime)
                .orderByDesc(CommunityPost::getId);
        if (!adminMode) {
            wrapper.eq(CommunityPost::getStatus, POST_PUBLISHED);
        } else if (safeRequest.getStatus() != null) {
            wrapper.eq(CommunityPost::getStatus, safeRequest.getStatus());
        }
        if (safeRequest.getPostType() != null) {
            if (safeRequest.getPostType() != POST_TYPE_SHARE && safeRequest.getPostType() != POST_TYPE_HELP) {
                throw new BusinessException(ErrorCode.PARAMS_ERROR, "帖子类型不合法");
            }
            wrapper.eq(CommunityPost::getPostType, safeRequest.getPostType());
        }
        if (safeRequest.getCategoryId() != null) {
            wrapper.eq(CommunityPost::getCategoryId, safeRequest.getCategoryId());
        }
        if (safeRequest.getTagId() != null) {
            wrapper.apply("id IN (SELECT postId FROM community_post_tag WHERE tagId = {0})", safeRequest.getTagId());
        }
        if (StringUtils.hasText(safeRequest.getSearchText())) {
            String keyword = safeRequest.getSearchText().trim();
            if (keyword.length() > 100) {
                throw new BusinessException(ErrorCode.PARAMS_ERROR, "搜索词过长");
            }
            //todo  这里的查询效率不知道怎么样
            wrapper.and(w -> w.like(CommunityPost::getTitle, keyword).or().like(CommunityPost::getContent, keyword));
        }
        Page<CommunityPost> postPage = communityPostMapper.selectPage(new Page<>(current, size), wrapper);
        List<CommunityPostVO> records = fillPostVOList(postPage.getRecords());
        Page<CommunityPostVO> voPage = new Page<>(postPage.getCurrent(), postPage.getSize(), postPage.getTotal());
        voPage.setRecords(records);
        return voPage;
    }
    //为查询到的帖子填充图片等参数
    private List<CommunityPostVO> fillPostVOList(List<CommunityPost> posts) {
        if (posts == null || posts.isEmpty()) {
            return Collections.emptyList();
        }
        List<Long> postIds = posts.stream().map(CommunityPost::getId).collect(Collectors.toList());
        List<Long> userIds = posts.stream().map(CommunityPost::getUserId).distinct().collect(Collectors.toList());
        List<Long> categoryIds = posts.stream().map(CommunityPost::getCategoryId).distinct().collect(Collectors.toList());
        //查出所有帖子图片关联类
        List<CommunityPostImage> imageRelations = communityPostImageMapper.selectList(
                new LambdaQueryWrapper<CommunityPostImage>().in(CommunityPostImage::getPostId, postIds)
                        .orderByAsc(CommunityPostImage::getSortOrder).orderByAsc(CommunityPostImage::getId));
        //建立图片和帖子之间的映射关系
        Map<Long, List<CommunityPostImage>> relationByPost = imageRelations.stream()
                .collect(Collectors.groupingBy(CommunityPostImage::getPostId));
        //查询社区图片id
        List<Long> communityImageIds = imageRelations.stream().map(CommunityPostImage::getCommunityImageId).distinct()
                .collect(Collectors.toList());
        //建立社区图片id和社区图片的映射
        Map<Long, CommunityImage> imageMap = communityImageIds.isEmpty() ? new HashMap<>()
                : communityImageMapper.selectList(
                        new LambdaQueryWrapper<CommunityImage>().in(CommunityImage::getId, communityImageIds))
                .stream().collect(Collectors.toMap(CommunityImage::getId, image -> image, (a, b) -> a));

        // 查询涉及到的帖子标签类（增加对 postIds 的判空防护，防止空集合引发 SQL 异常）
        List<CommunityPostTag> tagRelations = (postIds == null || postIds.isEmpty()) ? new ArrayList<>()
                : communityPostTagMapper.selectList(
                new LambdaQueryWrapper<CommunityPostTag>().in(CommunityPostTag::getPostId, postIds));

        // 建立帖子 id 和帖子标签类的映射
        Map<Long, List<CommunityPostTag>> tagRelationByPost = tagRelations.stream()
                .collect(Collectors.groupingBy(CommunityPostTag::getPostId));
        // 查询标签 id
        List<Long> tagIds = tagRelations.stream().map(CommunityPostTag::getTagId).distinct().collect(Collectors.toList());
        // 标签 id 和标签类的映射
        Map<Long, CommunityTag> tagMap = tagIds.isEmpty() ? new HashMap<>()
                : communityTagMapper.selectList(
                        new LambdaQueryWrapper<CommunityTag>().in(CommunityTag::getId, tagIds))
                .stream().collect(Collectors.toMap(CommunityTag::getId, tag -> tag, (a, b) -> a));
        // 分类 id 和分类的映射（补上判空保护，防止 categoryIds 为空时生成非法的 SQL IN 语句）
        Map<Long, CommunityCategory> categoryMap = (categoryIds == null || categoryIds.isEmpty()) ? new HashMap<>()
                : communityCategoryMapper.selectList(
                        new LambdaQueryWrapper<CommunityCategory>().in(CommunityCategory::getId, categoryIds))
                .stream().collect(Collectors.toMap(CommunityCategory::getId, category -> category, (a, b) -> a));        //用户id和用户的映射
        Map<Long, User> userMap = userService.listByIds(userIds).stream()
                .collect(Collectors.toMap(User::getId, user -> user, (a, b) -> a));
        // 用户 id 和用户社区资料的映射（增加判空保护，防止 userIds 为空时触发异常）
        Map<Long, UserCommunityProfile> profileMap = (userIds == null || userIds.isEmpty()) ? new HashMap<>()
                : userCommunityProfileMapper.selectList(
                        new LambdaQueryWrapper<UserCommunityProfile>().in(UserCommunityProfile::getUserId, userIds))
                .stream().collect(Collectors.toMap(UserCommunityProfile::getUserId, profile -> profile, (a, b) -> a));        //开始填充返回结果
        List<CommunityPostVO> result = new ArrayList<>();
        for (CommunityPost post : posts) {
            CommunityPostVO vo = new CommunityPostVO();
            vo.setId(post.getId());
            vo.setUserId(post.getUserId());
            vo.setPostType(post.getPostType());
            vo.setPublishType(post.getPublishType());
            vo.setPublishSpaceId(post.getPublishSpaceId());
            vo.setPublishUserName(post.getPublishUserNameSnapshot());
            vo.setPublishSpaceName(post.getPublishSpaceNameSnapshot());
            vo.setPublishRole(post.getPublishRoleSnapshot());
            vo.setTitle(post.getTitle());
            vo.setContent(post.getContent());
            vo.setCategoryId(post.getCategoryId());
            CommunityCategory category = categoryMap.get(post.getCategoryId());
            vo.setCategoryName(category == null ? null : category.getCategoryName());
            vo.setStatus(post.getStatus());
            vo.setLikeCount(post.getLikeCount() == null ? 0L : post.getLikeCount());
            vo.setCommentCount(post.getCommentCount() == null ? 0L : post.getCommentCount());
            vo.setCreateTime(post.getCreateTime());
            vo.setUpdateTime(post.getUpdateTime());

            User author = userMap.get(post.getUserId());
            if (author != null) {
                vo.setPublishUserAvatar(author.getUserAvatar());
            }
            UserCommunityProfile profile = profileMap.get(post.getUserId());
            vo.setPublishUserLevel(profile == null || profile.getLevel() == null ? 1 : profile.getLevel());

            List<CommunityPostImage> postImageRelations = relationByPost.get(post.getId());
            if (postImageRelations != null) {
                List<CommunityImageVO> images = new ArrayList<>();
                for (CommunityPostImage relation : postImageRelations) {
                    CommunityImage image = imageMap.get(relation.getCommunityImageId());
                    if (image != null && image.getReviewStatus() != null && image.getReviewStatus() == IMAGE_REVIEW_PASSED) {
                        images.add(CommunityImageVO.objToVo(image));
                    }
                }
                vo.setImages(images);
            }
            List<CommunityPostTag> postTagRelations = tagRelationByPost.get(post.getId());
            if (postTagRelations != null) {
                List<String> names = new ArrayList<>();
                for (CommunityPostTag relation : postTagRelations) {
                    CommunityTag tag = tagMap.get(relation.getTagId());
                    if (tag != null && tag.getStatus() != null && tag.getStatus() == 1) {
                        names.add(tag.getTagName());
                    }
                }
                vo.setTags(names);
            }
            result.add(vo);
        }
        return result;
    }

    private void fillPublisherSnapshot(CommunityPost post, CommunityPostAddRequest request, User loginUser) {
        if (request.getPublishType() == PUBLISH_PERSONAL) {
            if (request.getPublishSpaceId() != null) {
                throw new BusinessException(ErrorCode.PARAMS_ERROR, "个人身份发布时不能指定团队空间");
            }
            post.setPublishSpaceId(null);
            post.setPublishSpaceNameSnapshot(null);
            post.setPublishRoleSnapshot(null);
            return;
        }
        if (request.getPublishType() != PUBLISH_TEAM || request.getPublishSpaceId() == null) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "团队身份发布时必须指定团队空间");
        }
        Space space = spaceService.getById(request.getPublishSpaceId());
        if (space == null || space.getSpaceType() == null || space.getSpaceType() != 1) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "目标空间不是有效的团队空间");
        }
        SpaceUser member = spaceUserService.getOne(new QueryWrapper<SpaceUser>()
                .eq("spaceId", space.getId()).eq("userId", loginUser.getId()).last("limit 1"));
        if (member == null || !("admin".equals(member.getSpaceRole()) || "editor".equals(member.getSpaceRole()))) {
            throw new BusinessException(ErrorCode.NO_AUTH_ERROR, "仅团队管理员或编辑可以使用团队身份发帖");
        }
        post.setPublishSpaceId(space.getId());
        post.setPublishSpaceNameSnapshot(space.getSpaceName());
        post.setPublishRoleSnapshot(member.getSpaceRole());
    }

    private void validateCommunityImages(List<Long> imageIds, User loginUser) {
        List<CommunityImage> images = (imageIds == null || imageIds.isEmpty()) ? new ArrayList<>()
                : communityImageMapper.selectList(
                new LambdaQueryWrapper<CommunityImage>().in(CommunityImage::getId, imageIds));
        if (images.size() != imageIds.size()) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "部分社区图片不存在或已删除");
        }
        Map<Long, CommunityImage> imageMap = images.stream()
                .collect(Collectors.toMap(CommunityImage::getId, image -> image, (a, b) -> a));
        for (Long imageId : imageIds) {
            CommunityImage image = imageMap.get(imageId);
            if (image == null) {
                throw new BusinessException(ErrorCode.PARAMS_ERROR, "社区图片不存在");
            }
            if (image.getReviewStatus() != null && image.getReviewStatus() == 2) {
                throw new BusinessException(ErrorCode.OPERATION_ERROR, "不能使用审核拒绝的社区图片");
            }
            if ((image.getReviewStatus() == null || image.getReviewStatus() != IMAGE_REVIEW_PASSED)
                    && !loginUser.getId().equals(image.getOwnerUserId())) {
                throw new BusinessException(ErrorCode.NO_AUTH_ERROR, "不能使用其他用户尚未审核通过的社区图片");
            }
        }
    }

    private void validateTags(List<Long> tagIds) {
        List<CommunityTag> tags = communityTagMapper.selectBatchIds(tagIds);
        if (tags.size() != tagIds.size() || tags.stream().anyMatch(tag -> tag.getStatus() == null || tag.getStatus() != 1)) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "部分标签不存在或已停用");
        }
    }

    private List<Long> normalizeIds(List<Long> ids, int maxSize, String fieldName) {
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyList();
        }
        if (ids.size() > maxSize || ids.stream().anyMatch(id -> id == null || id <= 0)) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, fieldName + "参数不合法或数量超限");
        }
        List<Long> normalized = new ArrayList<>(new LinkedHashSet<>(ids));
        if (normalized.size() != ids.size()) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, fieldName + "不能重复");
        }
        return normalized;
    }

    private void validatePostRequest(CommunityPostAddRequest request) {
        if (request == null) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "请求参数不能为空");
        }
        if (request.getPostType() == null || (request.getPostType() != POST_TYPE_SHARE && request.getPostType() != POST_TYPE_HELP)) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "帖子类型不合法");
        }
        if (request.getPublishType() == null || (request.getPublishType() != PUBLISH_PERSONAL && request.getPublishType() != PUBLISH_TEAM)) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "发布身份不合法");
        }
        if (!StringUtils.hasText(request.getTitle()) || request.getTitle().trim().length() > 128) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "标题不能为空且不能超过 128 个字符");
        }
        if (!StringUtils.hasText(request.getContent()) || request.getContent().trim().length() > 10000) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "正文不能为空且不能超过 10000 个字符");
        }
        if (request.getCategoryId() == null || request.getCategoryId() <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "必须选择有效的社区分类");
        }
    }

    private void requireLogin(User loginUser) {
        if (loginUser == null || loginUser.getId() == null) {
            throw new BusinessException(ErrorCode.NO_AUTH_ERROR);
        }
    }
}
