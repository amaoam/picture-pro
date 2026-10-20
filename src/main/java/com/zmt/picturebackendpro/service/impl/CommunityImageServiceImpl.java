package com.zmt.picturebackendpro.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.model.CopyObjectRequest;
import com.zmt.picturebackendpro.config.CosClientConfig;
import com.zmt.picturebackendpro.exception.BusinessException;
import com.zmt.picturebackendpro.exception.ErrorCode;
import com.zmt.picturebackendpro.manager.CosManager;
import com.zmt.picturebackendpro.manager.upload.FilePictureUpload;
import com.zmt.picturebackendpro.mapper.CommunityImageMapper;
import com.zmt.picturebackendpro.model.dto.file.UploadPictureResult;
import com.zmt.picturebackendpro.model.dto.picture.PictureUploadRequest;
import com.zmt.picturebackendpro.model.entity.*;
import com.zmt.picturebackendpro.model.vo.PictureVO;
import com.zmt.picturebackendpro.model.vo.community.CommunityImageVO;
import com.zmt.picturebackendpro.service.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 社区图片服务。
 * COS 对象复制/上传与数据库事务并非一个原子操作，因此在入库失败时尽量做补偿清理。
 */
@Slf4j
@Service
public class CommunityImageServiceImpl implements CommunityImageService {

    private static final int SOURCE_EXISTING_PICTURE = 1;
    private static final int SOURCE_NEW_UPLOAD = 2;
    private static final int REVIEW_PENDING = 0;
    private static final int REVIEW_PASSED = 1;
    private static final int REVIEW_REJECTED = 2;

    @Resource
    private CommunityImageMapper communityImageMapper;
    @Resource
    private PictureService pictureService;
    @Resource
    private SpaceService spaceService;
    @Resource
    private SpaceUserService spaceUserService;
    @Resource
    private UserService userService;
    @Resource
    private FilePictureUpload filePictureUpload;
    @Resource
    private CosClientConfig cosClientConfig;
    @Resource
    private COSClient cosClient;
    @Resource
    private CosManager cosManager;

    @Override
    public CommunityImageVO uploadImage(MultipartFile file, Long spaceId, boolean saveToSpace, User loginUser) {
        //登录鉴权
        requireLogin(loginUser);
        if (file == null || file.isEmpty()) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "图片文件不能为空");
        }
        //处理不需要保持到空间的图片
        if (!saveToSpace) {
            if (spaceId != null) {
                throw new BusinessException(ErrorCode.PARAMS_ERROR, "不保存到图库时，不应传入 spaceId");
            }
            return uploadOnlyToCommunity(file, loginUser);
        }
        //处理需要保持到空间的图片
        if (spaceId == null) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "保存到个人/团队空间时必须指定 spaceId");
        }
        //判断用户是否有传入图片到空间的权限
        checkSpaceWritePermission(spaceId, loginUser);
        //设置图片上传参数
        PictureUploadRequest uploadRequest = new PictureUploadRequest();
        uploadRequest.setSpaceId(spaceId);
        if (StringUtils.hasText(file.getOriginalFilename())) {
            String originalName = file.getOriginalFilename();
            int dot = originalName.lastIndexOf('.');
            uploadRequest.setPicName(dot > 0 ? originalName.substring(0, dot) : originalName);
        }
        // 先经现有图库上传逻辑保存入个人/团队空间，复用容量校验、picture 表和空间统计。
        PictureVO savedPicture = pictureService.uploadPicture(file, uploadRequest, loginUser);
        try {
            Picture source = pictureService.getById(savedPicture.getId());
            if (source == null) {
                throw new BusinessException(ErrorCode.OPERATION_ERROR, "图片已保存到空间，但读取图片记录失败");
            }
            return copyPictureToCommunity(source, loginUser, SOURCE_NEW_UPLOAD, source.getId(), false);
        } catch (RuntimeException e) {
            // 对社区副本生成失败进行补偿：尽量删除本次新建的图库图片，避免留下用户无法判断的半成功状态。
            try {
                pictureService.deletePicture(savedPicture.getId(), loginUser);
            } catch (Exception cleanupException) {
                log.error("社区图片复制失败，且清理图库图片失败，pictureId={}", savedPicture.getId(), cleanupException);
            }
            throw e;
        }
    }

    @Override
    public CommunityImageVO copyExistingPicture(Long pictureId, User loginUser) {
        requireLogin(loginUser);
        if (pictureId == null || pictureId <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "pictureId 不合法");
        }
        Picture picture = pictureService.getById(pictureId);
        if (picture == null || (picture.getIsDelete() != null && picture.getIsDelete() == 1)) {
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR, "原图片不存在");
        }
        if (picture.getReviewStatus() == null || picture.getReviewStatus() != REVIEW_PASSED) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "只能分享审核通过的图库图片");
        }
        //进行图片的鉴权
        checkCanPublicizePicture(picture, loginUser);
        return copyPictureToCommunity(picture, loginUser, SOURCE_EXISTING_PICTURE, null, true);
    }

    @Override
    public Page<CommunityImageVO> pageReviewImages(long current, long pageSize, Integer status, User loginUser) {
        //登录and鉴权
        requireLogin(loginUser);
        if (!userService.isAdmin(loginUser)) {
            throw new BusinessException(ErrorCode.NO_AUTH_ERROR, "仅管理员可以查看社区图片审核队列");
        }
        //参数设置
        if (current < 1 || pageSize < 1 || pageSize > 50) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "分页参数不合法，每页最多 50 条");
        }
        //设置条件查询 
        LambdaQueryWrapper<CommunityImage> wrapper = new LambdaQueryWrapper<CommunityImage>()
                .eq(CommunityImage::getIsDelete, 0)
                .orderByAsc(CommunityImage::getCreateTime)
                .orderByAsc(CommunityImage::getId);
        if (status == null) {
            wrapper.eq(CommunityImage::getReviewStatus, REVIEW_PENDING);
        } else {
            if (status != REVIEW_PENDING && status != REVIEW_PASSED && status != REVIEW_REJECTED) {
                throw new BusinessException(ErrorCode.PARAMS_ERROR, "图片审核状态不合法");
            }
            wrapper.eq(CommunityImage::getReviewStatus, status);
        }
        Page<CommunityImage> imagePage = communityImageMapper.selectPage(new Page<>(current, pageSize), wrapper);
        Page<CommunityImageVO> result = new Page<>(imagePage.getCurrent(), imagePage.getSize(), imagePage.getTotal());
        List<CommunityImageVO> records = new ArrayList<>();
        for (CommunityImage image : imagePage.getRecords()) {
            records.add(CommunityImageVO.objToVo(image));
        }
        result.setRecords(records);
        return result;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void reviewImage(Long imageId, Integer status, String reviewMessage, User loginUser) {
        //登录和鉴权
        requireLogin(loginUser);
        if (!userService.isAdmin(loginUser)) {
            throw new BusinessException(ErrorCode.NO_AUTH_ERROR, "仅管理员可以审核社区图片");
        }
        if (imageId == null || imageId <= 0 || (status == null || (status != REVIEW_PASSED && status != REVIEW_REJECTED))) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "审核参数不合法");
        }
        CommunityImage image = communityImageMapper.selectById(imageId);
        if (image == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR, "社区图片不存在");
        }
        image.setReviewStatus(status);
        image.setReviewMessage(reviewMessage);
        image.setReviewerId(loginUser.getId());
        image.setReviewTime(new Date());
        image.setUpdateTime(new Date());
        if (communityImageMapper.updateById(image) != 1) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "社区图片审核更新失败");
        }
    }

    /**
     * 只上传图片到社区
     * @param file
     * @param loginUser
     * @return
     */
    private CommunityImageVO uploadOnlyToCommunity(MultipartFile file, User loginUser) {
        //上传图片并获取结果
        UploadPictureResult result = filePictureUpload.uploadPicture(file, "community/" + loginUser.getId());
        //获取文件在存储桶中的相对路径
        String storageKey = toCosKey(result.getUrl());
        //同理，不过获取的是缩略图,如果缩略图是null或者和url一样的话设置为null
        String thumbnailKey = result.getThumbnailUrl() == null || result.getThumbnailUrl().equals(result.getUrl())
                ? null : toCosKey(result.getThumbnailUrl());
        //设置CommunityImage属性
        CommunityImage image = new CommunityImage();
        image.setOwnerUserId(loginUser.getId());
        image.setSourceType(SOURCE_NEW_UPLOAD);
        image.setName(StringUtils.hasText(result.getPicName()) ? result.getPicName() : "community-image");
        image.setUrl(result.getUrl());
        image.setThumbnailUrl(result.getThumbnailUrl());
        image.setStorageKey(storageKey);
        image.setThumbnailStorageKey(thumbnailKey);
        image.setPicSize(result.getPicSize());
        image.setPicWidth(result.getPicWidth());
        image.setPicHeight(result.getPicHeight());
        image.setPicFormat(result.getPicFormat());
        // 新上传的社区图片需要审核。帖子在关联图片审核通过前也不能被公开发布。
        image.setReviewStatus(REVIEW_PENDING);
        image.setCreateTime(new Date());
        image.setUpdateTime(new Date());
        try {
            int inserted = communityImageMapper.insert(image);
            if (inserted != 1) {
                throw new BusinessException(ErrorCode.OPERATION_ERROR, "社区图片记录保存失败");
            }
        } catch (RuntimeException e) {
            //这里没有弄明白为什么要直接删除，感觉这个也不是副本啊
            //好了弄懂了，这次的图片是只存在在社区中的，要是插入失败当然要直接删除了
            deleteCosQuietly(storageKey, thumbnailKey);
            throw e;
        }
        return CommunityImageVO.objToVo(image);
    }

    /**
     * 将已存入图库或已有的审核通过图片复制到 community/ 路径，形成独立的公开副本。
     */
    private CommunityImageVO copyPictureToCommunity(Picture picture, User loginUser, int sourceType,
                                                     Long savedPictureId, boolean sourceAlreadyReviewed) {
        String sourceUrl = picture.getUrl();
        if (!StringUtils.hasText(sourceUrl)) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "源图片 URL 为空");
        }
        //获取图片在桶中的存储key
        String sourceKey = toCosKey(sourceUrl);
        String newStorageKey = createTargetKey(loginUser.getId(), sourceKey);
        String newUrl = cosClientConfig.getHost().replaceAll("/+$", "") + "/" + newStorageKey;
        String newThumbnailUrl = newUrl;
        String newThumbnailKey = null;
        boolean mainCopied = false;
        try {
            copyCosObject(sourceKey, newStorageKey);
            mainCopied = true;
            //复制缩略图
            if (StringUtils.hasText(picture.getThumbnailUrl()) && !picture.getThumbnailUrl().equals(sourceUrl)) {
                String sourceThumbnailKey = toCosKey(picture.getThumbnailUrl());
                newThumbnailKey = createTargetKey(loginUser.getId(), sourceThumbnailKey);
                copyCosObject(sourceThumbnailKey, newThumbnailKey);
                newThumbnailUrl = cosClientConfig.getHost().replaceAll("/+$", "") + "/" + newThumbnailKey;
            }
            //设置属性
            CommunityImage image = new CommunityImage();
            image.setOwnerUserId(loginUser.getId());
            image.setSourceType(sourceType);
            image.setSourcePictureId(sourceType == SOURCE_EXISTING_PICTURE ? picture.getId() : null);
            image.setSavedPictureId(savedPictureId);
            image.setName(StringUtils.hasText(picture.getName()) ? picture.getName() : "community-image");
            image.setUrl(newUrl);
            image.setThumbnailUrl(newThumbnailUrl);
            image.setStorageKey(newStorageKey);
            image.setThumbnailStorageKey(newThumbnailKey);
            image.setPicSize(picture.getPicSize());
            image.setPicWidth(picture.getPicWidth());
            image.setPicHeight(picture.getPicHeight());
            image.setPicFormat(picture.getPicFormat());
            //根据传入参数和选择的图片的审核状态设置社区图片审核状态
            image.setReviewStatus(sourceAlreadyReviewed ? REVIEW_PASSED
                    : (picture.getReviewStatus() != null && picture.getReviewStatus() == REVIEW_PASSED
                    ? REVIEW_PASSED : REVIEW_PENDING));
            if (image.getReviewStatus() == REVIEW_PASSED) {
                image.setReviewerId(picture.getReviewerId());
                image.setReviewTime(picture.getReviewTime() == null ? new Date() : picture.getReviewTime());
            }
            image.setCreateTime(new Date());
            image.setUpdateTime(new Date());
            if (communityImageMapper.insert(image) != 1) {
                throw new BusinessException(ErrorCode.OPERATION_ERROR, "社区图片记录保存失败");
            }
            return CommunityImageVO.objToVo(image);
        } catch (RuntimeException e) {
            if (mainCopied) {
                deleteCosQuietly(newStorageKey, newThumbnailKey);
            }
            throw e;
        }
    }

    private void checkCanPublicizePicture(Picture picture, User loginUser) {
        Long spaceId = picture.getSpaceId();
        if (spaceId == null) {
            // 公共图库：审核通过即可作为社区图片源。
            return;
        }
        Space space = spaceService.getById(spaceId);
        if (space == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR, "源图片所属空间不存在");
        }
        //todo 这里其实不是很对 应该是在一开始获取能从空间上传的图片的时候就只能返回自己个人空间或者有权限的团队空间的图片
        if (space.getSpaceType() != null && space.getSpaceType() == 0) {
            
            if (!loginUser.getId().equals(space.getUserId()) && !userService.isAdmin(loginUser)) {
                throw new BusinessException(ErrorCode.NO_AUTH_ERROR, "无权公开分享该个人空间图片");
            }
            return;
        }
        if (space.getSpaceType() != null && space.getSpaceType() == 1) {
            SpaceUser member = getSpaceMember(spaceId, loginUser.getId());
            if (member == null || !("admin".equals(member.getSpaceRole()) || "editor".equals(member.getSpaceRole()))) {
                throw new BusinessException(ErrorCode.NO_AUTH_ERROR, "仅团队管理员或编辑可以公开分享团队图片");
            }
            return;
        }
        throw new BusinessException(ErrorCode.OPERATION_ERROR, "不支持的空间类型");
    }

    private void checkSpaceWritePermission(Long spaceId, User loginUser) {
        Space space = spaceService.getById(spaceId);
        if (space == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR, "目标空间不存在");
        }
        if (space.getSpaceType() != null && space.getSpaceType() == 0) {
            if (!loginUser.getId().equals(space.getUserId()) && !userService.isAdmin(loginUser)) {
                throw new BusinessException(ErrorCode.NO_AUTH_ERROR, "无权向该个人空间上传图片");
            }
            return;
        }
        if (space.getSpaceType() != null && space.getSpaceType() == 1) {
            SpaceUser member = getSpaceMember(spaceId, loginUser.getId());
            if (member == null || !("admin".equals(member.getSpaceRole()) || "editor".equals(member.getSpaceRole()))) {
                throw new BusinessException(ErrorCode.NO_AUTH_ERROR, "仅团队管理员或编辑可以向团队空间上传图片");
            }
            return;
        }
        throw new BusinessException(ErrorCode.OPERATION_ERROR, "不支持的空间类型");
    }

    private SpaceUser getSpaceMember(Long spaceId, Long userId) {
        return spaceUserService.getOne(new QueryWrapper<SpaceUser>()
                .eq("spaceId", spaceId)
                .eq("userId", userId)
                .last("limit 1"));
    }

    /**
     * 从一个完整的图片 URL 中解析出腾讯云 COS（对象存储）的对象键（Key，即文件在存储桶中的相对路径）。
     * @param url
     * @return
     */
    private String toCosKey(String url) {
        String host = cosClientConfig.getHost();
        if (!StringUtils.hasText(host) || !StringUtils.hasText(url)) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "COS host 或图片 URL 为空");
        }
        //使用正则 replaceAll("/+$", "") 清理掉 host 末尾多余的斜杠（/），防止后续拼接时出现双斜杠（如 http://xxx//path）
        String normalizedHost = host.replaceAll("/+$", "");
        if (!url.startsWith(normalizedHost + "/")) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "图片 URL 与当前 COS 域名不一致，拒绝复制");
        }
        // 旧上传模板的 key 本身以 / 开头，拼接 URL 后通常会出现 host//space/...；这里保留 key 的前导斜杠。
        String key = url.substring(normalizedHost.length() + 1);
        if (!key.startsWith("/")) {
            key = "/" + key;
        }
        return key;
    }

    /**
     * 返回了一个路径格式： /community/{userId}/{时间戳}_{12位随机字符串}.{后缀}
     * @param userId
     * @param sourceKey
     * @return
     */
    private String createTargetKey(Long userId, String sourceKey) {
        String suffix = "webp";
        int dot = sourceKey.lastIndexOf('.');
        if (dot >= 0 && dot < sourceKey.length() - 1) {
            suffix = sourceKey.substring(dot + 1).replaceAll("[^A-Za-z0-9]", "");
            if (!StringUtils.hasText(suffix)) {
                suffix = "webp";
            }
        }
        return "/community/" + userId + "/" + System.currentTimeMillis() + "_"
                + cn.hutool.core.util.RandomUtil.randomString(12) + "." + suffix;
    }

    /**
     * 将图片从原来的空间复制一份到社区对应空间
     * @param sourceKey
     * @param destinationKey
     */
    private void copyCosObject(String sourceKey, String destinationKey) {
        cosClient.copyObject(new CopyObjectRequest(
                cosClientConfig.getBucket(), sourceKey,
                cosClientConfig.getBucket(), destinationKey));
    }
    //删除社区图片
    private void deleteCosQuietly(String storageKey, String thumbnailStorageKey) {
        try {
            if (StringUtils.hasText(storageKey)) {
                cosManager.deleteObject(storageKey);
            }
        } catch (Exception e) {
            log.warn("清理社区主图失败，key={}", storageKey, e);
        }
        try {
            if (StringUtils.hasText(thumbnailStorageKey) && !thumbnailStorageKey.equals(storageKey)) {
                cosManager.deleteObject(thumbnailStorageKey);
            }
        } catch (Exception e) {
            log.warn("清理社区缩略图失败，key={}", thumbnailStorageKey, e);
        }
    }

    private void requireLogin(User loginUser) {
        if (loginUser == null || loginUser.getId() == null) {
            throw new BusinessException(ErrorCode.NO_AUTH_ERROR);
        }
    }
}
