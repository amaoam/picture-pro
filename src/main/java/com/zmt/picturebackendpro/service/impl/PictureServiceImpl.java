package com.zmt.picturebackendpro.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.ObjUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.zmt.picturebackendpro.api.aliyunai.model.AliYunAiApi;
import com.zmt.picturebackendpro.api.aliyunai.model.CreateOutPaintingTaskRequest;
import com.zmt.picturebackendpro.api.aliyunai.model.CreateOutPaintingTaskResponse;
import com.zmt.picturebackendpro.exception.BusinessException;
import com.zmt.picturebackendpro.exception.ErrorCode;
import com.zmt.picturebackendpro.exception.ThrowUtils;
import com.zmt.picturebackendpro.manager.CosManager;
import com.zmt.picturebackendpro.manager.upload.FilePictureUpload;
import com.zmt.picturebackendpro.manager.upload.PictureUploadTemplate;
import com.zmt.picturebackendpro.manager.upload.UrlPictureUpload;
import com.zmt.picturebackendpro.model.dto.file.UploadPictureResult;
import com.zmt.picturebackendpro.model.dto.picture.*;
import com.zmt.picturebackendpro.model.entity.Picture;
import com.zmt.picturebackendpro.model.entity.Space;
import com.zmt.picturebackendpro.model.entity.User;
import com.zmt.picturebackendpro.model.enums.PictureReviewStatusEnum;
import com.zmt.picturebackendpro.model.vo.PictureVO;
import com.zmt.picturebackendpro.model.vo.UserVO;
import com.zmt.picturebackendpro.service.PictureService;
import com.zmt.picturebackendpro.mapper.PictureMapper;
import com.zmt.picturebackendpro.service.SpaceService;
import com.zmt.picturebackendpro.service.UserService;

import com.zmt.picturebackendpro.utils.ColorSimilarUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.beans.BeanUtils;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import javax.servlet.http.HttpServletRequest;
import java.awt.*;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

/**
* @author ZMT
* @description 针对表【picture(图片)】的数据库操作Service实现
* @createDate 2026-03-23 09:38:11
*/
@Service
@RequiredArgsConstructor
@Slf4j
public class PictureServiceImpl extends ServiceImpl<PictureMapper, Picture> implements PictureService{
    
    private final FilePictureUpload filePictureUpload;
    
    private final UrlPictureUpload urlPictureUpload;
    
    private final UserService userService;
    
    private final SpaceService spaceService;
    
    private final CosManager cosManager;
    
    private final TransactionTemplate transactionTemplate;
    
    private final ThreadPoolExecutor customExecutor;
    
    private final AliYunAiApi aliYunAiApi;
    
    

    @Override
    public PictureVO uploadPicture(Object inputSource, PictureUploadRequest pictureUploadRequest, User loginUser) {
        ThrowUtils.throwIf(loginUser == null, ErrorCode.NO_AUTH_ERROR);
//        // 空间权限校验
        Long spaceId = pictureUploadRequest.getSpaceId();
//        if (spaceId != null) {
//            Space space = spaceService.getById(spaceId);
//            ThrowUtils.throwIf(space == null, ErrorCode.NOT_FOUND_ERROR, "空间不存在");
//            // 必须空间创建人（管理员）才能上传
//            
//            if (!loginUser.getId().equals(space.getUserId())) {
//                throw new BusinessException(ErrorCode.NO_AUTH_ERROR, "没有空间权限");
//            }
//            // 校验额度
//            if (space.getTotalCount() >= space.getMaxCount()) {
//                throw new BusinessException(ErrorCode.OPERATION_ERROR, "空间条数不足");
//            }
//            if (space.getTotalSize() >= space.getMaxSize()) {
//                throw new BusinessException(ErrorCode.OPERATION_ERROR, "空间大小不足");
//            }
//        }
        //用于判断是新增图片还是更新图片
        Long pictureId = null;
        pictureId = pictureUploadRequest.getId()==null?null:pictureUploadRequest.getId();
        //如果是更新图片  需要校验图片是否存在
        Picture oldPicture = this.getById(pictureId);
        if (pictureId != null) {
            ThrowUtils.throwIf(oldPicture == null, ErrorCode.NOT_FOUND_ERROR, "图片不存在");
//            // 仅本人或管理员可编辑
//            if ((!oldPicture.getUserId().equals(loginUser.getId()) && !userService.isAdmin(loginUser))) {
//                throw new BusinessException(ErrorCode.NO_AUTH_ERROR);
//            }
            // 校验空间是否一致
            // 没传 spaceId，则复用原有图片的 spaceId
            if (spaceId == null) {
                if (oldPicture.getSpaceId() != null) {
                    spaceId = oldPicture.getSpaceId();
                }
            } else {
                // 传了 spaceId，必须和原有图片一致
                if (ObjUtil.notEqual(spaceId, oldPicture.getSpaceId())) {
                    throw new BusinessException(ErrorCode.PARAMS_ERROR, "空间 id 不一致");
                }
            }
        }
        //上传图片，得到信息
//        String uploadPathPrefix = String.format("public/%s",loginUser.getId());
        // 按照用户 id 划分目录 => 按照空间划分目录
        String uploadPathPrefix;
        if (spaceId == null) {
            uploadPathPrefix = String.format("public/%s", loginUser.getId());
        } else {
            uploadPathPrefix = String.format("space/%s", spaceId);
        }
        PictureUploadTemplate pictureUploadTemplate = filePictureUpload;
        if(inputSource instanceof String){
            pictureUploadTemplate = urlPictureUpload;
        }
        UploadPictureResult uploadPictureResult = pictureUploadTemplate.uploadPicture(inputSource,uploadPathPrefix);
        // 构造要入库的图片信息  
        Picture picture = new Picture();
        picture.setUrl(uploadPictureResult.getUrl());
        String picName = uploadPictureResult.getPicName();
        if (pictureUploadRequest != null && StrUtil.isNotBlank(pictureUploadRequest.getPicName())) {
            picName = pictureUploadRequest.getPicName();
        }
        //通过name的长度异常来判断是ai生成的图片结果，进而继承原图片的名字
        if(picName.length()>10&&oldPicture!=null) {
            picName = oldPicture.getName();
        }
        picture.setName(picName);
        picture.setPicSize(uploadPictureResult.getPicSize());
        picture.setPicWidth(uploadPictureResult.getPicWidth());
        picture.setPicHeight(uploadPictureResult.getPicHeight());
        picture.setPicScale(uploadPictureResult.getPicScale());
        picture.setPicFormat(uploadPictureResult.getPicFormat());
        picture.setUserId(loginUser.getId());
        picture.setSpaceId(spaceId);
        picture.setPicColor(uploadPictureResult.getPicColor());
        this.fillReviewParams(picture,loginUser);
        // 如果 pictureId 不为空，表示更新，否则是新增  
        if (pictureId != null) {
            // 如果是更新，需要补充 id 和编辑时间  
            picture.setId(pictureId);
            picture.setEditTime(new Date());
        }
        // 开启事务
        Long finalSpaceId = spaceId;
        transactionTemplate.execute(status -> {
            // 插入数据
            boolean result = this.saveOrUpdate(picture);
            ThrowUtils.throwIf(!result, ErrorCode.OPERATION_ERROR, "图片上传失败，数据库操作失败");
            if (finalSpaceId != null) {
                // 更新空间的使用额度
                boolean update = spaceService.lambdaUpdate()
                        .eq(Space::getId, finalSpaceId)
                        .setSql("totalSize = totalSize + " + picture.getPicSize())
                        .setSql("totalCount = totalCount + 1")
                        .update();
                ThrowUtils.throwIf(!update, ErrorCode.OPERATION_ERROR, "额度更新失败");
            }
            return picture;
        });
        // 可自行实现，如果是更新，可以清理图片资源
        // this.clearPictureFile(oldPicture);
        return PictureVO.objToVo(picture);
    }

    @Override
    public QueryWrapper<Picture> getQueryWrapper(PictureQueryRequest pictureQueryRequest) {
        QueryWrapper<Picture> queryWrapper = new QueryWrapper<>();
        if (pictureQueryRequest == null) {
            return queryWrapper;
        }
        // 从对象中取值  
        Long id = pictureQueryRequest.getId();
        String name = pictureQueryRequest.getName();
        String introduction = pictureQueryRequest.getIntroduction();
        String category = pictureQueryRequest.getCategory();
        List<String> tags = pictureQueryRequest.getTags();
        Long picSize = pictureQueryRequest.getPicSize();
        Integer picWidth = pictureQueryRequest.getPicWidth();
        Integer picHeight = pictureQueryRequest.getPicHeight();
        Double picScale = pictureQueryRequest.getPicScale();
        String picFormat = pictureQueryRequest.getPicFormat();
        String searchText = pictureQueryRequest.getSearchText();
        Long userId = pictureQueryRequest.getUserId();
        String sortField = pictureQueryRequest.getSortField();
        String sortOrder = pictureQueryRequest.getSortOrder();
        Integer reviewStatus = pictureQueryRequest.getReviewStatus();
        String reviewMessage = pictureQueryRequest.getReviewMessage();
        Long reviewerId = pictureQueryRequest.getReviewerId();
        Long spaceId = pictureQueryRequest.getSpaceId();
        boolean nullSpaceId = pictureQueryRequest.isNullSpaceId();
        // 从多字段中搜索  
        if (StrUtil.isNotBlank(searchText)) {
            // 需要拼接查询条件  
            queryWrapper.and(qw -> qw.like("name", searchText)
                    .or()
                    .like("introduction", searchText)
            );
        }
        Date startEditTime = pictureQueryRequest.getStartEditTime();
        Date endEditTime = pictureQueryRequest.getEndEditTime();
        queryWrapper.eq(ObjUtil.isNotEmpty(id), "id", id);
        queryWrapper.eq(ObjUtil.isNotEmpty(userId), "userId", userId);
        queryWrapper.like(StrUtil.isNotBlank(name), "name", name);
        queryWrapper.like(StrUtil.isNotBlank(introduction), "introduction", introduction);
        queryWrapper.like(StrUtil.isNotBlank(picFormat), "picFormat", picFormat);
        queryWrapper.eq(StrUtil.isNotBlank(category), "category", category);
        queryWrapper.eq(ObjUtil.isNotEmpty(picWidth), "picWidth", picWidth);
        queryWrapper.eq(ObjUtil.isNotEmpty(picHeight), "picHeight", picHeight);
        queryWrapper.eq(ObjUtil.isNotEmpty(picSize), "picSize", picSize);
        queryWrapper.eq(ObjUtil.isNotEmpty(picScale), "picScale", picScale);
        queryWrapper.eq(ObjUtil.isNotEmpty(reviewStatus),"reviewStatus",reviewStatus);
        queryWrapper.like(StrUtil.isNotBlank(reviewMessage),"reviewMessage",reviewMessage);
        queryWrapper.eq(ObjUtil.isNotEmpty(reviewerId),"reviewerId",reviewerId);
        queryWrapper.eq(ObjUtil.isNotEmpty(spaceId), "spaceId", spaceId);
        queryWrapper.isNull(nullSpaceId, "spaceId");
        queryWrapper.ge(ObjUtil.isNotEmpty(startEditTime), "editTime", startEditTime);
        queryWrapper.lt(ObjUtil.isNotEmpty(endEditTime), "editTime", endEditTime);
        // JSON 数组查询  
        if (CollUtil.isNotEmpty(tags)) {
            for (String tag : tags) {
                queryWrapper.like("tags", "\"" + tag + "\"");
            }
        }
        // 排序  
        queryWrapper.orderBy(StrUtil.isNotEmpty(sortField), sortOrder.equals("ascend"), sortField);
        return queryWrapper;
    }

    @Override
    public PictureVO getPictureVO(Picture picture, HttpServletRequest request) {
        // 对象转封装类  
        PictureVO pictureVO = PictureVO.objToVo(picture);
        // 关联查询用户信息  
        Long userId = picture.getUserId();
        if (userId != null && userId > 0) {
            User user = userService.getById(userId);
            UserVO userVO = userService.getUserVO(user);
            pictureVO.setUser(userVO);
        }
        return pictureVO;
    }

    /**
     * 分页获取图片封装  
     */
    @Override
    public Page<PictureVO> getPictureVOPage(Page<Picture> picturePage, HttpServletRequest request) {
        List<Picture> pictureList = picturePage.getRecords();
        Page<PictureVO> pictureVOPage = new Page<>(picturePage.getCurrent(), picturePage.getSize(), picturePage.getTotal());
        if (CollUtil.isEmpty(pictureList)) {
            return pictureVOPage;
        }
        // 对象列表 => 封装对象列表  
        List<PictureVO> pictureVOList = pictureList.stream().map(PictureVO::objToVo).collect(Collectors.toList());
        // 1. 关联查询用户信息  
        Set<Long> userIdSet = pictureList.stream().map(Picture::getUserId).collect(Collectors.toSet());
        Map<Long, User> userIdUserMap = userService.listByIds(userIdSet).stream()
                .collect(Collectors.toMap(User::getId, user -> user));
        // 2. 填充信息  
        pictureVOList.forEach(pictureVO -> {
            Long userId = pictureVO.getUserId();
            User user = null;
            if (userIdUserMap.containsKey(userId)) {
                user = userIdUserMap.get(userId);
            }   
            pictureVO.setUser(userService.getUserVO(user));
        });
        pictureVOPage.setRecords(pictureVOList);
        return pictureVOPage;
    }

    @Override
    public void validPicture(Picture picture) {
        ThrowUtils.throwIf(picture == null, ErrorCode.PARAMS_ERROR);
        // 从对象中取值  
        Long id = picture.getId();
        String url = picture.getUrl();
        String introduction = picture.getIntroduction();
        // 修改数据时，id 不能为空，有参数则校验  
        ThrowUtils.throwIf(ObjUtil.isNull(id), ErrorCode.PARAMS_ERROR, "id 不能为空");
        if (StrUtil.isNotBlank(url)) {
            ThrowUtils.throwIf(url.length() > 1024, ErrorCode.PARAMS_ERROR, "url 过长");
        }
        if (StrUtil.isNotBlank(introduction)) {
            ThrowUtils.throwIf(introduction.length() > 800, ErrorCode.PARAMS_ERROR, "简介过长");
        }
    }
    
    
    @Override
    public void doPictureReview(PictureReviewRequest pictureReviewRequest,User loginUser){
        Long id = pictureReviewRequest.getId();
        Integer reviewStatus = pictureReviewRequest.getReviewStatus();
        PictureReviewStatusEnum pictureReviewStatusEnum = PictureReviewStatusEnum.getEnumByValue(reviewStatus);
        if(id == null||pictureReviewStatusEnum==null||PictureReviewStatusEnum.REVIEWING.equals(pictureReviewStatusEnum)){
            throw new BusinessException(ErrorCode.PARAMS_ERROR);
        }
        Picture oldPicture = this.getById(id);
        ThrowUtils.throwIf(oldPicture==null,ErrorCode.NOT_FOUND_ERROR);
        if(oldPicture.getReviewStatus().equals(reviewStatus)){
            throw new BusinessException(ErrorCode.PARAMS_ERROR,"该图片已经被审核判定为该状态");
        }
        Picture updatePicture = new Picture();
        BeanUtils.copyProperties(pictureReviewRequest,updatePicture);
        updatePicture.setReviewerId(loginUser.getId());
        updatePicture.setReviewTime(new Date());
        boolean result = this.updateById(updatePicture);
        ThrowUtils.throwIf(!result,ErrorCode.OPERATION_ERROR);
    }

    @Override
    public void fillReviewParams(Picture picture, User loginUser) {
        if (userService.isAdmin(loginUser)) {
            // 管理员自动过审  
            picture.setReviewStatus(PictureReviewStatusEnum.PASS.getValue());
            picture.setReviewerId(loginUser.getId());
            picture.setReviewMessage("管理员自动过审");
            picture.setReviewTime(new Date());
        } else {
            // 非管理员，创建或编辑都要改为待审核  
            picture.setReviewStatus(PictureReviewStatusEnum.REVIEWING.getValue());
        }
    }

    @Override
    public Integer uploadPictureByBatch(PictureUploadByBatchRequest pictureUploadByBatchRequest, User loginUser) {
        String searchText = pictureUploadByBatchRequest.getSearchText();
        String namePrefix = pictureUploadByBatchRequest.getNamePrefix();
        if (StrUtil.isBlank(namePrefix)) {
            namePrefix = searchText;
        }
        // 格式化数量  
        Integer count = pictureUploadByBatchRequest.getCount();
        ThrowUtils.throwIf(count > 30, ErrorCode.PARAMS_ERROR, "最多 30 条");
        // 要抓取的地址  
        String fetchUrl = String.format("https://cn.bing.com/images/async?q=%s&mmasync=1", searchText);
        Document document;
        try {
            document = Jsoup.connect(fetchUrl).get();
        } catch (IOException e) {
            log.error("获取页面失败", e);
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "获取页面失败");
        }
        Element div = document.getElementsByClass("dgControl").first();
        if (ObjUtil.isNull(div)) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "获取元素失败");
        }
        Elements imgElementList = div.select(".iusc");
        int uploadCount = 0;
        for (Element imgElement : imgElementList) {
//            String fileUrl = imgElement.attr("src");
//            if (StrUtil.isBlank(fileUrl)) {
//                log.info("当前链接为空，已跳过: {}", fileUrl);
//                continue;
//            }
//            // 处理图片上传地址，防止出现转义问题  
//            int questionMarkIndex = fileUrl.indexOf("?");
//            if (questionMarkIndex > -1) {
//                fileUrl = fileUrl.substring(0, questionMarkIndex);
//            }
            String dataM = imgElement.attr("m");
            String fileUrl;
            try {
                // 解析JSON字符串
                JSONObject jsonObject = JSONUtil.parseObj(dataM);
                // 获取murl字段（原始图片URL）
                fileUrl = jsonObject.getStr("murl");
            } catch (Exception e) {
                log.error("解析图片数据失败", e);
                continue;
            }
            if (StrUtil.isBlank(fileUrl)) {
                log.info("当前链接为空，已跳过: {}", fileUrl);
                continue;
            }
            // 处理图片上传地址，防止出现转义问题  
            int questionMarkIndex = fileUrl.indexOf("?");
            if (questionMarkIndex > -1) {
                fileUrl = fileUrl.substring(0, questionMarkIndex);
            }
            // 上传图片  
            PictureUploadRequest pictureUploadRequest = new PictureUploadRequest();
            if (StrUtil.isNotBlank(namePrefix)) {
                // 设置图片名称，序号连续递增  
                pictureUploadRequest.setPicName(namePrefix + (uploadCount + 1));
            }
            try {
                PictureVO pictureVO = this.uploadPicture(fileUrl, pictureUploadRequest, loginUser);
                log.info("图片上传成功, id = {}", pictureVO.getId());
                uploadCount++;
                
            } catch (Exception e) {
                log.error("图片上传失败", e);
                continue;
            }
            if (uploadCount >= count) {
                break;
            }
        }
        return uploadCount;
    }


    @Override
    public void checkPictureAuth(User loginUser, Picture picture) {
        Long spaceId = picture.getSpaceId();
        if (spaceId == null) {
            // 公共图库，仅本人或管理员可操作
            if (!picture.getUserId().equals(loginUser.getId()) && !userService.isAdmin(loginUser)) {
                throw new BusinessException(ErrorCode.NO_AUTH_ERROR);
            }
        } else {
            // 私有空间，仅空间管理员可操作
            if (!picture.getUserId().equals(loginUser.getId())) {
                throw new BusinessException(ErrorCode.NO_AUTH_ERROR);
            }
        }
    }

    @Override
    public void deletePicture(long pictureId, User loginUser) {
        ThrowUtils.throwIf(pictureId <= 0, ErrorCode.PARAMS_ERROR);
        ThrowUtils.throwIf(loginUser == null, ErrorCode.NO_AUTH_ERROR);
        // 判断是否存在
        Picture oldPicture = this.getById(pictureId);
        ThrowUtils.throwIf(oldPicture == null, ErrorCode.NOT_FOUND_ERROR);
        // 校验权限
//        checkPictureAuth(loginUser, oldPicture);
        // 开启事务
        transactionTemplate.execute(status -> {
            // 操作数据库
            boolean result = this.removeById(pictureId);
            ThrowUtils.throwIf(!result, ErrorCode.OPERATION_ERROR);
            // 释放额度
            Long spaceId = oldPicture.getSpaceId();
            if (spaceId != null) {
                boolean update = spaceService.lambdaUpdate()
                        .eq(Space::getId, spaceId)
                        .setSql("totalSize = totalSize - " + oldPicture.getPicSize())
                        .setSql("totalCount = totalCount - 1")
                        .update();
                ThrowUtils.throwIf(!update, ErrorCode.OPERATION_ERROR, "额度更新失败");
            }
            return true;
        });
        // 异步清理文件
        this.clearPictureFile(oldPicture);

    }

    @Async
    @Override
    public void clearPictureFile(Picture oldPicture) {
        // 判断改图片是否被多条记录使用
        String pictureUrl = oldPicture.getUrl();
        long count = this.lambdaQuery()
                .eq(Picture::getUrl, pictureUrl)
                .count();
        // 有不止一条记录用到了该图片，不清理
        if (count > 1) {
            return;
        }
        // 删除图片
        cosManager.deleteObject(pictureUrl);
        // 删除缩略图
        String thumbnailUrl = oldPicture.getThumbnailUrl();
        if (StrUtil.isNotBlank(thumbnailUrl)) {
            cosManager.deleteObject(thumbnailUrl);
        }
    }

    @Override
    public void editPicture(PictureEditRequest pictureEditRequest, User loginUser) {
        // 在此处将实体类和 DTO 进行转换
        Picture picture = new Picture();
        BeanUtils.copyProperties(pictureEditRequest, picture);
        // 注意将 list 转为 string
        picture.setTags(JSONUtil.toJsonStr(pictureEditRequest.getTags()));
        // 设置编辑时间
        picture.setEditTime(new Date());
        // 数据校验
        this.validPicture(picture);
        // 判断是否存在
        long id = pictureEditRequest.getId();
        Picture oldPicture = this.getById(id);
        ThrowUtils.throwIf(oldPicture == null, ErrorCode.NOT_FOUND_ERROR);
        // 校验权限
//        checkPictureAuth(loginUser, oldPicture);
        // 补充审核参数
        this.fillReviewParams(picture, loginUser);
        // 操作数据库
        boolean result = this.updateById(picture);
        ThrowUtils.throwIf(!result, ErrorCode.OPERATION_ERROR);
    }


    @Override
    public List<PictureVO> searchPictureByColor(Long spaceId, String picColor, User loginUser) {
        // 1. 校验参数
        ThrowUtils.throwIf(spaceId == null || StrUtil.isBlank(picColor), ErrorCode.PARAMS_ERROR);
        ThrowUtils.throwIf(loginUser == null, ErrorCode.NO_AUTH_ERROR);
        // 2. 校验空间权限
        Space space = spaceService.getById(spaceId);
        ThrowUtils.throwIf(space == null, ErrorCode.NOT_FOUND_ERROR, "空间不存在");
//        if (!loginUser.getId().equals(space.getUserId())) {
//            throw new BusinessException(ErrorCode.NO_AUTH_ERROR, "没有空间访问权限");
//        }
        // 3. 查询该空间下所有图片（必须有主色调）
        List<Picture> pictureList = this.lambdaQuery()
                .eq(Picture::getSpaceId, spaceId)
                .isNotNull(Picture::getPicColor)
                .list();
        // 如果没有图片，直接返回空列表
        if (CollUtil.isEmpty(pictureList)) {
            return Collections.emptyList();
        }
        // 将目标颜色转为 Color 对象
        Color targetColor = Color.decode(picColor);
        // 4. 计算相似度并排序
        List<Picture> sortedPictures = pictureList.stream()
                .sorted(Comparator.comparingDouble(picture -> {
                    // 提取图片主色调
                    String hexColor = picture.getPicColor();
                    // 没有主色调的图片放到最后
                    if (StrUtil.isBlank(hexColor)) {
                        return Double.MAX_VALUE;
                    }
                    Color pictureColor = Color.decode(hexColor);
                    // 越大越相似
                    return ColorSimilarUtils.calculateSimilarity(targetColor, pictureColor);
                }))
                // 取前 12 个
                .limit(12)
                .collect(Collectors.toList());

        // 转换为 PictureVO
        return sortedPictures.stream()
                .map(PictureVO::objToVo)
                .collect(Collectors.toList());
    }
    
    /**
     * 批量编辑图片分类和标签
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void editPictureByBatch(PictureEditByBatchRequest pictureEditByBatchRequest, User loginUser) {
        List<Long> pictureIdList = pictureEditByBatchRequest.getPictureIdList();
        Long spaceId = pictureEditByBatchRequest.getSpaceId();
        String category = pictureEditByBatchRequest.getCategory();
        List<String> tags = pictureEditByBatchRequest.getTags();
        // 1. 校验参数
        ThrowUtils.throwIf(spaceId == null || CollUtil.isEmpty(pictureIdList), ErrorCode.PARAMS_ERROR);
        ThrowUtils.throwIf(loginUser == null, ErrorCode.NO_AUTH_ERROR);
        // 2. 校验空间权限
        Space space = spaceService.getById(spaceId);
        ThrowUtils.throwIf(space == null, ErrorCode.NOT_FOUND_ERROR, "空间不存在");
//        if (!loginUser.getId().equals(space.getUserId())) {
//            throw new BusinessException(ErrorCode.NO_AUTH_ERROR, "没有空间访问权限");
//        }
        // 查询空间下的图片
        List<Picture> pictureList = this.lambdaQuery()
                .eq(Picture::getSpaceId, spaceId)
                .in(Picture::getId, pictureEditByBatchRequest.getPictureIdList())
                .list();
        if (pictureList.isEmpty()) {
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR, "指定的图片不存在或不属于该空间");
        }
        // 批量重命名
        String nameRule = pictureEditByBatchRequest.getNameRule();
        fillPictureWithNameRule(pictureList, nameRule);
        // 分批处理避免长事务
        int batchSize = 100;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < pictureList.size(); i += batchSize) {
            List<Picture> batch = pictureList.subList(i, Math.min(i + batchSize, pictureList.size()));

            // 异步处理每批数据
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                batch.forEach(picture -> {
                    // 编辑分类和标签
                    if (pictureEditByBatchRequest.getCategory() != null) {
                        picture.setCategory(pictureEditByBatchRequest.getCategory());
                    }
                    if (pictureEditByBatchRequest.getTags() != null) {
                        picture.setTags(String.join(",", pictureEditByBatchRequest.getTags()));
                    }
                });
                boolean result = this.updateBatchById(batch);
                if (!result) {
                    throw new BusinessException(ErrorCode.OPERATION_ERROR, "批量更新图片失败");
                }
            }, customExecutor);

            futures.add(future);
        }

        // 等待所有任务完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    /**
     * nameRule 格式：图片{序号}
     *
     * @param pictureList
     * @param nameRule
     */
    private void fillPictureWithNameRule(List<Picture> pictureList, String nameRule) {
        if (CollUtil.isEmpty(pictureList) || StrUtil.isBlank(nameRule)) {
            return;
        }
        long count = 1;
        try {
            for (Picture picture : pictureList) {
                String pictureName = nameRule.replaceAll("\\{序号}", String.valueOf(count++));
                picture.setName(pictureName);
            }
        } catch (Exception e) {
            log.error("名称解析错误", e);
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "名称解析错误");
        }
    }

    @Override
    public CreateOutPaintingTaskResponse createPictureOutPaintingTask(CreatePictureOutPaintingTaskRequest createPictureOutPaintingTaskRequest, User loginUser) {
        // 获取图片信息
        Long pictureId = createPictureOutPaintingTaskRequest.getPictureId();
        Picture picture = Optional.ofNullable(this.getById(pictureId))
                .orElseThrow(() -> new BusinessException(ErrorCode.NOT_FOUND_ERROR));
        // 权限校验
//        checkPictureAuth(loginUser, picture);
        // 构造请求参数
        CreateOutPaintingTaskRequest taskRequest = new CreateOutPaintingTaskRequest();
        CreateOutPaintingTaskRequest.Input input = new CreateOutPaintingTaskRequest.Input();
        input.setImageUrl(picture.getUrl());
        taskRequest.setInput(input);
        BeanUtil.copyProperties(createPictureOutPaintingTaskRequest, taskRequest);
        // 创建任务
        return aliYunAiApi.createOutPaintingTask(taskRequest);
    }
    
}




