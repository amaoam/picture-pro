package com.zmt.yupicturebackendtry.manager.upload;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.RandomUtil;
import com.qcloud.cos.model.PutObjectResult;
import com.qcloud.cos.model.ciModel.persistence.CIObject;
import com.qcloud.cos.model.ciModel.persistence.ImageInfo;
import com.qcloud.cos.model.ciModel.persistence.ProcessResults;
import com.zmt.yupicturebackendtry.config.CosClientConfig;
import com.zmt.yupicturebackendtry.exception.BusinessException;
import com.zmt.yupicturebackendtry.exception.ErrorCode;
import com.zmt.yupicturebackendtry.manager.CosManager;
import com.zmt.yupicturebackendtry.model.dto.file.UploadPictureResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Date;
import java.util.List;

/**
 * @author: ZMT
 * @date: 2026/3/24
 * @Description: 文件上传模板类
 */
@RequiredArgsConstructor
@Slf4j
public abstract class PictureUploadTemplate {

    
    protected final CosManager cosManager;

    
    protected final CosClientConfig cosClientConfig;

    /**
     * 模板方法，定义上传流程  
     */
    public final UploadPictureResult uploadPicture(Object inputSource, String uploadPathPrefix) {
        // 1. 校验图片  
        String contentType = validPicture(inputSource);
        // 2. 图片上传地址  
        String uuid = RandomUtil.randomString(16);
        String originFilename = getOriginFilename(inputSource);
        String uploadFilename;
        if(!contentType.isEmpty()){
            uploadFilename = String.format("%s_%s.%s", DateUtil.formatDate(new Date()), uuid,
                    contentType);
        } else {
            uploadFilename = String.format("%s_%s.%s", DateUtil.formatDate(new Date()), uuid,
                    FileUtil.getSuffix(originFilename));
        }
        //针对ai生成的图片
        if(uploadFilename.contains("?")){
            uploadFilename = uploadFilename.split("\\?")[0];
            
        }
        String uploadPath = String.format("/%s/%s", uploadPathPrefix, uploadFilename);
        File file = null;
        try {
            // 创建临时文件
            file = File.createTempFile(uploadPath, null);
            // 处理文件来源（本地或 URL）
            processFile(inputSource, file);
            // 上传图片到对象存储
            PutObjectResult putObjectResult = cosManager.putPictureObject(uploadPath, file);
            ImageInfo imageInfo = putObjectResult.getCiUploadResult().getOriginalInfo().getImageInfo();
            ProcessResults processResults = putObjectResult.getCiUploadResult().getProcessResults();
            List<CIObject> objectList = processResults.getObjectList();
            if (CollUtil.isNotEmpty(objectList)) {
                CIObject compressedCiObject = objectList.get(0);
                // 封装压缩图返回结果
                return buildResult(originFilename, compressedCiObject,imageInfo);
            }
            // 封装原图返回结果
            return buildResult(originFilename, file, uploadPath, imageInfo);
        } catch (Exception e) {
            log.error("图片上传到对象存储失败", e);
            throw new BusinessException(ErrorCode.SYSTEM_ERROR, "上传失败");
        }

    }

    private UploadPictureResult buildResult(String originFilename, CIObject compressedCiObject,ImageInfo imageInfo) {
        UploadPictureResult uploadPictureResult = new UploadPictureResult();
        int picWidth = compressedCiObject.getWidth();
        int picHeight = compressedCiObject.getHeight();
        double picScale = NumberUtil.round(picWidth * 1.0 / picHeight, 2).doubleValue();
        uploadPictureResult.setPicName(FileUtil.mainName(originFilename));
        uploadPictureResult.setPicWidth(picWidth);
        uploadPictureResult.setPicHeight(picHeight);
        uploadPictureResult.setPicScale(picScale);
        uploadPictureResult.setPicFormat(compressedCiObject.getFormat());
        uploadPictureResult.setPicSize(compressedCiObject.getSize().longValue());
        uploadPictureResult.setPicColor(imageInfo.getAve());
        // 设置图片为压缩后的地址
        uploadPictureResult.setUrl(cosClientConfig.getHost() + "/" + compressedCiObject.getKey());
        return uploadPictureResult;
    }


    /**
     * 校验输入源（本地文件或 URL）
     *
     * @return
     */
    protected abstract String validPicture(Object inputSource);

    /**
     * 获取输入源的原始文件名  
     */
    protected abstract String getOriginFilename(Object inputSource);

    /**
     * 处理输入源并生成本地临时文件  
     */
    protected abstract void processFile(Object inputSource, File file) throws Exception;

    /**
     * 封装返回结果  
     */
    private UploadPictureResult buildResult(String originFilename, File file, String uploadPath, ImageInfo imageInfo) {
        UploadPictureResult uploadPictureResult = new UploadPictureResult();
        int picWidth = imageInfo.getWidth();
        int picHeight = imageInfo.getHeight();
        double picScale = NumberUtil.round(picWidth * 1.0 / picHeight, 2).doubleValue();
        uploadPictureResult.setPicName(FileUtil.mainName(originFilename));
        uploadPictureResult.setPicWidth(picWidth);
        uploadPictureResult.setPicHeight(picHeight);
        uploadPictureResult.setPicScale(picScale);
        uploadPictureResult.setPicFormat(imageInfo.getFormat());
        uploadPictureResult.setPicSize(FileUtil.size(file));
        uploadPictureResult.setUrl(cosClientConfig.getHost() + "/" + uploadPath);
        uploadPictureResult.setPicColor(imageInfo.getAve());
        return uploadPictureResult;
    }

    /**
     * 删除临时文件  
     */
    public void deleteTempFile(File file) {
        if (file == null) {
            return;
        }
        boolean deleteResult = file.delete();
        if (!deleteResult) {
            log.error("file delete error, filepath = {}", file.getAbsolutePath());
        }
    }
}
