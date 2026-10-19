package com.zmt.picturebackendpro.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.zmt.picturebackendpro.api.aliyunai.model.CreateOutPaintingTaskResponse;
import com.zmt.picturebackendpro.model.dto.picture.*;
import com.zmt.picturebackendpro.model.entity.Picture;
import com.baomidou.mybatisplus.extension.service.IService;
import com.zmt.picturebackendpro.model.entity.User;
import com.zmt.picturebackendpro.model.vo.PictureVO;
import org.springframework.scheduling.annotation.Async;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
* @author ZMT
* @description 针对表【picture(图片)】的数据库操作Service
* @createDate 2026-03-23 09:38:11
*/
public interface PictureService extends IService<Picture> {

    /**
     * 上传图片  
     *
     * @param inputSource
     * @param pictureUploadRequest
     * @param loginUser
     * @return
     */
    
//    PictureVO uploadPicture(MultipartFile multipartFile,
//                            PictureUploadRequest pictureUploadRequest,
//                            User loginUser);

    PictureVO uploadPicture(Object inputSource, PictureUploadRequest pictureUploadRequest, User loginUser);

    /**
     * 获取查询对象
     *
     * @param pictureQueryRequest
     * @return
     */
    QueryWrapper<Picture> getQueryWrapper(PictureQueryRequest pictureQueryRequest);
    
    /**
     * 获取图片包装类（单条）
     *
     * @param picture
     * @param request
     * @return
     */
    PictureVO getPictureVO(Picture picture, HttpServletRequest request);

    /**
     * 获取图片包装类（分页）
     *
     * @param picturePage
     * @param request
     * @return
     */
    Page<PictureVO> getPictureVOPage(Page<Picture> picturePage, HttpServletRequest request);
    /**
     * 校验图片
     *
     * @param picture
     */
    void validPicture(Picture picture);

    /**
     * 图片审核  
     *
     * @param pictureReviewRequest
     * @param loginUser
     */
    void doPictureReview(PictureReviewRequest pictureReviewRequest, User loginUser);

    /**
     * 设置审核状态
     * @param picture
     * @param loginUser
     */
    void fillReviewParams(Picture picture, User loginUser);

    /**
     * 批量抓取和创建图片  
     *
     * @param pictureUploadByBatchRequest
     * @param loginUser
     * @return 成功创建的图片数
     */
    Integer uploadPictureByBatch(
            PictureUploadByBatchRequest pictureUploadByBatchRequest,
            User loginUser
    );

    /**
     * 空间权限校验
     * @param loginUser
     * @param picture
     */
    void checkPictureAuth(User loginUser, Picture picture);

    /**
     * 删除图片
     * @param id
     * @param loginUser
     */
    void deletePicture(long id, User loginUser);

    @Async
    void clearPictureFile(Picture oldPicture);

    /**
     * 编辑图片
     * @param pictureEditRequest
     * @param loginUser
     */

    void editPicture(PictureEditRequest pictureEditRequest, User loginUser);

    /**
     * 根据颜色搜索图片
     * @param spaceId
     * @param picColor
     * @param loginUser
     * @return
     */
    List<PictureVO> searchPictureByColor(Long spaceId, String picColor, User loginUser);

    /**
     * 批量修改图片信息
     * @param pictureEditByBatchRequest
     * @param loginUser
     */
    @Transactional(rollbackFor = Exception.class)
    void editPictureByBatch(PictureEditByBatchRequest pictureEditByBatchRequest, User loginUser);

    CreateOutPaintingTaskResponse createPictureOutPaintingTask(CreatePictureOutPaintingTaskRequest createPictureOutPaintingTaskRequest, User loginUser);
}
