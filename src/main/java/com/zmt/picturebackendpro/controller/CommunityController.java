package com.zmt.picturebackendpro.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.zmt.picturebackendpro.common.BaseResponse;
import com.zmt.picturebackendpro.common.ResultUtils;
import com.zmt.picturebackendpro.exception.BusinessException;
import com.zmt.picturebackendpro.exception.ErrorCode;
import com.zmt.picturebackendpro.model.dto.community.*;
import com.zmt.picturebackendpro.model.entity.CommunityCategory;
import com.zmt.picturebackendpro.model.entity.CommunityTag;
import com.zmt.picturebackendpro.model.entity.User;
import com.zmt.picturebackendpro.model.vo.community.CommunityImageVO;
import com.zmt.picturebackendpro.model.vo.community.CommunityPostVO;
import com.zmt.picturebackendpro.service.CommunityImageService;
import com.zmt.picturebackendpro.service.CommunityPostService;
import com.zmt.picturebackendpro.service.UserService;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * 社区 V1 接口。所有涉及个人数据写入的接口均从现有登录态获取用户，禁止客户端传入 userId。
 */
@RestController
@RequestMapping("/community")
public class CommunityController {

    @Resource
    private CommunityImageService communityImageService;
    @Resource
    private CommunityPostService communityPostService;
    @Resource
    private UserService userService;

    /**
     * 上传社区图片。
     * saveToSpace=false：只存社区资源；saveToSpace=true：先存入指定个人/团队空间，再复制社区公开副本。
     */
    @PostMapping("/image/upload")
    public BaseResponse<CommunityImageVO> uploadCommunityImage(
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "saveToSpace", defaultValue = "false") boolean saveToSpace,
            @RequestParam(value = "spaceId", required = false) Long spaceId,
            HttpServletRequest request) {
        User loginUser = userService.getLoginUser(request);
        CommunityImageVO imageVO = communityImageService.uploadImage(file, spaceId, saveToSpace, loginUser);
        return ResultUtils.success(imageVO);
    }

    /** 选择现有图库图片，服务端校验来源图片权限并复制到独立社区资源路径。 */
    @PostMapping("/image/copy")
    public BaseResponse<CommunityImageVO> copyExistingPicture(@RequestBody CommunityImageCopyRequest copyRequest,
                                                               HttpServletRequest request) {
        if (copyRequest == null || copyRequest.getPictureId() == null) {
            throw new BusinessException(
                    ErrorCode.PARAMS_ERROR, "pictureId 不能为空");
        }
        User loginUser = userService.getLoginUser(request);
        return ResultUtils.success(communityImageService.copyExistingPicture(copyRequest.getPictureId(), loginUser));
    }

    /** 管理员图片审核队列。默认查询待审核图片；status 可传 0/1/2。 */
    @GetMapping("/image/admin/page")
    public BaseResponse<Page<CommunityImageVO>> pageReviewImages(
            @RequestParam(value = "current", defaultValue = "1") long current,
            @RequestParam(value = "pageSize", defaultValue = "10") long pageSize,
            @RequestParam(value = "status", required = false) Integer status,
            HttpServletRequest request) {
        User loginUser = userService.getLoginUser(request);
        return ResultUtils.success(communityImageService.pageReviewImages(current, pageSize, status, loginUser));
    }

    /** 管理员审核社区图片：status=1 通过，status=2 拒绝。 */
    @PostMapping("/image/{id}/review")
    public BaseResponse<Boolean> reviewCommunityImage(@PathVariable("id") Long id,
                                                      @RequestBody CommunityReviewRequest reviewRequest,
                                                      HttpServletRequest request) {
        if (reviewRequest == null) {
            throw new BusinessException(
                    ErrorCode.PARAMS_ERROR, "审核参数不能为空");
        }
        User loginUser = userService.getLoginUser(request);
        communityImageService.reviewImage(id, reviewRequest.getStatus(), reviewRequest.getReviewMessage(), loginUser);
        return ResultUtils.success(true);
    }

    /**
     * 发布帖子
     * @param addRequest
     * @param request
     * @return
     */
    @PostMapping("/post")
    public BaseResponse<Long> createPost(@RequestBody CommunityPostAddRequest addRequest,
                                         HttpServletRequest request) {
        User loginUser = userService.getLoginUser(request);
        return ResultUtils.success(communityPostService.createPost(addRequest, loginUser));
    }

    /** 公开帖子列表：仅返回已审核发布内容。 */
    @PostMapping("/post/page")
    public BaseResponse<Page<CommunityPostVO>> pagePublicPosts(@RequestBody(required = false) CommunityPostQueryRequest queryRequest) {
        return ResultUtils.success(communityPostService.pagePublicPosts(queryRequest));
    }

    /**
     * 根据id获取帖子
     * @param id
     * @return
     */
    @GetMapping("/post/{id}")
    public BaseResponse<CommunityPostVO> getPublicPost(@PathVariable("id") Long id) {
        return ResultUtils.success(communityPostService.getPublicPost(id));
    }

    /** 管理员审核队列。可选 status：0 待审核、1 已发布、2 已拒绝、3 已下架。 */
    @PostMapping("/post/admin/page")
    public BaseResponse<Page<CommunityPostVO>> pageAdminPosts(@RequestBody(required = false) CommunityPostQueryRequest queryRequest,
                                                              HttpServletRequest request) {
        User loginUser = userService.getLoginUser(request);
        return ResultUtils.success(communityPostService.pageAdminPosts(queryRequest, loginUser));
    }

    /** 管理员审核帖子：status=1 发布、2 拒绝、3 下架。 */
    @PostMapping("/post/{id}/review")
    public BaseResponse<Boolean> reviewPost(@PathVariable("id") Long id,
                                            @RequestBody CommunityReviewRequest reviewRequest,
                                            HttpServletRequest request) {
        if (reviewRequest == null) {
            throw new com.zmt.picturebackendpro.exception.BusinessException(
                    com.zmt.picturebackendpro.exception.ErrorCode.PARAMS_ERROR, "审核参数不能为空");
        }
        User loginUser = userService.getLoginUser(request);
        communityPostService.reviewPost(id, reviewRequest.getStatus(), loginUser);
        return ResultUtils.success(true);
    }

    /**
     * 获取分类列表
     * @return
     */
    @GetMapping("/category/list")
    public BaseResponse<List<CommunityCategory>> listCategories() {
        return ResultUtils.success(communityPostService.listCategories());
    }

    @GetMapping("/tag/list")
    public BaseResponse<List<CommunityTag>> listTags(@RequestParam(value = "keyword", required = false) String keyword) {
        return ResultUtils.success(communityPostService.listTags(keyword));
    }

    @PostMapping("/tag")
    public BaseResponse<CommunityTag> createOrGetTag(@RequestBody CommunityTagAddRequest addRequest,
                                                      HttpServletRequest request) {
        if (addRequest == null) {
            throw new BusinessException(
                    ErrorCode.PARAMS_ERROR, "标签参数不能为空");
        }
        User loginUser = userService.getLoginUser(request);
        return ResultUtils.success(communityPostService.createOrGetTag(addRequest.getTagName(), loginUser));
    }
}
