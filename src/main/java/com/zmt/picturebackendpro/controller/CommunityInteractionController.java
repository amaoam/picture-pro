package com.zmt.picturebackendpro.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.zmt.picturebackendpro.common.BaseResponse;
import com.zmt.picturebackendpro.common.ResultUtils;
import com.zmt.picturebackendpro.exception.BusinessException;
import com.zmt.picturebackendpro.exception.ErrorCode;
import com.zmt.picturebackendpro.model.dto.community.CommunityCommentAddRequest;
import com.zmt.picturebackendpro.model.dto.community.CommunityCommentQueryRequest;
import com.zmt.picturebackendpro.model.entity.User;
import com.zmt.picturebackendpro.model.vo.community.CommunityCommentVO;
import com.zmt.picturebackendpro.model.vo.community.CommunityLikeStatusVO;
import com.zmt.picturebackendpro.service.CommunityInteractionService;
import com.zmt.picturebackendpro.service.UserService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/community")
public class CommunityInteractionController {
    @Resource private CommunityInteractionService interactionService;
    @Resource private UserService userService;
    
    //给帖子点赞
    @PostMapping("/post/{postId}/like")
    public BaseResponse<CommunityLikeStatusVO> likePost(@PathVariable("postId") Long postId,
                                                        HttpServletRequest request) {
        User user = userService.getLoginUser(request);
        return ResultUtils.success(interactionService.likePost(postId, user));
    }
    //给帖子取消点赞
    @DeleteMapping("/post/{postId}/like")
    public BaseResponse<CommunityLikeStatusVO> unlikePost(@PathVariable("postId") Long postId,
                                                          HttpServletRequest request) {
        User user = userService.getLoginUser(request);
        return ResultUtils.success(interactionService.unlikePost(postId, user));
    }
    
    //获取帖子点赞状态
    @GetMapping("/post/{postId}/like/status")
    public BaseResponse<CommunityLikeStatusVO> postLikeStatus(@PathVariable("postId") Long postId,
                                                              HttpServletRequest request) {
        User user = userService.getLoginUser(request);
        return ResultUtils.success(interactionService.getPostLikeStatus(postId, user));
    }
    
    //图片点赞
    @PostMapping("/picture/{pictureId}/like")
    public BaseResponse<CommunityLikeStatusVO> likePublicPicture(@PathVariable("pictureId") Long pictureId,
                                                                 HttpServletRequest request) {
        User user = userService.getLoginUser(request);
        return ResultUtils.success(interactionService.likePublicPicture(pictureId, user));
    }
    
    //图片取消点赞
    @DeleteMapping("/picture/{pictureId}/like")
    public BaseResponse<CommunityLikeStatusVO> unlikePublicPicture(@PathVariable("pictureId") Long pictureId,
                                                                   HttpServletRequest request) {
        User user = userService.getLoginUser(request);
        return ResultUtils.success(interactionService.unlikePublicPicture(pictureId, user));
    }
    
    //查看图片点赞状态
    @GetMapping("/picture/{pictureId}/like/status")
    public BaseResponse<CommunityLikeStatusVO> publicPictureLikeStatus(@PathVariable("pictureId") Long pictureId,
                                                                       HttpServletRequest request) {
        User user = userService.getLoginUser(request);
        return ResultUtils.success(interactionService.getPublicPictureLikeStatus(pictureId, user));
    }
    
    //给帖子评论
    @PostMapping("/post/{postId}/comments")
    public BaseResponse<Long> createComment(@PathVariable("postId") Long postId,
                                            @RequestBody CommunityCommentAddRequest addRequest,
                                            HttpServletRequest request) {
        User user = userService.getLoginUser(request);
        if (addRequest == null) {
            throw new BusinessException(
                    ErrorCode.PARAMS_ERROR, "评论参数不能为空");
        }
        return ResultUtils.success(interactionService.createComment(postId, addRequest.getParentId(),
                addRequest.getContent(), user));
    }
    
    //查看帖子评论  
    /** parentId 为空查询一级评论；非空时传一级评论 ID 查询该线程的回复分页。 */
    @PostMapping("/post/{postId}/comments/page")
    public BaseResponse<Page<CommunityCommentVO>> pageComments(@PathVariable("postId") Long postId,
                                                               @RequestBody(required = false) CommunityCommentQueryRequest queryRequest) {
        return ResultUtils.success(interactionService.pageComments(postId, queryRequest));
    }
    
    //删除帖子评论
    @DeleteMapping("/comment/{commentId}")
    public BaseResponse<Boolean> deleteComment(@PathVariable("commentId") Long commentId,
                                               HttpServletRequest request) {
        User user = userService.getLoginUser(request);
        interactionService.deleteComment(commentId, user);
        return ResultUtils.success(true);
    }
}
