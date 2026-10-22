package com.zmt.picturebackendpro.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.zmt.picturebackendpro.model.dto.community.CommunityCommentQueryRequest;
import com.zmt.picturebackendpro.model.entity.User;
import com.zmt.picturebackendpro.model.vo.community.CommunityCommentVO;
import com.zmt.picturebackendpro.model.vo.community.CommunityLikeStatusVO;


public interface CommunityInteractionService {
    CommunityLikeStatusVO likePost(Long postId, User loginUser);
    CommunityLikeStatusVO unlikePost(Long postId, User loginUser);
    CommunityLikeStatusVO getPostLikeStatus(Long postId, User loginUser);

    CommunityLikeStatusVO likePublicPicture(Long pictureId, User loginUser);
    CommunityLikeStatusVO unlikePublicPicture(Long pictureId, User loginUser);
    CommunityLikeStatusVO getPublicPictureLikeStatus(Long pictureId, User loginUser);

    Long createComment(Long postId, Long parentId, String content, User loginUser);
    Page<CommunityCommentVO> pageComments(Long postId, CommunityCommentQueryRequest request);
    void deleteComment(Long commentId, User loginUser);
}
