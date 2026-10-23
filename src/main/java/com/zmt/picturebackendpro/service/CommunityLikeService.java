package com.zmt.picturebackendpro.service;

import com.zmt.picturebackendpro.model.entity.User;
import com.zmt.picturebackendpro.model.vo.community.CommunityLikeStatusVO;

/** 帖子和公共图库图片点赞入口。评论业务仍保留在 CommunityInteractionService。 */
public interface CommunityLikeService {
    CommunityLikeStatusVO likePost(Long postId, User loginUser);
    CommunityLikeStatusVO unlikePost(Long postId, User loginUser);
    CommunityLikeStatusVO getPostLikeStatus(Long postId, User loginUser);

    CommunityLikeStatusVO likePublicPicture(Long pictureId, User loginUser);
    CommunityLikeStatusVO unlikePublicPicture(Long pictureId, User loginUser);
    CommunityLikeStatusVO getPublicPictureLikeStatus(Long pictureId, User loginUser);
}
