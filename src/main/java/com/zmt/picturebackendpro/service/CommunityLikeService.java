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

    /** 默认实现保持旧数据库服务兼容；Redis 版会覆盖该重载并使用调用方提供的 requestId。 */
    default CommunityLikeStatusVO likePost(Long postId, User loginUser, String requestId) {
        return likePost(postId, loginUser);
    }

    default CommunityLikeStatusVO unlikePost(Long postId, User loginUser, String requestId) {
        return unlikePost(postId, loginUser);
    }

    default CommunityLikeStatusVO likePublicPicture(Long pictureId, User loginUser, String requestId) {
        return likePublicPicture(pictureId, loginUser);
    }

    default CommunityLikeStatusVO unlikePublicPicture(Long pictureId, User loginUser, String requestId) {
        return unlikePublicPicture(pictureId, loginUser);
    }
}
