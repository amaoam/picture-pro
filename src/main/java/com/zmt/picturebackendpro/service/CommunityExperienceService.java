package com.zmt.picturebackendpro.service;


import com.zmt.picturebackendpro.model.vo.UserCommunityProfileVO;

public interface CommunityExperienceService {
    void recordPostPublished(Long postId, Long authorId);
    void recordCommentCreated(Long commentId, Long postId, Long authorId);
    void recordCommentDeleted(Long commentId, Long postId, Long authorId);
    void recordPostLikeAdded(Long postId, Long likerId, Long authorId);
    void recordPostLikeRemoved(Long authorId);
    void recordPictureLikeAdded(Long pictureId, Long likerId, Long authorId);
    void recordPictureLikeRemoved(Long authorId);
    UserCommunityProfileVO getProfile(Long userId);
}
