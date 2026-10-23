package com.zmt.picturebackendpro.service;


import com.zmt.picturebackendpro.model.entity.CommunityLikeOutboxEvent;
import com.zmt.picturebackendpro.model.vo.community.UserCommunityProfileVO;

import java.util.List;

public interface CommunityExperienceService {
    void recordPostPublished(Long postId, Long authorId);
    void recordCommentCreated(Long commentId, Long postId, Long authorId);
    void recordCommentDeleted(Long commentId, Long postId, Long authorId);

    /** 兼容旧调用，新的点赞链路由 processLikeEvents 批量处理。 */
    void recordPostLikeAdded(Long postId, Long likerId, Long authorId);
    void recordPostLikeRemoved(Long authorId);
    void recordPictureLikeAdded(Long pictureId, Long likerId, Long authorId);
    void recordPictureLikeRemoved(Long authorId);

    /** 在调用方事务中批量维护获赞数量和首次获赞经验奖励。 */
    void processLikeEvents(List<CommunityLikeOutboxEvent> events);

    UserCommunityProfileVO getProfile(Long userId);
}
