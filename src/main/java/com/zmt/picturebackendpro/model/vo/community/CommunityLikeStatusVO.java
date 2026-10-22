package com.zmt.picturebackendpro.model.vo.community;

import lombok.Data;

import java.io.Serializable;

/**
 * 当前用户点赞视图
 */
@Data
public class CommunityLikeStatusVO implements Serializable {
    private Long targetId;
    private Long likeCount;
    private Boolean likedByCurrentUser;
    private static final long serialVersionUID = 1L;
}
