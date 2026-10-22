package com.zmt.picturebackendpro.model.vo.community;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;
//用户评论视图
@Data
public class CommunityCommentVO implements Serializable {
    private Long id;
    private Long postId;
    private Long userId;
    private String userName;
    private String userAvatar;
    private Integer userLevel;
    private Long parentId;
    private Integer level;
    private Long rootId;
    /** 一级评论的有效回复数；回复记录自身为 0。 */
    private Long replyCount;
    private Long replyToUserId;
    private String replyToUserName;
    private String content;
    private Date createTime;
    private static final long serialVersionUID = 1L;
}
