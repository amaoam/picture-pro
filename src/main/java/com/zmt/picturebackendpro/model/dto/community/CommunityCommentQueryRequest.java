package com.zmt.picturebackendpro.model.dto.community;

import lombok.Data;

import java.io.Serializable;

/**
 * 帖子评论查询请求
 */
@Data
public class CommunityCommentQueryRequest implements Serializable {
    private Long postId;
    /** 为空表示顶层评论；有值时只能回复顶层评论，暂不支持无限层级。 */
    private Long parentId;
    private long current = 1;
    private long pageSize = 20;
    private static final long serialVersionUID = 1L;
}
