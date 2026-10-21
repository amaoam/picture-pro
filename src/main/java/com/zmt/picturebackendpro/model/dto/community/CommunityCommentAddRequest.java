package com.zmt.picturebackendpro.model.dto.community;

import lombok.Data;

import java.io.Serializable;

/**
 * 评论添加请求
 */
@Data
public class CommunityCommentAddRequest implements Serializable {
    /** 为空表示顶层评论；有值时只能回复顶层评论，暂不支持无限层级。 */
    private Long parentId;
    private String content;
    private static final long serialVersionUID = 1L;
}
