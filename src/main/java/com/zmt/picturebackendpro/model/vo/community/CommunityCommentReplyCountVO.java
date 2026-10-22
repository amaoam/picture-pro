package com.zmt.picturebackendpro.model.vo.community;

import lombok.Data;

import java.io.Serializable;

/** 批量统计一级评论回复数的 Mapper 投影对象。 */
@Data
public class CommunityCommentReplyCountVO implements Serializable {
    private Long rootId;
    private Long replyCount;
    private static final long serialVersionUID = 1L;
}
