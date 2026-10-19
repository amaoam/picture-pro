package com.zmt.picturebackendpro.model.dto.community;

import lombok.Data;

import java.io.Serializable;

@Data
public class CommunityReviewRequest implements Serializable {
    /** 审核帖子时：1-发布，2-拒绝，3-下架；审核图片时：1-通过，2-拒绝 */
    private Integer status;
    private String reviewMessage;
    private static final long serialVersionUID = 1L;
}
