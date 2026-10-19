package com.zmt.picturebackendpro.model.dto.community;

import com.zmt.picturebackendpro.common.PageRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * 社区帖子查询请求
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class CommunityPostQueryRequest extends PageRequest implements Serializable {
    /** 1-分享帖，2-求助帖 */
    private Integer postType;
    /** 社区类别id */
    private Long categoryId;
    /** 社区标签id */
    private Long tagId;
    /**
     * 搜索词
     */
    private String searchText;
    /** 仅管理员审核队列使用，公开列表会忽略该字段 */
    private Integer status;

    private static final long serialVersionUID = 1L;
}
