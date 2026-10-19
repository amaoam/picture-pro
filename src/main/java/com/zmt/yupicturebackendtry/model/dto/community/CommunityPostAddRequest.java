package com.zmt.yupicturebackendtry.model.dto.community;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/** 创建社区帖子请求。userId 与发布者快照一律由服务端确定。 */
@Data
public class CommunityPostAddRequest implements Serializable {
    /** 1-分享帖，2-求助帖 */
    private Integer postType;
    /** 1-个人身份，2-团队身份 */
    private Integer publishType;
    /** publishType=2 时必填 */
    private Long publishSpaceId;
    private String title;
    private String content;
    private Long categoryId;
    /** community_image.id 列表，按展示顺序传入 */
    private List<Long> imageIds;
    /** community_tag.id 列表 */
    private List<Long> tagIds;

    private static final long serialVersionUID = 1L;
}
