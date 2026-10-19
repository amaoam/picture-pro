package com.zmt.yupicturebackendtry.model.vo.community;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 社区帖子视图
 */
@Data
public class CommunityPostVO implements Serializable {
    private Long id;
    private Long userId;
    private Integer postType;
    private Integer publishType;
    private Long publishSpaceId;
    private String publishUserName;
    private String publishUserAvatar;
    private Integer publishUserLevel;
    private String publishSpaceName;
    private String publishRole;
    private String title;
    private String content;
    private Long categoryId;
    private String categoryName;
    private Integer status;
    private Long likeCount;
    private Long commentCount;
    private Date createTime;
    private Date updateTime;
    private List<CommunityImageVO> images = new ArrayList<>();
    private List<String> tags = new ArrayList<>();

    private static final long serialVersionUID = 1L;
}
