package com.zmt.picturebackendpro.model.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * community_post_image 对应的社区业务实体。  社区帖子图片关系类
 * 字段名与 community_feature.sql 保持一致。
 */
@Data
@TableName("community_post_image")
public class CommunityPostImage implements Serializable {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    /**
     * community_post.id
     */
    private Long postId;
    /**
     * community_image.id
     */
    private Long communityImageId;
    /**
     * 图片展示顺序
     */
    private Integer sortOrder;
    /**
     * 创建时间
     */
    private Date createTime;

    private static final long serialVersionUID = 1L;
}
