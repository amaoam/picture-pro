package com.zmt.picturebackendpro.model.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * community_post_tag 对应的社区业务实体 帖子标签类
 * 字段名与 community_feature.sql 保持一致。
 */
@Data
@TableName("community_post_tag")
public class CommunityPostTag implements Serializable {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    /**
     * community_post.id
     */
    private Long postId;
    /**
     * community_tag.id
     */
    private Long tagId;
    /**
     * 创建时间
     */
    private Date createTime;

    private static final long serialVersionUID = 1L;
}
