package com.zmt.picturebackendpro.model.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * community_post_like 对应的社区业务实体。 帖子用户点赞记录
 * 字段名与 community_feature.sql 保持一致。
 */
@Data
@TableName("community_post_like")
public class CommunityPostLike implements Serializable {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    /**
     * community_post.id
     */
    private Long postId;
    /**
     * 点赞用户 id
     */
    private Long userId;
    /**
     * 创建时间
     */
    private Date createTime;

    private static final long serialVersionUID = 1L;
}
