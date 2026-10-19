package com.zmt.yupicturebackendtry.model.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableLogic;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * community_comment 对应的社区业务实体。 社区评论实体类
 * 字段名与 community_feature.sql 保持一致。
 */
@Data
@TableName("community_comment")
public class CommunityComment implements Serializable {
    /**
     * id
     */
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    /**
     * community_post.id
     */
    private Long postId;
    /**
     * 评论用户 id  
     */
    private Long userId;
    /**
     * 顶层评论 id；为空表示顶层评论
     */
    private Long parentId;
    /**
     * 被回复的用户 id
     */
    private Long replyToUserId;
    /**
     * 评论内容
     */
    private String content;
    /**
     * 状态：1-正常 2-隐藏
     */
    private Integer status;
    /**
     * 创建时间
     */
    private Date createTime;
    /**
     * 更新时间
     */
    private Date updateTime;
    /**
     * 逻辑删除：0-否 1-是
     */
    @TableLogic(value = "0", delval = "1")
    private Integer isDelete;

    private static final long serialVersionUID = 1L;
}
