package com.zmt.yupicturebackendtry.model.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableLogic;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * community_post 对应的社区业务实体 社区帖子类
 * 字段名与 community_feature.sql 保持一致。
 */
@Data
@TableName("community_post")
public class CommunityPost implements Serializable {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    /**
     * 实际发帖用户 id，关联 user.id
     */
    private Long userId;
    /**
     * 帖子类型：1-分享帖 2-求助帖
     */
    private Integer postType;
    /**
     * 发布身份：1-个人 2-团队
     */
    private Integer publishType;
    /**
     * 团队发布时的空间 id，关联 space.id
     */
    private Long publishSpaceId;
    /**
     * 发布时个人昵称快照
     */
    private String publishUserNameSnapshot;
    /**
     * 发布时团队名称快照
     */
    private String publishSpaceNameSnapshot;
    /**
     * 发布时团队角色快照：viewer/editor/admin
     */
    private String publishRoleSnapshot;
    /**
     * 帖子标题
     */
    private String title;
    /**
     * 帖子正文
     */
    private String content;
    /**
     * 社区主分类 id
     */
    private Long categoryId;
    /**
     * 状态：0-待审核 1-已发布 2-已拒绝 3-已下架
     */
    private Integer status;
    /**
     * 帖子点赞数冗余统计
     */
    private Long likeCount;
    /**
     * 帖子评论数冗余统计
     */
    private Long commentCount;
    /**
     * 创建时间
     */
    private Date createTime;
    /**
     * 编辑时间
     */
    private Date editTime;
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
