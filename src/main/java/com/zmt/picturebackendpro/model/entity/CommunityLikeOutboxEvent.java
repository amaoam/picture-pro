package com.zmt.picturebackendpro.model.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * 点赞计数事务事件。
 * 点赞关系与本事件必须在同一个 MySQL 事务中写入，确保不会出现关系已变更但事件丢失的情况。
 */
@Data
@TableName("like_event_outbox")
public class CommunityLikeOutboxEvent implements Serializable {

    /**
     * 点赞目标
     */
    public static final String TARGET_POST = "POST";
    public static final String TARGET_PICTURE = "PICTURE";
    
    /**
     * 事件状态
     */
    public static final int STATUS_PENDING = 0;
    public static final int STATUS_PROCESSED = 1;
    public static final int STATUS_DEAD = 2;

    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    /** 点赞目标类型：POST / PICTURE。 */
    private String targetType;

    /** 帖子 ID 或公共图片 ID。 */
    private Long targetId;

    /** 实际触发点赞变更的用户。 */
    private Long actorUserId;

    /** 点赞目标的作者/所有者快照，用于更新获赞统计。 */
    private Long ownerUserId;

    /** +1 表示真实新增点赞关系，-1 表示真实删除点赞关系。 */
    private Integer delta;

    /** 0-待处理，1-已完成，2-多次失败后进入死信状态。 */
    private Integer status;

    /**
     * 尝试次数
     */
    private Integer retryCount;
    /**
     * 下次尝试时间
     */
    private Date nextRetryTime;
    /**
     * 上次错误
     */
    private String lastError;
    /**
     * 创建时间
     */
    private Date createTime;
    /**
     * 执行时间
     */
    private Date processedTime;

    /** SQL 根据数据库 NOW() 计算此值，防止应用服务器与数据库时钟偏差破坏事件顺序。 */
    @TableField(exist = false)
    private Boolean retryDue;

    private static final long serialVersionUID = 1L;
}
