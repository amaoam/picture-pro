package com.zmt.picturebackendpro.model.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.io.Serializable;
import java.util.Date;

/**
 * user_exp_record 对应的社区业务实体  个人经验流水记录
 * 字段名与 community_feature.sql 保持一致。
 */
@Data
@TableName("user_exp_record")
public class UserExpRecord implements Serializable {
    /**
     * id
     */
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    /**
     * 经验归属用户 id
     */
    private Long userId;
    /**
     * 事件类型：0-管理员增加  1-发帖 2-评论 3-获赞 4-经验撤销
     */
    private Integer eventType;
    /**
     * 幂等业务键，全局唯一
     */
    private String eventKey;
    /**
     * 经验变化，正数增加，负数扣减
     */
    private Long changeExp;
    /**
     * 业务发生日期，用于每日限额统计
     */
    private Date eventDate;
    /**
     * 来源帖子 id
     */
    private Long sourcePostId;
    /**
     * 来源评论 id
     */
    private Long sourceCommentId;
    /**
     * 触发获赞的点赞用户 id（仅获赞奖励）
     */
    private Long sourceUserId;
    /**
     * 经验变化说明
     */
    private String description;
    /**
     * 流水创建时间
     */
    private Date createTime;

    private static final long serialVersionUID = 1L;
}
