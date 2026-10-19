package com.zmt.yupicturebackendtry.model.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.io.Serializable;
import java.util.Date;

/**
 * user_community_profile 对应的社区业务实体 个人社区资料
 * 字段名与 community_feature.sql 保持一致。
 */
@Data
@TableName("user_community_profile")
public class UserCommunityProfile implements Serializable {
    
    private Long userId;
    /**
     * 当前累计经验值
     */
    private Long experience;
    /**
     * 当前等级，由服务端根据经验阈值计算
     */
    private Integer level;
    /**
     * 有效帖子数统计
     */
    private Long postCount;
    /**
     * 有效评论数统计
     */
    private Long commentCount;
    /**
     * 帖子收到的点赞数统计
     */
    private Long receivedLikeCount;
    /**
     * 创建时间
     */
    private Date createTime;
    /**
     * 更新时间
     */
    private Date updateTime;

    private static final long serialVersionUID = 1L;
}
