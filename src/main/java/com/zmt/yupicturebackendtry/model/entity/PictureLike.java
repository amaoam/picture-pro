package com.zmt.yupicturebackendtry.model.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * picture_like 对应的社区业务实体 图片点赞类
 * 字段名与 community_feature.sql 保持一致。
 */
@Data
@TableName("picture_like")
public class PictureLike implements Serializable {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    /**
     * 公共图库图片 picture.id
     */
    private Long pictureId;
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
