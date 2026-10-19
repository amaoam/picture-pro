package com.zmt.yupicturebackendtry.model.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableLogic;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * community_image 对应的社区业务实体。  社区图片类
 * 字段名与 community_feature.sql 保持一致。
 */
@Data
@TableName("community_image")
public class CommunityImage implements Serializable {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    /**
     * 创建/上传该社区图片的用户 id
     */
    private Long ownerUserId;
    /**
     * 来源：1-选择现有图库图片 2-新上传
     */
    private Integer sourceType;
    /**
     * 选择现有图库图片时的源 picture.id
     */
    private Long sourcePictureId;
    /**
     * 新上传图片同时存入空间时的 picture.id
     */
    private Long savedPictureId;
    /**
     * 图片名称
     */
    private String name;
    /**
     * 社区公开副本 URL
     */
    private String url;
    /**
     * 社区公开副本缩略图 URL
     */
    private String thumbnailUrl;
    /**
     * 社区公开副本 COS object key
     */
    private String storageKey;
    /**
     * 社区缩略图 COS object key
     */
    private String thumbnailStorageKey;
    /**
     * 图片大小（字节） 
     */
    private Long picSize;
    /**
     * 图片宽度
     */
    private Integer picWidth;
    /**
     * 图片高度
     */
    private Integer picHeight;
    /**
     * 图片格式
     */
    private String picFormat;
    /**
     * 审核状态：0-待审核 1-通过 2-拒绝
     */
    private Integer reviewStatus;
    /**
     * 审核意见
     */
    private String reviewMessage;
    /**
     * 审核人 user.id
     */
    private Long reviewerId;
    /**
     * 审核时间
     */
    private Date reviewTime;
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
