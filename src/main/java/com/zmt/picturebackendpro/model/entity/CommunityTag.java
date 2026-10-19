package com.zmt.picturebackendpro.model.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableLogic;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * community_tag 对应的社区业务实体。  社区标签实体类
 * 字段名与 community_feature.sql 保持一致。
 */
@Data
@TableName("community_tag")
public class CommunityTag implements Serializable {
    /**
     * id
     */
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    /**
     * 标签名称（服务端 trim 后存储）
     */
    private String tagName;
    /**
     * 使用次数冗余统计，可后续启用
     */
    private Long useCount;
    /**
     * 状态：1-启用 0-停用
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
