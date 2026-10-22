package com.zmt.picturebackendpro.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.zmt.picturebackendpro.model.entity.CommunityPostTag;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface CommunityPostTagMapper extends BaseMapper<CommunityPostTag> {

    /** 批量保存帖子标签关系。 */
    @Insert("<script>INSERT INTO community_post_tag (id, postId, tagId, createTime) VALUES "
            + "<foreach collection='list' item='item' separator=','>"
            + "(#{item.id}, #{item.postId}, #{item.tagId}, #{item.createTime})"
            + "</foreach></script>")
    int insertBatch(@Param("list") List<CommunityPostTag> list);
}
