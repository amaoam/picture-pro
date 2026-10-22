package com.zmt.picturebackendpro.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.zmt.picturebackendpro.model.entity.CommunityPostImage;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface CommunityPostImageMapper extends BaseMapper<CommunityPostImage> {
    /** 批量保存帖子图片关系。SQL 仍位于 createPost 的事务内。 */
    @Insert("<script>INSERT INTO community_post_image (id, postId, communityImageId, sortOrder, createTime) VALUES "
            + "<foreach collection='list' item='item' separator=','>"
            + "(#{item.id}, #{item.postId}, #{item.communityImageId}, #{item.sortOrder}, #{item.createTime})"
            + "</foreach></script>")
    int insertBatch(@Param("list") List<CommunityPostImage> list);
}
