package com.zmt.picturebackendpro.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.zmt.picturebackendpro.model.entity.CommunityComment;
import com.zmt.picturebackendpro.model.vo.community.CommunityCommentReplyCountVO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface CommunityCommentMapper extends BaseMapper<CommunityComment> {

    /**
     * 一次性统计当前页一级评论的回复数，避免按一级评论逐条执行 COUNT(*)。
     * 只统计正常且未逻辑删除的回复。
     */
    @Select("<script>SELECT rootId AS rootId, COUNT(*) AS replyCount "
            + "FROM community_comment WHERE rootId IN "
            + "<foreach collection='rootIds' item='rootId' open='(' separator=',' close=')'>#{rootId}</foreach> "
            + "AND level = 1 AND status = 1 AND isDelete = 0 GROUP BY rootId</script>")
    List<CommunityCommentReplyCountVO> selectReplyCounts(@Param("rootIds") List<Long> rootIds);
}
