package com.zmt.picturebackendpro.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.zmt.picturebackendpro.model.entity.UserCommunityProfile;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface UserCommunityProfileMapper extends BaseMapper<UserCommunityProfile> {

    @Insert("INSERT IGNORE INTO user_community_profile " +
            "(userId, experience, level, postCount, commentCount, receivedLikeCount, createTime, updateTime) " +
            "VALUES (#{userId}, 0, 1, 0, 0, 0, NOW(), NOW())")
    int insertIfAbsent(@Param("userId") Long userId);

    @Select("SELECT userId, experience, level, postCount, commentCount, receivedLikeCount, createTime, updateTime " +
            "FROM user_community_profile WHERE userId = #{userId} FOR UPDATE")
    UserCommunityProfile selectForUpdate(@Param("userId") Long userId);
}
