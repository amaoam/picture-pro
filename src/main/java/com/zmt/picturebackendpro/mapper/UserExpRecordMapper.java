package com.zmt.picturebackendpro.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.zmt.picturebackendpro.model.entity.UserExpRecord;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.Date;

@Mapper
public interface UserExpRecordMapper extends BaseMapper<UserExpRecord> {
    @Select("SELECT COALESCE(SUM(changeExp), 0) FROM user_exp_record " +
            "WHERE userId = #{userId} AND eventType = #{eventType} AND eventDate = #{eventDate}")
    Long sumGrantedExperienceByDate(@Param("userId") Long userId,
                                    @Param("eventType") Integer eventType,
                                    @Param("eventDate") Date eventDate);
}
