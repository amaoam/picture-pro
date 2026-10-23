package com.zmt.picturebackendpro.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.zmt.picturebackendpro.model.entity.UserExpRecord;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.Date;
import java.util.List;

@Mapper
public interface UserExpRecordMapper extends BaseMapper<UserExpRecord> {
    /**
     * 按日期统计某用户特定事件类型已发放的经验值总额
     *
     * <p><strong>典型应用场景：</strong>
     * <ul>
     *   <li>用于校验或限制用户每日某类行为（如点赞、发帖等）所能获得的经验上限（Daily Cap），防止通过脚本恶意刷取经验。</li>
     *   <li>使用 {@code COALESCE(SUM(changeExp), 0)} 确保在没有匹配记录时返回 {@code 0L} 而不是 {@code null}，直接进行数值比较时可有效避免拆箱抛出 {@code NullPointerException}。</li>
     * </ul>
     *
     * @param userId    用户 ID
     * @param eventType 事件类型（如：1-点赞、2-发帖、3-评论等）
     * @param eventDate 目标统计日期（通常对应流水记录中的日期字段）
     * @return 当天该事件类型已累计发放的经验值总额
     */
    @Select("SELECT COALESCE(SUM(changeExp), 0) FROM user_exp_record " +
            "WHERE userId = #{userId} AND eventType = #{eventType} AND eventDate = #{eventDate}")
    Long sumGrantedExperienceByDate(@Param("userId") Long userId,
                                    @Param("eventType") Integer eventType,
                                    @Param("eventDate") Date eventDate);

    /**
     * 批量查询已发放过奖励的事件键集合（用于幂等性防重过滤）
     *
     * <p><strong>核心优势：</strong>
     * <ul>
     *   <li>利用 MyBatis 的 {@code <foreach>} 动态生成 {@code IN} 子句，支持对一个批次的事件键进行一次性批量过滤。</li>
     *   <li>彻底避免了消费者在批量处理事件时循环执行单条 SQL 查询造成的 N+1 性能问题，极大提升了消费吞吐量。</li>
     * </ul>
     *
     * @param eventKeys 待校验的事件唯一业务键列表（Event Key，如业务消息的全局唯一 ID）
     * @return 数据库中已经存在的事件键列表（用于在内存中快速排除重复事件）
     */
    @Select({"<script>",
            "SELECT eventKey FROM user_exp_record WHERE eventKey IN",
            "<foreach collection='eventKeys' item='eventKey' open='(' separator=',' close=')'>#{eventKey}</foreach>",
            "</script>"})
    List<String> selectExistingEventKeys(@Param("eventKeys") List<String> eventKeys);

}
