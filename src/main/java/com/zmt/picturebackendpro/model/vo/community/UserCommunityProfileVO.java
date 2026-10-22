package com.zmt.picturebackendpro.model.vo.community;

import lombok.Data;

import java.io.Serializable;
//用户社区等级视图
@Data
public class UserCommunityProfileVO implements Serializable {
    private Long userId;
    private Long experience;
    private Integer level;
    /** 下一等级所需的累计经验；达到最高配置等级时为空。 */
    private Long nextLevelExperience;
    private Long experienceToNextLevel;
    private Long postCount;
    private Long commentCount;
    private Long receivedLikeCount;
    private static final long serialVersionUID = 1L;
}
