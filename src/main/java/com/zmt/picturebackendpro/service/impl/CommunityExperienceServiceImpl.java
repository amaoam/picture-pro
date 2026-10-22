package com.zmt.picturebackendpro.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.zmt.picturebackendpro.exception.BusinessException;
import com.zmt.picturebackendpro.exception.ErrorCode;
import com.zmt.picturebackendpro.mapper.UserCommunityProfileMapper;
import com.zmt.picturebackendpro.mapper.UserExpRecordMapper;
import com.zmt.picturebackendpro.model.entity.User;
import com.zmt.picturebackendpro.model.entity.UserCommunityProfile;
import com.zmt.picturebackendpro.model.entity.UserExpRecord;
import com.zmt.picturebackendpro.model.vo.community.UserCommunityProfileVO;
import com.zmt.picturebackendpro.service.CommunityExperienceService;
import com.zmt.picturebackendpro.service.UserService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

@Service
public class CommunityExperienceServiceImpl implements CommunityExperienceService {

    private static final int EVENT_POST = 1;
    private static final int EVENT_COMMENT = 2;
    private static final int EVENT_RECEIVED_LIKE = 3;
    private static final int EVENT_REVOKE = 4;

    @Value("${community.experience.post-reward:10}")
    private long postReward;
    @Value("${community.experience.comment-reward:2}")
    private long commentReward;
    @Value("${community.experience.received-like-reward:1}")
    private long receivedLikeReward;
    @Value("${community.experience.daily-post-cap:30}")
    private long dailyPostCap;
    @Value("${community.experience.daily-comment-cap:20}")
    private long dailyCommentCap;
    @Value("${community.experience.level-thresholds:0,100,300,600,1000,1500,2200,3000,4000,5500}")
    private String levelThresholdsText;
    @Value("${community.experience.zone-id:Asia/Shanghai}")
    private String zoneIdText;

    @Resource
    private UserCommunityProfileMapper profileMapper;
    @Resource
    private UserExpRecordMapper expRecordMapper;
    @Resource
    private UserService userService;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void recordPostPublished(Long postId, Long authorId) {
        if (postId == null || authorId == null) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "帖子或作者参数不能为空");
        }
        UserCommunityProfile profile = lockOrCreateProfile(authorId);
        String eventKey = "POST_PUBLISH:" + postId;
        if (findRecord(eventKey) != null) {
            return;
        }
        //计算应该获得的经验值
        long reward = calculateCappedReward(authorId, EVENT_POST, postReward, dailyPostCap);
        UserExpRecord record = buildRecord(authorId, EVENT_POST, eventKey, reward, postId, null, null,
                "帖子审核通过奖励");
        expRecordMapper.insert(record);
        profile.setPostCount(valueOrZero(profile.getPostCount()) + 1L);
        applyExperience(profile, reward);
        saveProfile(profile);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void recordCommentCreated(Long commentId, Long postId, Long authorId) {
        if (commentId == null || postId == null || authorId == null) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "评论奖励参数不能为空");
        }
        UserCommunityProfile profile = lockOrCreateProfile(authorId);
        String eventKey = "COMMENT_CREATE:" + commentId;
        if (findRecord(eventKey) != null) {
            return;
        }
        //计算应该获得的经验值
        long reward = calculateCappedReward(authorId, EVENT_COMMENT, commentReward, dailyCommentCap);
        //给记录填充参数
        UserExpRecord record = buildRecord(authorId, EVENT_COMMENT, eventKey, reward, postId, commentId, null,
                "发表评论奖励");
        expRecordMapper.insert(record);
        //更新个人资料
        profile.setCommentCount(valueOrZero(profile.getCommentCount()) + 1L);
        applyExperience(profile, reward);
        saveProfile(profile);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void recordCommentDeleted(Long commentId, Long postId, Long authorId) {
        if (commentId == null || authorId == null) {
            return;
        }
        UserCommunityProfile profile = lockOrCreateProfile(authorId);
        String revokeKey = "COMMENT_REVOKE:" + commentId;
        if (findRecord(revokeKey) != null) {
            return;
        }
        UserExpRecord original = findRecord("COMMENT_CREATE:" + commentId);
        long revokeExp = original == null ? 0L : Math.max(0L, valueOrZero(original.getChangeExp()));
        UserExpRecord revokeRecord = buildRecord(authorId, EVENT_REVOKE, revokeKey, -revokeExp,
                postId, commentId, null, "删除评论，撤销评论经验");
        expRecordMapper.insert(revokeRecord);
        profile.setCommentCount(Math.max(0L, valueOrZero(profile.getCommentCount()) - 1L));
        applyExperience(profile, -revokeExp);
        saveProfile(profile);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void recordPostLikeAdded(Long postId, Long likerId, Long authorId) {
        if (postId == null || likerId == null || authorId == null) {
            return;
        }
        //创建或者获取并锁住个人资料
        UserCommunityProfile profile = lockOrCreateProfile(authorId);
        //  疑似漏洞 这里为什么先加点赞数量然后后面又检验
        //懂了 点赞规则是 第一次点赞之后创建点赞记录 之后取消也不会删 然后经验也是增加的 取消也不会再扣经验了
        profile.setReceivedLikeCount(valueOrZero(profile.getReceivedLikeCount()) + 1L);
        String eventKey = "POST_RECEIVED_LIKE:" + postId + ":" + likerId;
        //检验是否用户已经有过点赞记录
        if (findRecord(eventKey) != null) {
            saveProfile(profile);
            return;
        }
        UserExpRecord record = buildRecord(authorId, EVENT_RECEIVED_LIKE, eventKey,
                Math.max(0L, receivedLikeReward), postId, null, likerId, "帖子收到有效点赞奖励");
        expRecordMapper.insert(record);
        applyExperience(profile, Math.max(0L, receivedLikeReward));
        saveProfile(profile);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void recordPostLikeRemoved(Long authorId) {
        decreaseReceivedLikeCount(authorId);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    //和帖子获赞相同的逻辑
    public void recordPictureLikeAdded(Long pictureId, Long likerId, Long authorId) {
        if (pictureId == null || likerId == null || authorId == null) {
            return;
        }
        UserCommunityProfile profile = lockOrCreateProfile(authorId);
        profile.setReceivedLikeCount(valueOrZero(profile.getReceivedLikeCount()) + 1L);
        String eventKey = "PICTURE_RECEIVED_LIKE:" + pictureId + ":" + likerId;
        if (findRecord(eventKey) != null) {
            saveProfile(profile);
            return;
        }
        UserExpRecord record = buildRecord(authorId, EVENT_RECEIVED_LIKE, eventKey,
                Math.max(0L, receivedLikeReward), null, null, likerId,
                "公共图库图片收到有效点赞奖励，pictureId=" + pictureId);
        expRecordMapper.insert(record);
        applyExperience(profile, Math.max(0L, receivedLikeReward));
        saveProfile(profile);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void recordPictureLikeRemoved(Long authorId) {
        decreaseReceivedLikeCount(authorId);
    }
    
    //获得个人社区资料
    @Override
    public UserCommunityProfileVO getProfile(Long userId) {
        if (userId == null || userId <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "用户 ID 不合法");
        }
        User user = userService.getById(userId);
        if (user == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR, "用户不存在");
        }
        UserCommunityProfile profile = profileMapper.selectById(userId);
        if (profile == null) {
            profile = new UserCommunityProfile();
            profile.setUserId(userId);
            profile.setExperience(0L);
            profile.setLevel(1);
            profile.setPostCount(0L);
            profile.setCommentCount(0L);
            profile.setReceivedLikeCount(0L);
        }
        return toVO(profile);
    }

    private void decreaseReceivedLikeCount(Long authorId) {
        if (authorId == null) {
            return;
        }
        UserCommunityProfile profile = lockOrCreateProfile(authorId);
        profile.setReceivedLikeCount(Math.max(0L, valueOrZero(profile.getReceivedLikeCount()) - 1L));
        saveProfile(profile);
    }
    //加锁或者创建用户资料
    private UserCommunityProfile lockOrCreateProfile(Long userId) {
        // INSERT IGNORE 先保证档案行存在，随后用当前读 FOR UPDATE 串行化同一用户的经验流水。
        profileMapper.insertIfAbsent(userId);
        UserCommunityProfile profile = profileMapper.selectForUpdate(userId);
        if (profile == null) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "初始化社区档案失败");
        }
        return profile;
    }

    private long calculateCappedReward(Long userId, int eventType, long baseReward, long dailyCap) {
        long safeBase = Math.max(0L, baseReward);
        long safeCap = Math.max(0L, dailyCap);
        if (safeBase == 0L || safeCap == 0L) {
            return 0L;
        }
        Date today = Date.valueOf(LocalDate.now(ZoneId.of(zoneIdText)));
        Long granted = expRecordMapper.sumGrantedExperienceByDate(userId, eventType, today);
        long alreadyGranted = granted == null ? 0L : Math.max(0L, granted);
        return Math.min(safeBase, Math.max(0L, safeCap - alreadyGranted));
    }

    private UserExpRecord findRecord(String eventKey) {
        return expRecordMapper.selectOne(new LambdaQueryWrapper<UserExpRecord>()
                .eq(UserExpRecord::getEventKey, eventKey)
                .last("limit 1"));
    }

    private UserExpRecord buildRecord(Long userId, int eventType, String eventKey, long changeExp,
                                      Long postId, Long commentId, Long sourceUserId, String description) {
        UserExpRecord record = new UserExpRecord();
        record.setUserId(userId);
        record.setEventType(eventType);
        record.setEventKey(eventKey);
        record.setChangeExp(changeExp);
        record.setEventDate(Date.valueOf(LocalDate.now(ZoneId.of(zoneIdText))));
        record.setSourcePostId(postId);
        record.setSourceCommentId(commentId);
        record.setSourceUserId(sourceUserId);
        record.setDescription(description);
        record.setCreateTime(new java.util.Date());
        return record;
    }

    private void applyExperience(UserCommunityProfile profile, long changeExp) {
        long experience = Math.max(0L, valueOrZero(profile.getExperience()) + changeExp);
        profile.setExperience(experience);
        profile.setLevel(calculateLevel(experience));
    }

    private void saveProfile(UserCommunityProfile profile) {
        profile.setUpdateTime(new java.util.Date());
        if (profileMapper.updateById(profile) != 1) {
            throw new BusinessException(ErrorCode.OPERATION_ERROR, "更新社区经验档案失败");
        }
    }

    private int calculateLevel(long experience) {
        List<Long> thresholds = getLevelThresholds();
        int level = 1;
        for (int i = 1; i < thresholds.size(); i++) {
            if (experience >= thresholds.get(i)) {
                level = i + 1;
            } else {
                break;
            }
        }
        return level;
    }
    //转化成返回视图
    private UserCommunityProfileVO toVO(UserCommunityProfile profile) {
        List<Long> thresholds = getLevelThresholds();
        long experience = valueOrZero(profile.getExperience());
        int level = calculateLevel(experience);
        UserCommunityProfileVO vo = new UserCommunityProfileVO();
        vo.setUserId(profile.getUserId());
        vo.setExperience(experience);
        vo.setLevel(level);
        vo.setPostCount(valueOrZero(profile.getPostCount()));
        vo.setCommentCount(valueOrZero(profile.getCommentCount()));
        vo.setReceivedLikeCount(valueOrZero(profile.getReceivedLikeCount()));
        if (level < thresholds.size()) {
            long next = thresholds.get(level);
            vo.setNextLevelExperience(next);
            vo.setExperienceToNextLevel(Math.max(0L, next - experience));
        }
        return vo;
    }
    
    //通过配置获取等级梯度
    private List<Long> getLevelThresholds() {
        List<Long> thresholds = new ArrayList<>();
        if (levelThresholdsText != null) {
            String[] parts = levelThresholdsText.split(",");
            for (String part : parts) {
                try {
                    long value = Long.parseLong(part.trim());
                    if (value >= 0 && (thresholds.isEmpty() || value > thresholds.get(thresholds.size() - 1))) {
                        thresholds.add(value);
                    }
                } catch (NumberFormatException ignored) {
                    // 忽略非法项，后面回退到默认阈值。
                }
            }
        }
        if (thresholds.isEmpty() || thresholds.get(0) != 0L) {
            return new ArrayList<Long>() {{
                add(0L); add(100L); add(300L); add(600L); add(1000L);
                add(1500L); add(2200L); add(3000L); add(4000L); add(5500L);
            }};
        }
        return thresholds;
    }

    private long valueOrZero(Long value) {
        return value == null ? 0L : value;
    }
}
