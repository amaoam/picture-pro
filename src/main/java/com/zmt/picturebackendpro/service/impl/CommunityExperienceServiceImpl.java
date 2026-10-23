package com.zmt.picturebackendpro.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.zmt.picturebackendpro.exception.BusinessException;
import com.zmt.picturebackendpro.exception.ErrorCode;
import com.zmt.picturebackendpro.mapper.UserCommunityProfileMapper;
import com.zmt.picturebackendpro.mapper.UserExpRecordMapper;
import com.zmt.picturebackendpro.model.entity.CommunityLikeOutboxEvent;
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
import java.util.*;

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

    /** 缓存解析后的等级阈值，避免每次点赞/经验变更都拆分配置字符串。 */
    private volatile List<Long> cachedLevelThresholds;
    

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

    /** 兼容旧调用：先检查幂等流水，再改获赞数，防止重复调用重复累计统计。新点赞 API 不再调用此方法。 */
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

    /** 兼容旧调用；新点赞链路使用带事件 ID 的 processLikeEvents 实现幂等计数。 */
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

    /**
     * 一批事件中每位作者只锁定、更新一次社区档案。获赞经验按 eventKey 永久去重：
     * 点赞 -> 取消 -> 再点赞，receivedLikeCount 恢复，但同一个目标/用户不会重复获得获赞经验。
     * 不处理图片和帖子的点赞数量更新
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void processLikeEvents(List<CommunityLikeOutboxEvent> events) {
        if (events == null || events.isEmpty()) return;
            
        //被点赞对象和点赞数量map
        Map<Long, Long> receivedLikeDeltas = new TreeMap<>();
        //被点赞对象set
        Set<Long> owners = new TreeSet<>();
        //通过业务键来筛选出需要添加经验的事件
        Map<String, CommunityLikeOutboxEvent> rewardCandidates = new LinkedHashMap<>();

        for (CommunityLikeOutboxEvent event : events) {
            Long ownerId = event.getOwnerUserId();
            if (ownerId != null) {
                owners.add(ownerId);
                receivedLikeDeltas.merge(ownerId, event.getDelta().longValue(), Long::sum);
            }
            if (event.getDelta() != null && event.getDelta() > 0
                    && event.getActorUserId() != null && ownerId != null) {
                String eventKey = likeRewardEventKey(event);
                // 同一批里相同业务事件也只保留一次，避免重复写入唯一键。
                rewardCandidates.putIfAbsent(eventKey, event);
            }
        }

        // 按用户 ID 从小到大加锁，降低多个消费者批量处理时的死锁概率。
        Map<Long, UserCommunityProfile> profiles = new TreeMap<>();
        for (Long ownerId : owners) {
            profiles.put(ownerId, lockOrCreateProfile(ownerId));
        }

        List<String> candidateKeys = new ArrayList<>(rewardCandidates.keySet());
        Set<String> existingKeys = candidateKeys.isEmpty()
                ? Collections.emptySet()
                : new TreeSet<>(expRecordMapper.selectExistingEventKeys(candidateKeys));
        Map<Long, Long> expRewards = new TreeMap<>();
        long rewardPerLike = Math.max(0L, receivedLikeReward);
        //用户经验增加流水记录
        for (Map.Entry<String, CommunityLikeOutboxEvent> candidate : rewardCandidates.entrySet()) {
            if (existingKeys.contains(candidate.getKey())) continue;
            CommunityLikeOutboxEvent event = candidate.getValue();
            boolean isPost = CommunityLikeOutboxEvent.TARGET_POST.equals(event.getTargetType());
            Long sourcePostId = isPost ? event.getTargetId() : null;
            String description = isPost ? "帖子收到有效点赞奖励" : "公共图库图片收到有效点赞奖励，pictureId=" + event.getTargetId();
            // 事件流水、经验值与档案在调用方的 Outbox 消费事务内提交。
            expRecordMapper.insert(buildRecord(event.getOwnerUserId(), EVENT_RECEIVED_LIKE,
                    candidate.getKey(), rewardPerLike, sourcePostId, null, event.getActorUserId(), description));
            expRewards.merge(event.getOwnerUserId(), rewardPerLike, Long::sum);
        }
        //添加用户的点赞数量和经验
        for (Map.Entry<Long, UserCommunityProfile> entry : profiles.entrySet()) {
            Long ownerId = entry.getKey();
            UserCommunityProfile profile = entry.getValue();
            long countDelta = receivedLikeDeltas.getOrDefault(ownerId, 0L);
            long expDelta = expRewards.getOrDefault(ownerId, 0L);
            // 同一批点赞/取消相互抵消且没有新经验时，不执行无意义的 profile UPDATE。
            if (countDelta == 0L && expDelta == 0L) {
                continue;
            }
            profile.setReceivedLikeCount(Math.max(0L, valueOrZero(profile.getReceivedLikeCount()) + countDelta));
            if (expDelta != 0L) applyExperience(profile, expDelta);
            saveProfile(profile);
        }
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

    private String likeRewardEventKey(CommunityLikeOutboxEvent event) {
        if (CommunityLikeOutboxEvent.TARGET_POST.equals(event.getTargetType())) {
            return "POST_RECEIVED_LIKE:" + event.getTargetId() + ":" + event.getActorUserId();
        }
        if (CommunityLikeOutboxEvent.TARGET_PICTURE.equals(event.getTargetType())) {
            return "PICTURE_RECEIVED_LIKE:" + event.getTargetId() + ":" + event.getActorUserId();
        }
        throw new IllegalArgumentException("未知点赞目标类型：" + event.getTargetType());
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
        List<Long> result = cachedLevelThresholds;
        if (result != null) return result;
        synchronized (this) {
            result = cachedLevelThresholds;
            if (result != null) return result;
            List<Long> thresholds = new ArrayList<>();
            if (levelThresholdsText != null) {
                for (String part : levelThresholdsText.split(",")) {
                    try {
                        long value = Long.parseLong(part.trim());
                        if (value >= 0 && (thresholds.isEmpty() || value > thresholds.get(thresholds.size() - 1))) {
                            thresholds.add(value);
                        }
                    } catch (NumberFormatException ignored) {
                        // 非法配置项忽略，最终回退到默认配置。
                    }
                }
            }
            if (thresholds.isEmpty() || thresholds.get(0) != 0L) {
                Collections.addAll(thresholds, 0L, 100L, 300L, 600L, 1000L, 1500L, 2200L, 3000L, 4000L, 5500L);
            }
            cachedLevelThresholds = List.copyOf(thresholds);
            return cachedLevelThresholds;
        }
    }

    private long valueOrZero(Long value) {
        return value == null ? 0L : value;
    }
}
