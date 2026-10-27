-- =========================================================================
-- V3 LIKE / UNLIKE 核心原子命令 Lua 脚本
-- 职责：在 Redis 端原子性地完成点赞/取消点赞的校验、状态更新、计数维护、幂等控制与事件追加。
-- =========================================================================

-- KEYS 定义：
-- KEYS[1]: 点赞用户集合 (Set)，存储所有点赞了该目标的 userId
-- KEYS[2]: 点赞总数 (String)，存储目标的实时点赞总数
-- KEYS[3]: 事件流 (Stream)，记录点赞状态变更事件，供异步消费者落地到 MySQL
-- KEYS[4]: 幂等键 (String)，用于防止客户端重复提交或请求 ID 滥用
-- KEYS[5]: 版本控制哈希 (Hash)，记录每个用户与目标点赞关系的对齐版本号
-- 注意：所有 Key 必须带有相同的 Redis Cluster Hash Tag（如 {like-XX}），以确保落入同一个 Slot。
local expectedTypes = { 'set', 'string', 'stream', 'string', 'hash' }
for index = 1, 5 do
    local actualType = redis.call('TYPE', KEYS[index]).ok
    if actualType ~= 'none' and actualType ~= expectedTypes[index] then
        -- 严格的类型保护：防止因数据结构错乱导致 Redis 命令报错或内存结构损坏
        return redis.error_reply('LIKE_KEY_TYPE_MISMATCH key=' .. KEYS[index] .. ' actual=' .. actualType)
    end
end

-- ARGV 参数解析：
local userId = ARGV[1]              -- 操作用户 ID
local desiredState = ARGV[2]        -- 期望状态：'LIKE'（点赞）或 'UNLIKE'（取消点赞）
local eventId = ARGV[3]             -- 全局唯一事件 ID（由 Java 端生成，供下游 MySQL 账本去重）
local requestId = ARGV[4]           -- 客户端请求唯一标识（用于幂等）
local targetType = ARGV[5]          -- 目标类型（如 POST、PICTURE）
local targetId = ARGV[6]            -- 目标对象 ID
local ownerUserId = ARGV[7]         -- 目标所有者用户 ID（用于消息通知等）
local createdAtMs = ARGV[8]         -- 事件发生的时间戳（毫秒）
local idempotencyTtl = tonumber(ARGV[9]) -- 幂等键的过期时间（秒）

-- =========================================================================
-- 1. 基础输入参数校验
-- =========================================================================
if not userId or userId == '' or not string.match(userId, '^%d+$') then
    return redis.error_reply('LIKE_INVALID_USER_ID')
end
if desiredState ~= 'LIKE' and desiredState ~= 'UNLIKE' then
    return redis.error_reply('LIKE_INVALID_DESIRED_STATE')
end
if not idempotencyTtl or idempotencyTtl < 1 then
    return redis.error_reply('LIKE_INVALID_IDEMPOTENCY_TTL')
end

-- =========================================================================
-- 2. 幂等控制与重试处理
-- =========================================================================
-- 相同 requestId 必须同时绑定用户和期望状态；严禁拿同一个请求 ID 重放另一条完全不同的命令。
local expectedIntent = userId .. '|' .. desiredState
local existingIntent = redis.call('GET', KEYS[4])
if existingIntent then
    -- 如果请求 ID 相同，但意图（用户或期望状态）发生变化，判定为冲突错误
    if existingIntent ~= expectedIntent then
        return cjson.encode({ outcome = 'error', error = 'REQUEST_ID_REUSED' })
    end
    
    -- 若是合法的幂等重试：只返回当前状态，绝对不再次追加 Stream 事件或改变点赞计数。
    local currentLiked = redis.call('SISMEMBER', KEYS[1], userId) == 1
    local currentCount = tonumber(redis.call('GET', KEYS[2]) or '0')
    
    -- 安全校验：检查当前计数器与 Set 真实数量是否一致，若遭破坏则报错中断
    if not currentCount or currentCount < 0 or currentCount ~= redis.call('SCARD', KEYS[1]) then
        return cjson.encode({ outcome = 'error', error = 'COUNTER_INVARIANT_BROKEN' })
    end
    
    return cjson.encode({ outcome = 'duplicate', duplicate = true, changed = false,
        liked = currentLiked, likeCount = currentCount, eventId = '', pairVersion = 0 })
end

-- =========================================================================
-- 3. 数据一致性校验（Fail-Fast 策略）
-- =========================================================================
local wasLiked = redis.call('SISMEMBER', KEYS[1], userId) == 1
local wantLiked = desiredState == 'LIKE'
local currentCount = tonumber(redis.call('GET', KEYS[2]) or '0')
if not currentCount then
    return cjson.encode({ outcome = 'error', error = 'COUNTER_NOT_NUMERIC' })
end

-- Redis Set 的 SCARD 命令复杂度为 O(1)。
-- 实时校验计数器与用户集合的实际基数是否相符，若发现历史迁移或部分写入异常，立即停止继续扩大偏差。
local relationCount = redis.call('SCARD', KEYS[1])
if currentCount < 0 or currentCount ~= relationCount then
    return cjson.encode({ outcome = 'error', error = 'COUNTER_INVARIANT_BROKEN' })
end

-- =========================================================================
-- 4. 核心状态变更与事件投递
-- =========================================================================
local changed = false
local delta = 0
-- 获取当前用户点赞对的版本号（用于乐观锁/状态对齐）
local pairVersion = tonumber(redis.call('HGET', KEYS[5], userId) or '0')

-- 仅当“当前状态”与“期望状态”不一致时，才真正执行写操作
if wasLiked ~= wantLiked then
    changed = true
    if wantLiked then
        redis.call('SADD', KEYS[1], userId) -- 将用户加入点赞集合
        delta = 1
    else
        redis.call('SREM', KEYS[1], userId) -- 将用户移出点赞集合
        delta = -1
    end
    
    -- 原子更新点赞计数器和版本号
    currentCount = redis.call('INCRBY', KEYS[2], delta)
    pairVersion = redis.call('HINCRBY', KEYS[5], userId, 1)

    -- 将点赞变更事件追加到 Redis Stream 中；事件仅在关系真实发生变化时产生。
    redis.call('XADD', KEYS[3], '*',
        'eventId', eventId,
        'requestId', requestId,
        'targetType', targetType,
        'targetId', targetId,
        'userId', userId,
        'ownerUserId', ownerUserId,
        'desiredState', desiredState,
        'delta', tostring(delta),
        'pairVersion', tostring(pairVersion),
        'createdAtMs', createdAtMs)
end

-- =========================================================================
-- 5. 写入幂等标记并返回结果
-- =========================================================================
-- 即使是 no-op（如已经点过赞又重复发送 LIKE 请求）也写入幂等键，避免同一命令在重试窗口内产生语义分叉。
redis.call('SET', KEYS[4], expectedIntent, 'EX', idempotencyTtl)

-- 通过 JSON 返回执行结果给 Java 层
return cjson.encode({ 
    outcome = changed and 'changed' or 'noop', 
    duplicate = false,
    changed = changed, 
    liked = wantLiked, 
    likeCount = currentCount,
    eventId = changed and eventId or '', 
    pairVersion = pairVersion 
})