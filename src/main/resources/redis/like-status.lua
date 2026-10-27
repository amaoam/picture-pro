-- Atomic status read: avoids obtaining liked and count from two different moments.
-- KEYS: 1 users Set, 2 counter String.
local usersType = redis.call('TYPE', KEYS[1]).ok
local counterType = redis.call('TYPE', KEYS[2]).ok
if usersType ~= 'none' and usersType ~= 'set' then
    return redis.error_reply('LIKE_KEY_TYPE_MISMATCH users')
end
if counterType ~= 'none' and counterType ~= 'string' then
    return redis.error_reply('LIKE_KEY_TYPE_MISMATCH counter')
end
local liked = redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1
local count = tonumber(redis.call('GET', KEYS[2]) or '0')
local relationCount = redis.call('SCARD', KEYS[1])
if not count or count < 0 or count ~= relationCount then
    return cjson.encode({ outcome = 'error', error = 'COUNTER_INVARIANT_BROKEN' })
end
return cjson.encode({ outcome = 'ok', liked = liked, likeCount = count })
