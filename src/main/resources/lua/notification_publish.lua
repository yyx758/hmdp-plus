local dedupKey = KEYS[1]
local detailKey = KEYS[2]
local inboxKey = KEYS[3]
local unreadKey = KEYS[4]

local id = ARGV[1]
local notificationType = ARGV[2]
local title = ARGV[3]
local content = ARGV[4]
local voucherId = ARGV[5]
local createdAt = ARGV[6]
local dedupMillis = tonumber(ARGV[7])
local retentionMillis = tonumber(ARGV[8])
local maxItems = tonumber(ARGV[9])

if not redis.call('SET', dedupKey, '1', 'NX', 'PX', dedupMillis) then
    return 0
end

redis.call('HSET', detailKey,
        'id', id,
        'type', notificationType,
        'title', title,
        'content', content,
        'voucherId', voucherId,
        'read', '0',
        'createTime', createdAt)
redis.call('PEXPIRE', detailKey, retentionMillis)
redis.call('ZADD', inboxKey, createdAt, id)
redis.call('PEXPIRE', inboxKey, retentionMillis)
redis.call('SADD', unreadKey, id)
redis.call('PEXPIRE', unreadKey, retentionMillis)

local overflow = redis.call('ZCARD', inboxKey) - maxItems
if overflow > 0 then
    local expiredIds = redis.call('ZRANGE', inboxKey, 0, overflow - 1)
    for _, expiredId in ipairs(expiredIds) do
        redis.call('ZREM', inboxKey, expiredId)
        redis.call('SREM', unreadKey, expiredId)
    end
end

return 1
