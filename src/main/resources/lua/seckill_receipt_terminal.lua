local acceptedKey = KEYS[1]
local expireKey = KEYS[2]
local userIndexKey = KEYS[3]
local auditKey = KEYS[4]
local orderId = ARGV[1]
local userId = ARGV[2]
local voucherId = ARGV[3]
local status = ARGV[4]
local expiresAt = tonumber(ARGV[5])

local receipt = redis.call('HGET', acceptedKey, orderId)
if not receipt then
    return 0
end
local prefix = userId .. '|' .. voucherId
if receipt ~= prefix and string.sub(receipt, 1, #prefix + 1) ~= prefix .. '|' then
    return -1
end

local parts = {}
for part in string.gmatch(receipt, '[^|]+') do
    parts[#parts + 1] = part
end
local acceptedAt = parts[3] or '0'
-- A delayed duplicate delivery cannot undo a committed cancellation.
if parts[4] == 'CANCELLED' and status == 'SUCCESS' then
    return 0
end
redis.call('HSET', acceptedKey, orderId,
        userId .. '|' .. voucherId .. '|' .. acceptedAt .. '|' .. status)
redis.call('ZADD', expireKey, expiresAt, orderId)
redis.call('ZREM', auditKey, orderId)
if status == 'CANCELLED' and redis.call('HGET', userIndexKey, userId) == orderId then
    redis.call('HDEL', userIndexKey, userId)
end
return 1
