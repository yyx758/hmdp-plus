local acceptedKey = KEYS[1]
local expireKey = KEYS[2]
local now = tonumber(ARGV[1])
local removed = 0
for index = 2, #ARGV do
    local orderId = ARGV[index]
    local expiresAt = redis.call('ZSCORE', expireKey, orderId)
    if expiresAt and tonumber(expiresAt) <= now then
        redis.call('HDEL', acceptedKey, orderId)
        redis.call('ZREM', expireKey, orderId)
        removed = removed + 1
    end
end
return removed
