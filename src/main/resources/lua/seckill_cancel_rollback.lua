local stockKey = KEYS[1]
local orderKey = KEYS[2]
local userId = ARGV[1]

local stock = tonumber(redis.call('GET', stockKey))
if not stock or stock <= 0 then
    return -1
end

redis.call('DECR', stockKey)
redis.call('SADD', orderKey, userId)
return 1
