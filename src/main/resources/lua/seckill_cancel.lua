local stockKey = KEYS[1]
local orderKey = KEYS[2]
local userId = ARGV[1]

if redis.call('EXISTS', stockKey) == 0 then
    return -1
end

if redis.call('SISMEMBER', orderKey, userId) ~= 1 then
    return -2
end
local removed = redis.call('SREM', orderKey, userId)
redis.call('INCR', stockKey)
return removed
