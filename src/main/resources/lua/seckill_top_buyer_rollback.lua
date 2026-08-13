local dailyKey = KEYS[1]
local userId = ARGV[1]
local score = tonumber(redis.call('ZSCORE', dailyKey, userId))

if not score then
    return 0
end

local newScore = redis.call('ZINCRBY', dailyKey, -1, userId)
if tonumber(newScore) <= 0 then
    redis.call('ZREM', dailyKey, userId)
end
return 1
