local recordedKey = KEYS[1]
local dailyKey = KEYS[2]
local userId = ARGV[1]
local ttlMillis = tonumber(ARGV[2])

if not redis.call('SET', recordedKey, '1', 'NX', 'PX', ttlMillis) then
    return 0
end

redis.call('ZINCRBY', dailyKey, 1, userId)
redis.call('PEXPIRE', dailyKey, ttlMillis)
return 1
