local handoffKey = KEYS[1]
local quarantineKey = KEYS[2]
local member = ARGV[1]
local score = ARGV[2]
local reason = ARGV[3]

if redis.call('ZREM', handoffKey, member) == 0 then
    return 0
end
redis.call('ZADD', quarantineKey, score, handoffKey .. '|' .. member .. '|' .. reason)
return 1
