local stockKey = KEYS[1]
local orderKey = KEYS[2]
local subscriberKey = KEYS[3]
local subscribeQueueKey = KEYS[4]
local subscribeStatusKey = KEYS[5]
local handoffKey = KEYS[6]
local recoveryKey = 'seckill:recovery:' .. ARGV[1]

local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]
local currentTime = ARGV[4]
local subscribeTtlMillis = tonumber(ARGV[5])

if redis.call('EXISTS', recoveryKey) == 1 then
    return 4
end

if redis.call('SISMEMBER', subscriberKey, userId) == 0 then
    redis.call('ZREM', subscribeQueueKey, userId)
    return 3
end

if redis.call('SISMEMBER', orderKey, userId) == 1 then
    redis.call('SREM', subscriberKey, userId)
    redis.call('ZREM', subscribeQueueKey, userId)
    redis.call('HSET', subscribeStatusKey, userId, '2')
    redis.call('PEXPIRE', subscribeStatusKey, subscribeTtlMillis)
    return 2
end

local stock = tonumber(redis.call('GET', stockKey))
if not stock or stock <= 0 then
    return 1
end

redis.call('DECR', stockKey)
redis.call('SADD', orderKey, userId)
redis.call('SREM', subscriberKey, userId)
redis.call('ZREM', subscribeQueueKey, userId)
redis.call('ZADD', handoffKey, currentTime, orderId .. '|' .. userId .. '|1')
return 0
