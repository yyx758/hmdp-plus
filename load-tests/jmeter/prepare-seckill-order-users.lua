local attackUserCount = tonumber(ARGV[1])
local legitThreadCount = tonumber(ARGV[2])
local legitIterationsPerThread = tonumber(ARGV[3])
local attackUserIdBase = tonumber(ARGV[4])
local legitUserIdBase = tonumber(ARGV[5])
local ttlSeconds = tonumber(ARGV[6])

for ordinal = 1, attackUserCount do
    local tokenKey = 'login:token:jmeter-order-attack-' .. ordinal
    redis.call('HSET', tokenKey,
            'id', attackUserIdBase + ordinal,
            'nickName', 'order-attack-' .. ordinal,
            'credits', '0',
            'level', '0')
    redis.call('EXPIRE', tokenKey, ttlSeconds)
end

for threadNumber = 0, legitThreadCount - 1 do
    for iteration = 1, legitIterationsPerThread do
        local tokenKey = 'login:token:jmeter-order-legit-'
                .. threadNumber .. '-' .. iteration
        local userOffset = threadNumber * legitIterationsPerThread + iteration
        redis.call('HSET', tokenKey,
                'id', legitUserIdBase + userOffset,
                'nickName', 'order-legit-' .. threadNumber .. '-' .. iteration,
                'credits', '0',
                'level', '0')
        redis.call('EXPIRE', tokenKey, ttlSeconds)
    end
end

return attackUserCount + legitThreadCount * legitIterationsPerThread
