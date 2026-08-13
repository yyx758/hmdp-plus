local threadCount = tonumber(ARGV[1])
local iterationsPerThread = tonumber(ARGV[2])
local userIdBase = tonumber(ARGV[3])
local ttlSeconds = tonumber(ARGV[4])
local tokenPrefix = ARGV[5] or 'capacity-user'
local threadOffset = tonumber(ARGV[6]) or 0

for threadNumber = threadOffset, threadOffset + threadCount - 1 do
    for iteration = 1, iterationsPerThread do
        local tokenKey = 'login:token:' .. tokenPrefix .. '-'
                .. threadNumber .. '-' .. iteration
        local userId = userIdBase + threadNumber * iterationsPerThread + iteration
        redis.call('HSET', tokenKey,
                'id', userId,
                'nickName', 'capacity-' .. userId,
                'credits', '0',
                'level', '0')
        redis.call('EXPIRE', tokenKey, ttlSeconds)
    end
end

return threadCount * iterationsPerThread
