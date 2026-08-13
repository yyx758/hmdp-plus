local recoveryKey = 'seckill:recovery:' .. ARGV[1]
if redis.call('GET', recoveryKey) ~= ARGV[2] then
    return 0
end
return redis.call('EXPIRE', recoveryKey, ARGV[3])
