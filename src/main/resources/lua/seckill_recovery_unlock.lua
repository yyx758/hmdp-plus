local recoveryKey = 'seckill:recovery:' .. ARGV[1]
if redis.call('GET', recoveryKey) == ARGV[2] then
    return redis.call('DEL', recoveryKey)
end
return 0
