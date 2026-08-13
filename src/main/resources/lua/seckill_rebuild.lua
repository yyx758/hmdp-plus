local voucherId = ARGV[1]
local recoveryToken = ARGV[2]
local expectedStock = ARGV[3]
local stagingOrderKey = ARGV[4]
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
local recoveryKey = 'seckill:recovery:' .. voucherId

if redis.call('GET', recoveryKey) ~= recoveryToken then
    return 0
end

redis.call('SET', stockKey, expectedStock)
redis.call('DEL', orderKey)
if redis.call('EXISTS', stagingOrderKey) == 1 then
    redis.call('SREM', stagingOrderKey, '__hmdp_rebuild_staging__')
    if redis.call('SCARD', stagingOrderKey) > 0 then
        redis.call('RENAME', stagingOrderKey, orderKey)
    else
        redis.call('DEL', stagingOrderKey)
    end
end
redis.call('DEL', recoveryKey)
return 1
