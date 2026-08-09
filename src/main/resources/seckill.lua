-- 1.参数列表
-- 1.1.优惠券id
local voucherId = ARGV[1]
-- 1.2.用户id
local userId = ARGV[2]
-- 1.3.订单id
local orderId = ARGV[3]
-- 1.4.当前时间戳
local currentTime = tonumber(ARGV[4])
-- 1.5.订单处理结果保留时长
local resultTtlMillis = tonumber(ARGV[5])

-- 2.数据key
-- 2.1.库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2.订单key
local orderKey = 'seckill:order:' .. voucherId
-- 2.3.秒杀券活动信息key
local metaKey = 'seckill:meta:' .. voucherId
-- 2.4.订单处理结果key
local resultKey = 'seckill:order:result:' .. orderId

-- 3.脚本业务
-- 3.1.获取秒杀活动开始时间和结束时间
local beginTime = tonumber(redis.call('hget', metaKey, 'beginTime'))
local endTime = tonumber(redis.call('hget', metaKey, 'endTime'))
local status = tonumber(redis.call('hget', metaKey, 'status'))
-- 3.2.判断秒杀活动配置是否存在
if(not currentTime or not resultTtlMillis or not beginTime or not endTime or not status) then
    -- 3.3.活动配置不存在，返回3
    return 3
end
-- 3.4.判断秒杀活动是否开始
if(currentTime < beginTime) then
    -- 3.5.秒杀尚未开始，返回4
    return 4
end
-- 3.6.判断秒杀活动是否结束
if(currentTime > endTime) then
    -- 3.7.秒杀已经结束，返回5
    return 5
end
-- 3.8.只有上架状态的秒杀券允许下单
if(status ~= 1) then
    return 6
end
-- 3.9.判断库存是否充足 get stockKey
local stock = redis.call('get', stockKey)
if(not stock or tonumber(stock) <= 0) then
    -- 3.10.库存不足，返回1
    return 1
end
-- 3.11.判断用户是否下单 SISMEMBER orderKey userId
if(redis.call('sismember', orderKey, userId) == 1) then
    -- 3.12.存在，说明是重复下单，返回2
    return 2
end
-- 3.13.扣库存 incrby stockKey -1
redis.call('incrby', stockKey, -1)
-- 3.14.下单（保存用户）sadd orderKey userId
redis.call('sadd', orderKey, userId)
-- 3.15.发送消息到队列中， XADD stream.orders * k1 v1 k2 v2 ...
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)
-- 3.16.记录异步订单的初始处理状态
redis.call('hset', resultKey,
        'orderId', orderId,
        'userId', userId,
        'voucherId', voucherId,
        'status', 'PROCESSING',
        'message', '抢购请求已受理，订单正在处理中',
        'updatedAt', currentTime)
-- 3.17.设置处理结果过期时间，避免状态key无限增长
redis.call('pexpire', resultKey, resultTtlMillis)
return 0
