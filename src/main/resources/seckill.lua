-- 1.参数列表
-- 1.1.优惠券id
local voucherId = ARGV[1]
-- 1.2.用户id
local userId = ARGV[2]
-- 1.3.订单id
local orderId = ARGV[3]
-- 1.4.当前时间戳
local currentTime = tonumber(ARGV[4])
-- 1.5.前置资格令牌
local accessToken = ARGV[5]
-- 1.6.是否启用资格令牌
local accessTokenEnabled = ARGV[6] == '1'

-- 2.数据key
-- 2.1.库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2.订单key
local orderKey = 'seckill:order:' .. voucherId
-- 2.3.秒杀券活动信息key
local metaKey = 'seckill:meta:' .. voucherId
-- 2.4.资格令牌key
local accessTokenKey = 'seckill:access:token:{' .. voucherId .. '}:' .. userId
-- 2.5.Redis到MySQL Outbox的临时交接队列
local handoffKey = 'seckill:order:handoff:{' .. voucherId .. '}'
local recoveryKey = 'seckill:recovery:' .. voucherId
local acceptedKey = 'seckill:order:accepted'

-- 恢复过程中不接受新预扣，避免数据库快照与新请求交叉。
if(redis.call('exists', recoveryKey) == 1) then
    return 8
end

-- 3.脚本业务
-- 3.1.获取秒杀活动开始时间和结束时间
local beginTime = tonumber(redis.call('hget', metaKey, 'beginTime'))
local endTime = tonumber(redis.call('hget', metaKey, 'endTime'))
local status = tonumber(redis.call('hget', metaKey, 'status'))
-- 3.2.判断秒杀活动配置是否存在
if(not currentTime or not beginTime or not endTime or not status) then
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
-- 3.13.资格令牌必须和预扣处于同一个Lua原子边界，避免令牌先被消费而预扣未发生
if accessTokenEnabled then
    local storedToken = redis.call('get', accessTokenKey)
    if not storedToken or storedToken ~= accessToken then
        return 7
    end
end
-- 3.14.消费资格令牌
if accessTokenEnabled then
    redis.call('del', accessTokenKey)
end
-- 3.15.扣库存 incrby stockKey -1
redis.call('incrby', stockKey, -1)
-- 3.16.下单（保存用户）sadd orderKey userId
redis.call('sadd', orderKey, userId)
-- 3.17.只写一条临时Handoff；Outbox提交后删除，不再维护Stream和Redis PROCESSING状态
local handoffMember = orderId .. '|' .. userId .. '|0'
redis.call('zadd', handoffKey, currentTime, handoffMember)
-- 对外已提示抢券成功时，Outbox 可能尚未创建；该短期凭证供内部状态查询校验订单归属。
-- Handoff 转存 Outbox 后立即 HDEL，避免为高峰订单创建海量独立 key。
redis.call('hset', acceptedKey, orderId, userId .. '|' .. voucherId)
return 0
