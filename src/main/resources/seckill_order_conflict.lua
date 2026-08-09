-- 1.参数列表
-- 1.1.优惠券id
local voucherId = ARGV[1]
-- 1.2.用户id
local userId = ARGV[2]
-- 1.3.本次请求生成的订单id
local currentOrderId = ARGV[3]
-- 1.4.数据库中已经存在的订单id
local existingOrderId = ARGV[4]
-- 1.5.原始Stream消息id
local messageId = ARGV[5]
-- 1.6.当前时间戳
local currentTime = ARGV[6]
-- 1.7.订单处理结果保留时长
local resultTtlMillis = tonumber(ARGV[7])

-- 2.数据key
-- 2.1.库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2.一人一单标记key
local orderKey = 'seckill:order:' .. voucherId
-- 2.3.订单处理结果key
local resultKey = 'seckill:order:result:' .. currentOrderId
-- 2.4.补偿幂等key
local compensatedKey = 'seckill:order:compensated:' .. voucherId

-- 3.脚本业务
-- 3.1.库存key丢失时无法安全补偿，交给人工核对
if(redis.call('exists', stockKey) == 0) then
    return -1
end
-- 3.2.同一个错误订单只允许回补一次Redis库存
local firstCompensation = redis.call('sadd', compensatedKey, currentOrderId)
if(firstCompensation == 1) then
    redis.call('incrby', stockKey, 1)
    redis.call('xadd', 'stream.orders.conflict', '*',
            'userId', userId,
            'voucherId', voucherId,
            'currentOrderId', currentOrderId,
            'existingOrderId', existingOrderId,
            'originalMessageId', messageId,
            'compensatedAt', currentTime)
end
-- 3.3.恢复一人一单标记，防止用户继续产生错误请求
redis.call('sadd', orderKey, userId)
-- 3.4.记录最终状态，并把数据库中的真实订单id返回给用户
redis.call('hset', resultKey,
        'orderId', currentOrderId,
        'userId', userId,
        'voucherId', voucherId,
        'existingOrderId', existingOrderId,
        'status', 'DUPLICATE_EXISTING',
        'message', '检测到你已购买该秒杀券，已恢复原订单',
        'updatedAt', currentTime)
redis.call('pexpire', resultKey, resultTtlMillis)
-- 3.5.补偿记录保留7天，避免Pending重复消费造成重复回补
redis.call('pexpire', compensatedKey, 604800000)
-- 3.6.状态和补偿完成后再确认原消息
redis.call('xack', 'stream.orders', 'g1', messageId)
redis.call('hdel', 'stream.orders:retry', messageId)
return firstCompensation
