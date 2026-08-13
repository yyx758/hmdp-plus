--接收三个层面的限流约束
local ipBucketKey = KEYS[1]
local userBucketKey = KEYS[2]
local policyKey = KEYS[3]
--IP 限流时间窗口 1000
local defaultIpWindowMillis = tonumber(ARGV[1])
--IP 限流容量 200
local defaultIpCapacity = tonumber(ARGV[2])
--用户 限流时间窗口 1000
local defaultUserWindowMillis = tonumber(ARGV[3])
--用户 限流容量  2
local defaultUserCapacity = tonumber(ARGV[4])
--用户 容量倍率 2
local userCapacityMultiplier = tonumber(ARGV[5]) or 1
-- 下游积压反馈倍率，NORMAL=1，WARNING/CRITICAL 会逐级收紧入口。
local admissionMultiplier = tonumber(ARGV[6]) or 1
-- 允许
local CODE_ALLOWED = 0
-- ip超出限流
local CODE_IP_EXCEEDED = 10007
-- 用户超出限流
local CODE_USER_EXCEEDED = 10008
--[[
*Redis Hash结构, key: field:value
*key是 seckill:rate:policy:{vocherId}:scene对应的是hash类型:
类似:
┌───────────────────┬──────┐
│ ipWindowMillis    │ 500  │
│ ipCapacity        │ 100  │
│ userWindowMillis  │ 1000 │
│ userCapacity      │ 3    │
└───────────────────┴──────┘
*传入的两个参数 一个是维度类型,一个是默认值
*从Redis里获取时间窗口或者容量,要是没有就用默认值
]]
local function positivePolicy(field, fallback)
    local value = tonumber(redis.call('HGET', policyKey, field))
    if value and value > 0 then
        return value
    end
    return fallback
end

local ipWindowMillis = positivePolicy('ipWindowMillis', defaultIpWindowMillis)
local ipCapacity = positivePolicy('ipCapacity', defaultIpCapacity)
local userWindowMillis = positivePolicy('userWindowMillis', defaultUserWindowMillis)
local userCapacity = positivePolicy('userCapacity', defaultUserCapacity)
ipCapacity = math.max(1, math.floor(ipCapacity * math.max(0.01, admissionMultiplier)))
userCapacity = math.max(1, math.floor(userCapacity * math.max(0.01, admissionMultiplier)))
--用户容量是动态的,不同身份,不同消费水平都不一样,需要重新计算一次
userCapacity = math.max(1, math.floor(userCapacity * math.max(1, userCapacityMultiplier)))

--获取当前时间
local redisTime = redis.call('TIME')
--这是把时间转换成毫秒,供令牌桶计算使用
local nowMillis = redisTime[1] * 1000 + math.floor(redisTime[2] / 1000)
--[[
*传入三个参数,一个是桶的key,一个是时间窗口,一个是容量
*拿出某个IP/用户的令牌桶当前状态；如果这个桶第一次出现，就先按照“满桶”状态初始化，然后准备计算它当前应该有多少令牌。
]]
local function preview(bucketKey, windowMillis, capacity)
    --查看最近更新的时间和剩余的令牌数目
    local lastMillis = tonumber(redis.call('HGET', bucketKey, 'lastMillis'))
    local tokens = tonumber(redis.call('HGET', bucketKey, 'tokens'))
   --如果这个桶第一次出现就进行初始化
    if not lastMillis or not tokens then
        lastMillis = nowMillis
        tokens = capacity
    end
    --否则就计算经过了多少时间,根据时间来计算应该补充多少令牌
    local elapsed = math.max(0, nowMillis - lastMillis)
    local refill = elapsed * capacity / windowMillis
    --令牌最多恢复满
    tokens = math.min(capacity, tokens + refill)
    --返回是否还可以进行操作,token<1就说明已经没有令牌了,不能进行操作,也就是流量超了
    return tokens, tokens >= 1
end
--[[
*consume来标志是否消费
]]
local function persist(bucketKey, windowMillis, tokens, consume)
    --如果消费,token减1
    if consume then
        tokens = tokens - 1
    end
    --重置token和lastMillis,并设置过期时间,过期时间是窗口的两倍,保证在窗口内没有访问的桶可以被清理掉
    redis.call('HSET', bucketKey, 'tokens', tokens, 'lastMillis', nowMillis)
    redis.call('PEXPIRE', bucketKey, math.max(1000, windowMillis * 2))
end

--拿到ip层面 token数目以及是否还能消费
local ipTokens, ipAllowed = preview(ipBucketKey, ipWindowMillis, ipCapacity)
--拿到user层面 token数目以及是否还能消费
local userTokens, userAllowed = preview(userBucketKey, userWindowMillis, userCapacity)
--两个都为true才允许消费,否则就不允许消费
local allowed = ipAllowed and userAllowed

--这里就是消费后刷新ip和用户的令牌桶
persist(ipBucketKey, ipWindowMillis, ipTokens, allowed)
persist(userBucketKey, userWindowMillis, userTokens, allowed)

if not ipAllowed then
    return CODE_IP_EXCEEDED
end
if not userAllowed then
    return CODE_USER_EXCEEDED
end
return CODE_ALLOWED
