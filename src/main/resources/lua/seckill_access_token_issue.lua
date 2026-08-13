-- 接收access token的key
local key = KEYS[1]
--接收随机字符串
local candidate = ARGV[1]
--接收过期的时间
local ttlMillis = tonumber(ARGV[2])
--[[
判断key是否存在，如果存在直接返回key对应的value
如果不存在，则将随机字符串candidate存入key中，并设置过期时间ttlMillis
]]
local existing = redis.call('GET', key)
if existing then
    return existing
end

redis.call('PSETEX', key, ttlMillis, candidate)
return candidate
