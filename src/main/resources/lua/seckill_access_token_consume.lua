--接收key
local key = KEYS[1]
--期望的token
local expected = ARGV[1]
--获取当前的token
local actual = redis.call('GET', key)
--如果两个token相同,说明令牌核验正确,可以删除了,返回1
if actual == expected then
    return redis.call('DEL', key)
end
-- 否则就返回0,代表令牌核验错误
return 0
