local auditKey = KEYS[1]
local now = tonumber(ARGV[1])
local leaseMillis = tonumber(ARGV[2])
local due = redis.call('ZRANGEBYSCORE', auditKey, '-inf', now, 'LIMIT', 0, 1)
if #due == 0 then
    return nil
end
redis.call('ZADD', auditKey, now + leaseMillis, due[1])
return due[1]
