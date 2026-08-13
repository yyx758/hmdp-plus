local handoffKey = KEYS[1]
local acceptedKey = KEYS[2]
local argumentCount = #ARGV
if argumentCount == 0 then
    return 0
end
if argumentCount % 2 ~= 0 then
    return redis.error_reply('handoff members and accepted order ids are inconsistent')
end

local members = {}
local orderIds = {}
for index = 1, argumentCount, 2 do
    members[#members + 1] = ARGV[index]
    orderIds[#orderIds + 1] = ARGV[index + 1]
end

-- Keep the responsibility-transfer cleanup atomic, but issue only two Redis
-- commands for the whole batch instead of two commands per event.
local removed = redis.call('ZREM', handoffKey, unpack(members))
redis.call('HDEL', acceptedKey, unpack(orderIds))
return removed
