local startId = ARGV[1]
local voucherIds = {}
for index = 2, #ARGV do
    voucherIds[ARGV[index]] = true
end

local records = redis.call('XRANGE', 'stream.orders', '(' .. startId, '+')
local removedEntries = 0
local removedResults = 0
for _, record in ipairs(records) do
    local recordId = record[1]
    local fields = record[2]
    local voucherId = nil
    local orderId = nil
    for fieldIndex = 1, #fields, 2 do
        if fields[fieldIndex] == 'voucherId' then
            voucherId = fields[fieldIndex + 1]
        elseif fields[fieldIndex] == 'id' then
            orderId = fields[fieldIndex + 1]
        end
    end
    if voucherId and voucherIds[voucherId] then
        removedEntries = removedEntries + redis.call('XDEL', 'stream.orders', recordId)
        redis.call('HDEL', 'stream.orders:retry', recordId)
        if orderId then
            removedResults = removedResults
                    + redis.call('DEL', 'seckill:order:result:' .. orderId)
        end
    end
end

return {removedEntries, removedResults}
