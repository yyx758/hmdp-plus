-- Outbox存在即代表订单已经受理。历史版本的COMPENSATED不能作为失败终态，
-- 必须恢复为PENDING，继续使用相同order_id投递并落库。
UPDATE `tb_seckill_order_outbox`
SET `status` = 'PENDING',
    `next_retry_time` = NOW(),
    `last_error` = '历史补偿订单重新进入持久化队列',
    `relay_owner` = NULL,
    `relay_lease_until` = NULL,
    `updated_time` = NOW()
WHERE `status` = 'COMPENSATED';

