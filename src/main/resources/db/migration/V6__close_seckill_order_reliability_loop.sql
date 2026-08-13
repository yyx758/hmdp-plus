ALTER TABLE `tb_seckill_order_outbox`
  ADD COLUMN `relay_owner` varchar(64) DEFAULT NULL AFTER `last_error`,
  ADD COLUMN `relay_lease_until` datetime DEFAULT NULL AFTER `relay_owner`;

ALTER TABLE `tb_seckill_order_outbox`
  ADD KEY `idx_seckill_order_outbox_relay_lease`
    (`status`, `next_retry_time`, `relay_lease_until`, `id`);
