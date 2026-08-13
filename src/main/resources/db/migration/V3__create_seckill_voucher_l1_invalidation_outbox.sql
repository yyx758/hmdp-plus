CREATE TABLE IF NOT EXISTS `tb_seckill_voucher_l1_invalidation_outbox` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'surrogate primary key',
  `event_id` VARCHAR(64) NOT NULL COMMENT 'globally unique idempotency and trace id',
  `voucher_id` BIGINT UNSIGNED NOT NULL COMMENT 'voucher whose JVM local cache must be invalidated',
  `reason` VARCHAR(64) NOT NULL COMMENT 'business reason for invalidation',
  `status` VARCHAR(16) NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING, SENT or FAILED',
  `retry_count` INT UNSIGNED NOT NULL DEFAULT 0,
  `next_retry_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_error` VARCHAR(512) NULL,
  `created_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `sent_time` DATETIME NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_seckill_voucher_l1_outbox_event_id` (`event_id`),
  KEY `idx_seckill_voucher_l1_outbox_dispatch` (`status`, `next_retry_time`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='transactional outbox for Kafka seckill voucher L1 invalidation';
