ALTER TABLE `tb_seckill_order_outbox`
  ADD COLUMN `review_owner` varchar(64) NULL COMMENT 'Responsible operator queue',
  ADD COLUMN `review_deadline` datetime NULL COMMENT 'Manual review deadline',
  ADD INDEX `idx_seckill_order_review_deadline` (`status`, `review_deadline`);
