CREATE TABLE IF NOT EXISTS `tb_seckill_manual_review_action` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `order_id` bigint NOT NULL,
  `operator_user_id` bigint NOT NULL,
  `action` varchar(32) NOT NULL,
  `reason` varchar(500) NOT NULL,
  `created_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_seckill_manual_review_order` (`order_id`, `created_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Audited operator decisions for accepted seckill orders';
