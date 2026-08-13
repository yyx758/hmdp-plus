CREATE TABLE IF NOT EXISTS `tb_id_segment` (
  `biz_tag` varchar(64) NOT NULL,
  `max_id` bigint NOT NULL,
  `step` int NOT NULL,
  `version` bigint NOT NULL DEFAULT 0,
  `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`biz_tag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Distributed ID segment high-water mark';

-- Keep database-segment order IDs in a namespace disjoint from the existing
-- timestamp-based Redis IDs. 2^62 remains positive in a signed BIGINT.
INSERT IGNORE INTO `tb_id_segment` (`biz_tag`, `max_id`, `step`)
VALUES ('voucher-order', 4611686018427387904, 10000);
