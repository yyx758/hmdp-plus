SET @idx_voucher_status_user_exists = (
  SELECT COUNT(1)
  FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 'tb_voucher_order'
    AND index_name = 'idx_voucher_status_user'
);

SET @create_idx_voucher_status_user = IF(
  @idx_voucher_status_user_exists = 0,
  'ALTER TABLE `tb_voucher_order` ADD INDEX `idx_voucher_status_user` (`voucher_id`, `status`, `user_id`)',
  'SELECT 1'
);
PREPARE create_idx_voucher_status_user_stmt FROM @create_idx_voucher_status_user;
EXECUTE create_idx_voucher_status_user_stmt;
DEALLOCATE PREPARE create_idx_voucher_status_user_stmt;
