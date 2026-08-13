SET @uk_user_voucher_exists = (
  SELECT COUNT(1)
  FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 'tb_voucher_order'
    AND index_name = 'uk_user_voucher'
);

SET @drop_uk_user_voucher = IF(
  @uk_user_voucher_exists > 0,
  'ALTER TABLE `tb_voucher_order` DROP INDEX `uk_user_voucher`',
  'SELECT 1'
);
PREPARE drop_uk_stmt FROM @drop_uk_user_voucher;
EXECUTE drop_uk_stmt;
DEALLOCATE PREPARE drop_uk_stmt;

SET @idx_user_voucher_status_exists = (
  SELECT COUNT(1)
  FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 'tb_voucher_order'
    AND index_name = 'idx_user_voucher_status'
);

SET @create_idx_user_voucher_status = IF(
  @idx_user_voucher_status_exists = 0,
  'ALTER TABLE `tb_voucher_order` ADD INDEX `idx_user_voucher_status` (`user_id`, `voucher_id`, `status`)',
  'SELECT 1'
);
PREPARE create_idx_stmt FROM @create_idx_user_voucher_status;
EXECUTE create_idx_stmt;
DEALLOCATE PREPARE create_idx_stmt;
