-- Cancelled rows get NULL, so historical cancelled purchases remain possible.
-- Every other status represents an active purchase for one user and voucher.
ALTER TABLE `tb_voucher_order`
  ADD COLUMN `active_order_marker` tinyint
    GENERATED ALWAYS AS (CASE WHEN `status` <> 4 THEN 1 ELSE NULL END) STORED,
  ADD UNIQUE KEY `uk_voucher_order_active_user_voucher`
    (`user_id`, `voucher_id`, `active_order_marker`);
