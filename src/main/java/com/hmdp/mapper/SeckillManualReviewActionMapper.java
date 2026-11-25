package com.hmdp.mapper;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;

public interface SeckillManualReviewActionMapper {

    @Insert("INSERT INTO tb_seckill_manual_review_action "
            + "(order_id, operator_user_id, action, reason) VALUES "
            + "(#{orderId}, #{operatorUserId}, #{action}, #{reason})")
    int insert(@Param("orderId") Long orderId,
               @Param("operatorUserId") Long operatorUserId,
               @Param("action") String action,
               @Param("reason") String reason);
}
