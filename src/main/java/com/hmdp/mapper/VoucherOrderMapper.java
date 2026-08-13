package com.hmdp.mapper;

import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface VoucherOrderMapper extends BaseMapper<VoucherOrder> {

    @Insert("INSERT IGNORE INTO tb_voucher_order (id, user_id, voucher_id) " +
            "VALUES (#{order.id}, #{order.userId}, #{order.voucherId})")
    int insertIgnore(@Param("order") VoucherOrder order);

    @Insert({
            "<script>",
            "INSERT IGNORE INTO tb_voucher_order (id, user_id, voucher_id) VALUES ",
            "<foreach collection='orders' item='order' separator=','>",
            "(#{order.id}, #{order.userId}, #{order.voucherId})",
            "</foreach>",
            "</script>"
    })
    int batchInsertIgnore(@Param("orders") List<VoucherOrder> orders);

    @org.apache.ibatis.annotations.Update("UPDATE tb_seckill_voucher " +
            "SET stock = stock - #{amount} " +
            "WHERE voucher_id = #{voucherId} AND stock >= #{amount}")
    int decrementStock(@Param("voucherId") Long voucherId, @Param("amount") int amount);

    @Select("SELECT id FROM tb_voucher_order " +
            "WHERE user_id = #{userId} AND voucher_id = #{voucherId} " +
            "AND status <> 4 ORDER BY create_time DESC LIMIT 1")
    Long selectOrderId(@Param("userId") Long userId, @Param("voucherId") Long voucherId);

    @Select("SELECT id, user_id, voucher_id, pay_type, status, create_time, " +
            "pay_time, use_time, refund_time, update_time " +
            "FROM tb_voucher_order WHERE user_id = #{userId} " +
            "AND voucher_id = #{voucherId} AND status <> 4 " +
            "ORDER BY create_time DESC LIMIT 1")
    VoucherOrder selectActiveOrder(
            @Param("userId") Long userId,
            @Param("voucherId") Long voucherId);

    @Update("UPDATE tb_voucher_order SET status = 4, update_time = CURRENT_TIMESTAMP " +
            "WHERE id = #{orderId} AND user_id = #{userId} AND status IN (1, 2)")
    int cancelActiveOrder(
            @Param("orderId") Long orderId,
            @Param("userId") Long userId);

    @Select("SELECT DISTINCT user_id FROM tb_voucher_order "
            + "WHERE voucher_id = #{voucherId} AND status <> 4")
    List<Long> findActiveUserIds(@Param("voucherId") Long voucherId);
}
