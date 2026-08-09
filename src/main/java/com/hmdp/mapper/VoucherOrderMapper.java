package com.hmdp.mapper;

import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

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
            "WHERE user_id = #{userId} AND voucher_id = #{voucherId} LIMIT 1")
    Long selectOrderId(@Param("userId") Long userId, @Param("voucherId") Long voucherId);
}
