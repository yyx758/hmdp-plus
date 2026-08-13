package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId, String accessToken);

    default Result seckillVoucher(Long voucherId) {
        return seckillVoucher(voucherId, null);
    }

    Result querySeckillOrderStatus(Long orderId);

    Result queryActiveOrderId(Long voucherId);

    Result cancelVoucherOrder(Long voucherId);
}
