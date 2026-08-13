package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillVoucherUpdateDTO;
import com.hmdp.dto.SeckillVoucherStockUpdateDTO;
import com.hmdp.dto.VoucherSubscribeBatchDTO;
import com.hmdp.entity.Voucher;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherService extends IService<Voucher> {

    Result queryVoucherOfShop(Long shopId);

    void addSeckillVoucher(Voucher voucher);

    Result updateSeckillVoucher(SeckillVoucherUpdateDTO update);

    Result updateSeckillVoucherStock(SeckillVoucherStockUpdateDTO update);

    Result subscribe(Long voucherId);

    Result unsubscribe(Long voucherId);

    Result getSubscribeStatus(Long voucherId);

    Result getSubscribeStatusBatch(VoucherSubscribeBatchDTO request);
}
