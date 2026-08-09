package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;

import java.util.List;


public interface IVoucherOrderPersistenceService {

    void createVoucherOrder(VoucherOrder voucherOrder);

    void createVoucherOrders(List<VoucherOrder> voucherOrders);

    Long findOrderId(Long userId, Long voucherId);

}
