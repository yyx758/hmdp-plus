package com.hmdp.service;

import com.hmdp.dto.TopBuyerDTO;
import com.hmdp.entity.VoucherOrder;

import java.util.List;
import java.util.Set;

public interface ISeckillTopBuyerService {

    void recordSuccessfulOrder(VoucherOrder voucherOrder);

    void rollbackCancelledOrder(VoucherOrder voucherOrder);

    List<TopBuyerDTO> queryTopBuyers(Long shopId, int days, int limit);

    Set<Long> queryTopBuyerUserIds(Long shopId, int days, int limit);
}
