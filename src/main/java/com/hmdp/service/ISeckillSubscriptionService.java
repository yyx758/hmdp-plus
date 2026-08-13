package com.hmdp.service;

import com.hmdp.dto.VoucherSubscribeStatusDTO;
import com.hmdp.entity.VoucherOrder;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface ISeckillSubscriptionService {

    void subscribe(Long voucherId, Long userId);

    void unsubscribe(Long voucherId, Long userId);

    int getSubscribeStatus(Long voucherId, Long userId);

    List<VoucherSubscribeStatusDTO> getSubscribeStatusBatch(
            Collection<Long> voucherIds, Long userId);

    void markPurchased(VoucherOrder voucherOrder);

    void clearPurchased(VoucherOrder voucherOrder);

    Set<Long> listSubscriberUserIds(Long voucherId, int limit);
}
