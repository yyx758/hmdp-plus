package com.hmdp.service.impl;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.exception.DatabaseStockMismatchException;
import com.hmdp.exception.OrderIdConflictException;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IVoucherOrderPersistenceService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class VoucherOrderPersistenceServiceImpl implements IVoucherOrderPersistenceService {

    @Resource
    private VoucherOrderMapper voucherOrderMapper;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        createVoucherOrders(Collections.singletonList(voucherOrder));
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void createVoucherOrders(List<VoucherOrder> voucherOrders) {
        if (voucherOrders == null || voucherOrders.isEmpty()) {
            return;
        }

        // 按券分组后，每种券只执行一次批量插入和一次库存扣减，避免逐单争抢同一库存行。
        Map<Long, List<VoucherOrder>> ordersByVoucher = new LinkedHashMap<>();
        for (VoucherOrder voucherOrder : voucherOrders) {
            validateOrder(voucherOrder);
            ordersByVoucher.computeIfAbsent(voucherOrder.getVoucherId(), ignored -> new ArrayList<>())
                    .add(voucherOrder);
        }

        for (Map.Entry<Long, List<VoucherOrder>> entry : ordersByVoucher.entrySet()) {
            Long voucherId = entry.getKey();
            List<VoucherOrder> orders = entry.getValue();
            int inserted = voucherOrderMapper.batchInsertIgnore(orders);

            if (inserted < orders.size()) {
                // 仅在重复投递或极少见的主键冲突路径查询，正常批次没有额外 SELECT。
                verifyIgnoredOrdersExist(orders);
            }
            if (inserted == 0) {
                continue;
            }

            int updated = voucherOrderMapper.decrementStock(voucherId, inserted);
            if (updated != 1) {
                throw new DatabaseStockMismatchException(voucherId, inserted);
            }
        }
    }

    @Override
    public Long findOrderId(Long userId, Long voucherId) {
        return voucherOrderMapper.selectOrderId(userId, voucherId);
    }

    private void validateOrder(VoucherOrder voucherOrder) {
        if (voucherOrder == null || voucherOrder.getId() == null
                || voucherOrder.getUserId() == null || voucherOrder.getVoucherId() == null) {
            throw new IllegalArgumentException("秒杀订单字段不完整");
        }
    }

    private void verifyIgnoredOrdersExist(List<VoucherOrder> orders) {
        for (VoucherOrder order : orders) {
            Long existingOrderId = voucherOrderMapper.selectOrderId(
                    order.getUserId(), order.getVoucherId()
            );
            if (existingOrderId == null) {
                throw new IllegalStateException("订单被忽略但不存在对应业务订单，可能发生订单ID冲突，orderId="
                        + order.getId());
            }
            if (!order.getId().equals(existingOrderId)) {
                throw new OrderIdConflictException(order.getId(), existingOrderId);
            }
        }
    }
}
