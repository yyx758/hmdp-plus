package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.message.SeckillOrderMessage;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class SeckillOrderConsumerService {

    private final IVoucherOrderPersistenceService persistenceService;
    private final SeckillOrderOutboxMapper outboxMapper;

    public SeckillOrderConsumerService(
            IVoucherOrderPersistenceService persistenceService,
            SeckillOrderOutboxMapper outboxMapper) {
        this.persistenceService = persistenceService;
        this.outboxMapper = outboxMapper;
    }

    @Transactional(rollbackFor = Exception.class)
    public VoucherOrder createOrder(SeckillOrderMessage message) {
        return createOrders(Collections.singletonList(message)).get(0);
    }

    @Transactional(rollbackFor = Exception.class)
    public List<VoucherOrder> createOrders(List<SeckillOrderMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return Collections.emptyList();
        }
        Map<Long, VoucherOrder> uniqueOrders = new LinkedHashMap<>();
        Map<Long, Long> outboxIdsByOrder = new LinkedHashMap<>();
        for (SeckillOrderMessage message : messages) {
            validate(message);
            VoucherOrder order = new VoucherOrder()
                    .setId(message.getOrderId())
                    .setVoucherId(message.getVoucherId())
                    .setUserId(message.getUserId())
                    .setAutoIssued(Boolean.TRUE.equals(message.getAutoIssued()));
            VoucherOrder previous = uniqueOrders.putIfAbsent(order.getId(), order);
            if (message.getOutboxId() != null) {
                outboxIdsByOrder.putIfAbsent(order.getId(), message.getOutboxId());
            }
            if (previous != null && (!previous.getVoucherId().equals(order.getVoucherId())
                    || !previous.getUserId().equals(order.getUserId()))) {
                throw new IllegalArgumentException(
                        "Same order id maps to different seckill messages, orderId=" + order.getId());
            }
        }

        List<VoucherOrder> orders = new ArrayList<>(uniqueOrders.values());
        persistenceService.createVoucherOrders(orders);
        List<Long> orderIds = new ArrayList<>(uniqueOrders.keySet());
        if (outboxIdsByOrder.size() == orderIds.size()) {
            List<Long> outboxIds = new ArrayList<>(outboxIdsByOrder.values());
            Collections.sort(outboxIds);
            outboxMapper.markCompletedBatchByIds(outboxIds);
        } else {
            // Compatibility path for retained messages produced before outboxId was added.
            Collections.sort(orderIds);
            outboxMapper.markCompletedBatch(orderIds);
        }
        return orders;
    }

    private void validate(SeckillOrderMessage message) {
        if (message == null || message.getOrderId() == null || message.getVoucherId() == null
                || message.getUserId() == null) {
            throw new IllegalArgumentException("Incomplete seckill order message");
        }
    }
}
