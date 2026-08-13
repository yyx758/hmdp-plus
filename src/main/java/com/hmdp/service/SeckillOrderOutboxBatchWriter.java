package com.hmdp.service;

import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.exception.SeckillOutboxEventConflictException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
public class SeckillOrderOutboxBatchWriter {

    private final SeckillOrderOutboxMapper outboxMapper;

    public SeckillOrderOutboxBatchWriter(SeckillOrderOutboxMapper outboxMapper) {
        this.outboxMapper = outboxMapper;
    }

    @Transactional(rollbackFor = Exception.class)
    public Map<Long, SeckillOrderOutboxEvent> insertCommitted(
            List<SeckillOrderOutboxEvent> events) {
        int inserted = outboxMapper.insertIgnoreBatch(events);
        Map<Long, SeckillOrderOutboxEvent> accepted = new HashMap<>();
        if (inserted == events.size()) {
            for (SeckillOrderOutboxEvent event : events) {
                accepted.put(event.getOrderId(), event);
            }
            return accepted;
        }

        List<Long> orderIds = new ArrayList<>(events.size());
        for (SeckillOrderOutboxEvent event : events) {
            orderIds.add(event.getOrderId());
        }
        List<SeckillOrderOutboxEvent> persisted = outboxMapper.findByOrderIds(orderIds);
        if (persisted != null) {
            for (SeckillOrderOutboxEvent event : persisted) {
                accepted.put(event.getOrderId(), event);
            }
        }
        java.util.Set<Long> requestedOrderIds = new java.util.HashSet<>();
        for (SeckillOrderOutboxEvent event : events) {
            requestedOrderIds.add(event.getOrderId());
        }
        if (accepted.size() != requestedOrderIds.size()) {
            throw new SeckillOutboxEventConflictException(
                    "秒杀订单Outbox批量写入结果不完整");
        }
        for (SeckillOrderOutboxEvent requested : events) {
            SeckillOrderOutboxEvent actual = accepted.get(requested.getOrderId());
            if (!Objects.equals(requested.getEventId(), actual.getEventId())
                    || !Objects.equals(requested.getVoucherId(), actual.getVoucherId())
                    || !Objects.equals(requested.getUserId(), actual.getUserId())
                    || !Objects.equals(requested.getAutoIssued(), actual.getAutoIssued())) {
                throw new SeckillOutboxEventConflictException(
                        "秒杀订单Outbox幂等键冲突，orderId=" + requested.getOrderId());
            }
        }
        return accepted;
    }
}
