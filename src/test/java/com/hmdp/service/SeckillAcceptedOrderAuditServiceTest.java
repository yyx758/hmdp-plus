package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;

class SeckillAcceptedOrderAuditServiceTest {

    private HashOperations<String, Object, Object> hashes;
    private VoucherOrderMapper orderMapper;
    private SeckillOrderOutboxMapper outboxMapper;
    private SeckillOrderOutboxBatchWriter batchWriter;
    private SeckillOrderReceiptService receiptService;
    private SeckillAcceptedOrderAuditService service;

    @BeforeEach
    void setUp() {
        StringRedisTemplate redis = Mockito.mock(StringRedisTemplate.class);
        hashes = Mockito.mock(HashOperations.class);
        Mockito.when(redis.opsForHash()).thenReturn(hashes);
        orderMapper = Mockito.mock(VoucherOrderMapper.class);
        outboxMapper = Mockito.mock(SeckillOrderOutboxMapper.class);
        batchWriter = Mockito.mock(SeckillOrderOutboxBatchWriter.class);
        receiptService = Mockito.mock(SeckillOrderReceiptService.class);
        service = new SeckillAcceptedOrderAuditService(
                redis, orderMapper, outboxMapper, batchWriter, receiptService, 10, 1000);
    }

    @Test
    void reconstructsMissingOutboxUsingOriginalOrderId() {
        Mockito.when(hashes.get("seckill:order:accepted", "1001"))
                .thenReturn("7|2|123456789|PROCESSING");

        service.auditOne(1001L);

        Mockito.verify(batchWriter).insertCommitted(Mockito.argThat(events ->
                events.size() == 1 && events.get(0).getOrderId().equals(1001L)
                        && events.get(0).getVoucherId().equals(2L)
                        && events.get(0).getUserId().equals(7L)));
    }

    @Test
    void existingOrderCompletesOutboxAndTerminalReceipt() {
        VoucherOrder order = new VoucherOrder().setId(1001L)
                .setUserId(7L).setVoucherId(2L).setStatus(1);
        Mockito.when(hashes.get("seckill:order:accepted", "1001"))
                .thenReturn("7|2|123456789|PROCESSING");
        Mockito.when(orderMapper.selectById(1001L)).thenReturn(order);
        Mockito.when(outboxMapper.findByOrderId(1001L)).thenReturn(
                SeckillOrderOutboxEvent.pending(1001L, 2L, 7L, false));

        service.auditOne(1001L);

        Mockito.verify(outboxMapper).markCompleted(1001L);
        Mockito.verify(receiptService).markSuccess(order);
        Mockito.verifyNoInteractions(batchWriter);
    }

    @Test
    void manualReviewIsNotAutomaticallyRequeued() {
        Mockito.when(hashes.get("seckill:order:accepted", "1001"))
                .thenReturn("7|2|123456789|PROCESSING");
        Mockito.when(outboxMapper.findByOrderId(1001L)).thenReturn(
                SeckillOrderOutboxEvent.pending(1001L, 2L, 7L, false)
                        .setStatus(SeckillOrderOutboxEvent.STATUS_MANUAL_REVIEW));

        service.auditOne(1001L);

        Mockito.verify(outboxMapper, Mockito.never()).requeueAccepted(
                Mockito.anyLong(), Mockito.any(), Mockito.anyString());
        Mockito.verifyNoInteractions(batchWriter, receiptService);
    }

    @Test
    void identityConflictStopsRecovery() {
        Mockito.when(hashes.get("seckill:order:accepted", "1001"))
                .thenReturn("7|2|123456789|PROCESSING");
        Mockito.when(orderMapper.selectById(1001L)).thenReturn(
                new VoucherOrder().setId(1001L).setUserId(8L).setVoucherId(2L));

        assertThrows(IllegalStateException.class, () -> service.auditOne(1001L));

        Mockito.verifyNoInteractions(batchWriter, receiptService);
    }
}
