package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.message.SeckillOrderMessage;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SeckillOrderConsumerServiceTest {

    private IVoucherOrderPersistenceService persistenceService;
    private SeckillOrderOutboxMapper outboxMapper;
    private SeckillOrderConsumerService service;

    @BeforeEach
    void setUp() {
        persistenceService = Mockito.mock(IVoucherOrderPersistenceService.class);
        outboxMapper = Mockito.mock(SeckillOrderOutboxMapper.class);
        service = new SeckillOrderConsumerService(persistenceService, outboxMapper);
    }

    @Test
    void persistsBatchThenCompletesOutboxesByIdsWithoutConfirmationReads() {
        List<VoucherOrder> result = service.createOrders(Arrays.asList(
                message(11L, 1001L, 2L, 7L), message(12L, 1002L, 2L, 8L)));

        ArgumentCaptor<List<VoucherOrder>> orders = ArgumentCaptor.forClass(List.class);
        InOrder inOrder = Mockito.inOrder(persistenceService, outboxMapper);
        inOrder.verify(persistenceService).createVoucherOrders(orders.capture());
        inOrder.verify(outboxMapper).markCompletedBatchByIds(Arrays.asList(11L, 12L));
        Mockito.verify(outboxMapper, Mockito.never()).findIdsByOrderIds(Mockito.anyList());
        Mockito.verify(outboxMapper, Mockito.never()).countCompletedBatchByIds(Mockito.anyList());
        assertEquals(2, result.size());
        assertEquals(7L, orders.getValue().get(0).getUserId());
    }

    @Test
    void deduplicatesRedeliveredOrderWithinBatch() {
        List<VoucherOrder> result = service.createOrders(Arrays.asList(
                message(11L, 1001L, 2L, 7L), message(11L, 1001L, 2L, 7L)));

        assertEquals(1, result.size());
        Mockito.verify(outboxMapper).markCompletedBatchByIds(
                java.util.Collections.singletonList(11L));
    }

    @Test
    void retainedMessageWithoutOutboxIdUsesOrderIdCompatibilityUpdate() {
        SeckillOrderMessage retained = message(null, 1001L, 2L, 7L);

        service.createOrders(java.util.Collections.singletonList(retained));

        Mockito.verify(outboxMapper).markCompletedBatch(
                java.util.Collections.singletonList(1001L));
    }

    private SeckillOrderMessage message(
            Long outboxId, long orderId, long voucherId, long userId) {
        return new SeckillOrderMessage(
                String.valueOf(orderId), outboxId, orderId, voucherId, userId, true, 1L);
    }
}
