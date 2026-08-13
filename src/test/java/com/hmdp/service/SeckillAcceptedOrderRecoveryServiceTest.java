package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

class SeckillAcceptedOrderRecoveryServiceTest {

    private VoucherOrderMapper orderMapper;
    private SeckillOrderOutboxMapper outboxMapper;
    private SeckillAcceptedOrderRecoveryService service;

    @BeforeEach
    void setUp() {
        orderMapper = Mockito.mock(VoucherOrderMapper.class);
        outboxMapper = Mockito.mock(SeckillOrderOutboxMapper.class);
        service = new SeckillAcceptedOrderRecoveryService(orderMapper, outboxMapper, 1, 300);
    }

    @Test
    void requeuesAcceptedOrderInsteadOfReleasingRedisReservation() {
        Mockito.when(outboxMapper.requeueAccepted(eq(1001L), any(LocalDateTime.class), eq("failed")))
                .thenReturn(1);

        service.retry(event(), "failed");

        Mockito.verify(outboxMapper).requeueAccepted(
                eq(1001L), any(LocalDateTime.class), eq("failed"));
        Mockito.verify(outboxMapper, Mockito.never()).markCompleted(any());
    }

    @Test
    void completesOutboxWhenOrderAlreadyExists() {
        Mockito.when(orderMapper.selectById(1001L)).thenReturn(
                new VoucherOrder().setId(1001L).setVoucherId(2L).setUserId(7L));
        Mockito.when(outboxMapper.markCompleted(1001L)).thenReturn(1);

        service.retry(event(), "failed");

        Mockito.verify(outboxMapper).markCompleted(1001L);
        Mockito.verify(outboxMapper, Mockito.never()).requeueAccepted(any(), any(), any());
    }

    private SeckillOrderOutboxEvent event() {
        return SeckillOrderOutboxEvent.pending(1001L, 2L, 7L, false);
    }
}
