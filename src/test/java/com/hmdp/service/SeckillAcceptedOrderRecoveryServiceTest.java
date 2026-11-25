package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.SeckillManualReviewActionMapper;
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
    private SeckillManualReviewActionMapper actionMapper;

    @BeforeEach
    void setUp() {
        orderMapper = Mockito.mock(VoucherOrderMapper.class);
        outboxMapper = Mockito.mock(SeckillOrderOutboxMapper.class);
        actionMapper = Mockito.mock(SeckillManualReviewActionMapper.class);
        service = new SeckillAcceptedOrderRecoveryService(
                orderMapper, outboxMapper, actionMapper, 1, 300);
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

    @Test
    void operatorRetryRequiresManualReviewAndWritesAudit() {
        SeckillOrderOutboxEvent parked = event().setStatus(
                SeckillOrderOutboxEvent.STATUS_MANUAL_REVIEW);
        Mockito.when(outboxMapper.findByOrderId(1001L)).thenReturn(parked);
        Mockito.when(outboxMapper.requeueManualReview(eq(1001L), any(LocalDateTime.class),
                Mockito.anyString())).thenReturn(1);
        Mockito.when(actionMapper.insert(1001L, 9L, "RETRY", "checked"))
                .thenReturn(1);

        service.retryManualReview(1001L, 9L, "checked");

        Mockito.verify(actionMapper).insert(1001L, 9L, "RETRY", "checked");
    }

    @Test
    void operatorCanConfirmOnlyAnExistingMatchingOrder() {
        SeckillOrderOutboxEvent parked = event().setStatus(
                SeckillOrderOutboxEvent.STATUS_MANUAL_REVIEW);
        Mockito.when(outboxMapper.findByOrderId(1001L)).thenReturn(parked);
        Mockito.when(orderMapper.selectById(1001L)).thenReturn(
                new VoucherOrder().setId(1001L).setUserId(7L).setVoucherId(2L));
        Mockito.when(outboxMapper.markCompletedFromManualReview(1001L)).thenReturn(1);
        Mockito.when(actionMapper.insert(1001L, 9L, "CONFIRM_COMPLETED", "verified"))
                .thenReturn(1);

        service.confirmCompleted(1001L, 9L, "verified");

        Mockito.verify(outboxMapper).markCompletedFromManualReview(1001L);
        Mockito.verify(actionMapper).insert(1001L, 9L, "CONFIRM_COMPLETED", "verified");
    }

    @Test
    void failureReviewRequestDoesNotReleaseOrRequeue() {
        Mockito.when(outboxMapper.findByOrderId(1001L)).thenReturn(
                event().setStatus(SeckillOrderOutboxEvent.STATUS_MANUAL_REVIEW));
        Mockito.when(actionMapper.insert(1001L, 9L,
                "REQUEST_FAILURE_REVIEW", "cannot fulfill")).thenReturn(1);

        service.requestFailureReview(1001L, 9L, "cannot fulfill");

        Mockito.verify(actionMapper).insert(1001L, 9L,
                "REQUEST_FAILURE_REVIEW", "cannot fulfill");
        Mockito.verify(outboxMapper, Mockito.never()).requeueManualReview(
                Mockito.anyLong(), Mockito.any(), Mockito.anyString());
    }

    private SeckillOrderOutboxEvent event() {
        return SeckillOrderOutboxEvent.pending(1001L, 2L, 7L, false);
    }
}
