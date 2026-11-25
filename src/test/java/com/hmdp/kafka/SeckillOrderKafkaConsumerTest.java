package com.hmdp.kafka;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.message.SeckillOrderMessage;
import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.service.ISeckillTopBuyerService;
import com.hmdp.service.SeckillOrderConsumerService;
import com.hmdp.service.SeckillOrderReceiptService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;

class SeckillOrderKafkaConsumerTest {

    private SeckillOrderConsumerService consumerService;
    private SeckillOrderOutboxMapper outboxMapper;
    private SeckillOrderReceiptService receiptService;
    private SeckillOrderKafkaConsumer consumer;
    private Acknowledgment acknowledgment;

    @BeforeEach
    void setUp() {
        consumerService = Mockito.mock(SeckillOrderConsumerService.class);
        outboxMapper = Mockito.mock(SeckillOrderOutboxMapper.class);
        receiptService = Mockito.mock(SeckillOrderReceiptService.class);
        consumer = new SeckillOrderKafkaConsumer(consumerService, outboxMapper,
                Mockito.mock(ISeckillTopBuyerService.class), receiptService);
        acknowledgment = Mockito.mock(Acknowledgment.class);
    }

    @Test
    void acknowledgesOnlyAfterOrderTransactionReturns() {
        VoucherOrder order = new VoucherOrder().setId(1001L).setUserId(7L).setVoucherId(2L);
        Mockito.when(consumerService.createOrders(Mockito.anyList()))
                .thenReturn(Collections.singletonList(order));

        consumer.onMessage(Collections.singletonList(message()), acknowledgment);

        InOrder calls = Mockito.inOrder(consumerService, acknowledgment, receiptService);
        calls.verify(consumerService).createOrders(Mockito.anyList());
        calls.verify(acknowledgment).acknowledge();
        calls.verify(receiptService).markSuccess(order);
    }

    @Test
    void failedOrderTransactionDoesNotAck() {
        Mockito.when(consumerService.createOrders(Mockito.anyList()))
                .thenThrow(new IllegalStateException("database unavailable"));

        assertThrows(IllegalStateException.class,
                () -> consumer.onMessage(Collections.singletonList(message()), acknowledgment));

        Mockito.verifyNoInteractions(acknowledgment, receiptService);
    }

    @Test
    void deadLetterParksThenAcknowledges() {
        Mockito.when(outboxMapper.findByOrderId(1001L)).thenReturn(
                SeckillOrderOutboxEvent.pending(1001L, 2L, 7L, false));
        Mockito.when(outboxMapper.markManualReview(Mockito.eq(1001L), Mockito.anyString()))
                .thenReturn(1);

        consumer.onDeadLetter(Collections.singletonList(message()), acknowledgment);

        InOrder calls = Mockito.inOrder(outboxMapper, acknowledgment);
        calls.verify(outboxMapper).markManualReview(Mockito.eq(1001L), Mockito.anyString());
        calls.verify(acknowledgment).acknowledge();
        Mockito.verify(outboxMapper, Mockito.never()).requeueAccepted(
                Mockito.anyLong(), Mockito.any(), Mockito.anyString());
    }

    @Test
    void failedDeadLetterParkingDoesNotAck() {
        Mockito.when(outboxMapper.findByOrderId(1001L)).thenReturn(
                SeckillOrderOutboxEvent.pending(1001L, 2L, 7L, false));

        assertThrows(IllegalStateException.class,
                () -> consumer.onDeadLetter(Collections.singletonList(message()), acknowledgment));

        Mockito.verifyNoInteractions(acknowledgment);
    }

    private SeckillOrderMessage message() {
        return new SeckillOrderMessage("1001", 1L, 1001L, 2L, 7L, false, 0L);
    }
}
