package com.hmdp.kafka.outbox;

import com.hmdp.kafka.SeckillVoucherLocalCacheInvalidationKafkaProducer;
import com.hmdp.kafka.message.SeckillVoucherLocalCacheInvalidationMessage;
import com.hmdp.mapper.SeckillVoucherLocalCacheInvalidationOutboxMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SeckillVoucherLocalCacheInvalidationOutboxRelayTest {

    private SeckillVoucherLocalCacheInvalidationOutboxMapper mapper;
    private SeckillVoucherLocalCacheInvalidationKafkaProducer producer;
    private SeckillVoucherLocalCacheInvalidationOutboxRelay relay;

    @BeforeEach
    void setUp() {
        mapper = Mockito.mock(SeckillVoucherLocalCacheInvalidationOutboxMapper.class);
        producer = Mockito.mock(SeckillVoucherLocalCacheInvalidationKafkaProducer.class);
        relay = new SeckillVoucherLocalCacheInvalidationOutboxRelay(
                mapper, producer, Mockito.mock(RedissonClient.class),
                100, 1, 3, 1, 60);
    }

    @Test
    void marksSentOnlyAfterKafkaAcknowledgment() {
        SeckillVoucherLocalCacheInvalidationOutboxEvent event = event(1L, 0);
        SettableListenableFuture<SendResult<
                String, SeckillVoucherLocalCacheInvalidationMessage>> future =
                new SettableListenableFuture<>();
        future.set(null);
        Mockito.when(producer.send(
                Mockito.any(SeckillVoucherLocalCacheInvalidationMessage.class)))
                .thenReturn(future);
        Mockito.when(mapper.markSent(1L)).thenReturn(1);

        relay.dispatch(event);

        ArgumentCaptor<SeckillVoucherLocalCacheInvalidationMessage> captor =
                ArgumentCaptor.forClass(SeckillVoucherLocalCacheInvalidationMessage.class);
        Mockito.verify(producer).send(captor.capture());
        assertEquals("event-1", captor.getValue().getEventId());
        assertEquals(7L, captor.getValue().getVoucherId());
        Mockito.verify(mapper).markSent(1L);
    }

    @Test
    void schedulesRetryWhenKafkaSendFails() {
        SeckillVoucherLocalCacheInvalidationOutboxEvent event = event(2L, 0);
        SettableListenableFuture<SendResult<
                String, SeckillVoucherLocalCacheInvalidationMessage>> future =
                new SettableListenableFuture<>();
        future.setException(new IllegalStateException("broker unavailable"));
        Mockito.when(producer.send(
                Mockito.any(SeckillVoucherLocalCacheInvalidationMessage.class)))
                .thenReturn(future);

        relay.dispatch(event);

        Mockito.verify(mapper).scheduleRetry(
                Mockito.eq(2L), Mockito.any(LocalDateTime.class),
                Mockito.contains("broker unavailable"));
        Mockito.verify(mapper, Mockito.never()).markSent(Mockito.anyLong());
    }

    @Test
    void exhaustedEventMovesToFailedParkingLot() {
        SeckillVoucherLocalCacheInvalidationOutboxEvent event = event(3L, 2);
        SettableListenableFuture<SendResult<
                String, SeckillVoucherLocalCacheInvalidationMessage>> future =
                new SettableListenableFuture<>();
        future.setException(new IllegalStateException("serialization failed"));
        Mockito.when(producer.send(
                Mockito.any(SeckillVoucherLocalCacheInvalidationMessage.class)))
                .thenReturn(future);

        relay.dispatch(event);

        Mockito.verify(mapper).markFailed(
                Mockito.eq(3L), Mockito.contains("serialization failed"));
    }

    private SeckillVoucherLocalCacheInvalidationOutboxEvent event(Long id, int retryCount) {
        return new SeckillVoucherLocalCacheInvalidationOutboxEvent()
                .setId(id)
                .setEventId("event-" + id)
                .setVoucherId(7L)
                .setReason("voucher-updated")
                .setStatus(SeckillVoucherLocalCacheInvalidationOutboxEvent.STATUS_PENDING)
                .setRetryCount(retryCount)
                .setCreatedTime(LocalDateTime.now());
    }
}
