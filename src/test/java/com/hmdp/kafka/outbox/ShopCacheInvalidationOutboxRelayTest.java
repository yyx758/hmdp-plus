package com.hmdp.kafka.outbox;

import com.hmdp.kafka.ShopCacheInvalidationKafkaProducer;
import com.hmdp.kafka.message.ShopCacheInvalidationMessage;
import com.hmdp.mapper.ShopCacheInvalidationOutboxMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ShopCacheInvalidationOutboxRelayTest {

    private ShopCacheInvalidationOutboxMapper mapper;
    private ShopCacheInvalidationKafkaProducer producer;
    private ShopCacheInvalidationOutboxRelay relay;

    @BeforeEach
    void setUp() {
        mapper = Mockito.mock(ShopCacheInvalidationOutboxMapper.class);
        producer = Mockito.mock(ShopCacheInvalidationKafkaProducer.class);
        relay = new ShopCacheInvalidationOutboxRelay(
                mapper,
                producer,
                Mockito.mock(RedissonClient.class),
                100,
                1,
                3,
                1,
                60
        );
    }

    @Test
    void marksOutboxSentOnlyAfterKafkaAcknowledgment() {
        ShopCacheInvalidationOutboxEvent event = event(1L, 0);
        SettableListenableFuture<SendResult<String, ShopCacheInvalidationMessage>> future =
                new SettableListenableFuture<>();
        future.set(null);
        Mockito.when(producer.send(Mockito.any(ShopCacheInvalidationMessage.class)))
                .thenReturn(future);
        Mockito.when(mapper.markSent(1L)).thenReturn(1);

        relay.dispatch(event);

        ArgumentCaptor<ShopCacheInvalidationMessage> messageCaptor =
                ArgumentCaptor.forClass(ShopCacheInvalidationMessage.class);
        Mockito.verify(producer).send(messageCaptor.capture());
        assertEquals("event-1", messageCaptor.getValue().getEventId());
        assertEquals(7L, messageCaptor.getValue().getShopId());
        Mockito.verify(mapper).markSent(1L);
        Mockito.verify(mapper, Mockito.never()).scheduleRetry(
                Mockito.anyLong(), Mockito.any(), Mockito.anyString());
    }

    @Test
    void schedulesOutboxRetryWhenKafkaSendFails() {
        ShopCacheInvalidationOutboxEvent event = event(2L, 0);
        SettableListenableFuture<SendResult<String, ShopCacheInvalidationMessage>> future =
                new SettableListenableFuture<>();
        future.setException(new IllegalStateException("broker unavailable"));
        Mockito.when(producer.send(Mockito.any(ShopCacheInvalidationMessage.class)))
                .thenReturn(future);

        relay.dispatch(event);

        Mockito.verify(mapper).scheduleRetry(
                Mockito.eq(2L),
                Mockito.any(LocalDateTime.class),
                Mockito.contains("broker unavailable")
        );
        Mockito.verify(mapper, Mockito.never()).markSent(Mockito.anyLong());
    }

    @Test
    void movesExhaustedOutboxEventToFailedParkingLot() {
        ShopCacheInvalidationOutboxEvent event = event(3L, 2);
        SettableListenableFuture<SendResult<String, ShopCacheInvalidationMessage>> future =
                new SettableListenableFuture<>();
        future.setException(new IllegalStateException("serialization failed"));
        Mockito.when(producer.send(Mockito.any(ShopCacheInvalidationMessage.class)))
                .thenReturn(future);

        relay.dispatch(event);

        Mockito.verify(mapper).markFailed(
                Mockito.eq(3L),
                Mockito.contains("serialization failed")
        );
        Mockito.verify(mapper, Mockito.never()).scheduleRetry(
                Mockito.anyLong(), Mockito.any(), Mockito.anyString());
    }

    private ShopCacheInvalidationOutboxEvent event(Long id, int retryCount) {
        return new ShopCacheInvalidationOutboxEvent()
                .setId(id)
                .setEventId("event-" + id)
                .setShopId(7L)
                .setReason("shop-updated")
                .setStatus(ShopCacheInvalidationOutboxEvent.STATUS_PENDING)
                .setRetryCount(retryCount)
                .setCreatedTime(LocalDateTime.now());
    }
}
