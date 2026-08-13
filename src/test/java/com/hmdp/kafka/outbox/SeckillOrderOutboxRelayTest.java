package com.hmdp.kafka.outbox;

import com.hmdp.kafka.SeckillOrderKafkaProducer;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.util.concurrent.SettableListenableFuture;
import org.springframework.dao.DeadlockLoserDataAccessException;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

class SeckillOrderOutboxRelayTest {

    private SeckillOrderOutboxMapper mapper;
    private SeckillOrderKafkaProducer producer;
    private SeckillOrderOutboxRelay relay;

    @BeforeEach
    void setUp() {
        mapper = Mockito.mock(SeckillOrderOutboxMapper.class);
        producer = Mockito.mock(SeckillOrderKafkaProducer.class);
        relay = new SeckillOrderOutboxRelay(
                mapper, producer, 100, 1, 30, 60, 1, 60);
    }

    @Test
    void sendsWholeBatchBeforeUpdatingSentState() {
        SettableListenableFuture first = successfulFuture();
        SettableListenableFuture second = successfulFuture();
        Mockito.when(producer.send(any())).thenReturn(first, second);

        relay.dispatchBatch(Arrays.asList(event(9L, 1001L), event(10L, 1002L)));

        Mockito.verify(producer, Mockito.times(2)).send(any());
        Mockito.verify(mapper).markSentBatch(
                eq(Arrays.asList(9L, 10L)), any(), any(LocalDateTime.class));
        Mockito.verify(mapper, Mockito.never()).scheduleRetryBatch(any(), any(), any(), any());
    }

    @Test
    void updatesSuccessAndFailureSubsetsSeparately() {
        SettableListenableFuture success = successfulFuture();
        SettableListenableFuture failure = new SettableListenableFuture();
        failure.setException(new IllegalStateException("broker unavailable"));
        Mockito.when(producer.send(any())).thenReturn(success, failure);

        relay.dispatchBatch(Arrays.asList(event(9L, 1001L), event(10L, 1002L)));

        Mockito.verify(mapper).markSentBatch(
                eq(Collections.singletonList(9L)), any(), any(LocalDateTime.class));
        Mockito.verify(mapper).scheduleRetryBatch(
                eq(Collections.singletonList(10L)), any(), any(LocalDateTime.class), any());
    }

    @Test
    void dispatchesOnlyRowsOwnedByThisRelayLease() {
        SeckillOrderOutboxEvent event = event(9L, 1001L);
        Mockito.when(mapper.findDispatchable(100)).thenReturn(Collections.singletonList(event));
        Mockito.when(mapper.claimRelayBatch(eq(Collections.singletonList(9L)), any(), any()))
                .thenReturn(0);

        relay.relayPending();

        Mockito.verifyNoInteractions(producer);
    }

    @Test
    void retriesBatchStateUpdateAfterDeadlock() {
        Mockito.when(producer.send(any())).thenReturn(successfulFuture());
        Mockito.when(mapper.markSentBatch(any(), any(), any()))
                .thenThrow(new DeadlockLoserDataAccessException("deadlock", null))
                .thenReturn(1);

        relay.dispatchBatch(Collections.singletonList(event(9L, 1001L)));

        Mockito.verify(mapper, Mockito.times(2)).markSentBatch(
                eq(Collections.singletonList(9L)), any(), any(LocalDateTime.class));
    }

    private SettableListenableFuture successfulFuture() {
        SettableListenableFuture future = new SettableListenableFuture();
        future.set(null);
        return future;
    }

    private SeckillOrderOutboxEvent event(long id, long orderId) {
        return SeckillOrderOutboxEvent.pending(orderId, 2L, orderId, false)
                .setId(id)
                .setCreatedTime(LocalDateTime.now());
    }
}
