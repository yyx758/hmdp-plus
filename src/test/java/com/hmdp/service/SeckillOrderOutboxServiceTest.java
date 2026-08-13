package com.hmdp.service;

import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SeckillOrderOutboxServiceTest {

    private SeckillOrderOutboxBatchWriter batchWriter;
    private SeckillOrderHandoffService handoffService;
    private SeckillOrderOutboxService service;

    @BeforeEach
    void setUp() {
        batchWriter = Mockito.mock(SeckillOrderOutboxBatchWriter.class);
        handoffService = Mockito.mock(SeckillOrderHandoffService.class);
        service = new SeckillOrderOutboxService(
                batchWriter, handoffService, 100, 20_000, 100, 2_000, 1);
    }

    @AfterEach
    void tearDown() {
        service.shutdown();
    }

    @Test
    void waitsUntilOutboxBatchTransactionReturnsCommittedResult() {
        Mockito.when(batchWriter.insertCommitted(Mockito.anyList()))
                .thenAnswer(invocation -> indexed(invocation.getArgument(0)));

        SeckillOrderOutboxEvent accepted = service.accept(1001L, 2L, 7L, false);

        assertEquals(1001L, accepted.getOrderId());
        Mockito.verify(batchWriter).insertCommitted(Mockito.anyList());
    }

    @Test
    void groupsConcurrentAcceptancesIntoOneTransaction() throws Exception {
        Mockito.when(batchWriter.insertCommitted(Mockito.anyList()))
                .thenAnswer(invocation -> indexed(invocation.getArgument(0)));

        CompletableFuture<SeckillOrderOutboxEvent> first = CompletableFuture.supplyAsync(
                () -> service.accept(1001L, 2L, 7L, false));
        CompletableFuture<SeckillOrderOutboxEvent> second = CompletableFuture.supplyAsync(
                () -> service.accept(1002L, 2L, 8L, false));

        assertEquals(1001L, first.get(2, TimeUnit.SECONDS).getOrderId());
        assertEquals(1002L, second.get(2, TimeUnit.SECONDS).getOrderId());
        org.mockito.ArgumentCaptor<List<SeckillOrderOutboxEvent>> batch =
                org.mockito.ArgumentCaptor.forClass(List.class);
        Mockito.verify(batchWriter).insertCommitted(batch.capture());
        assertEquals(2, batch.getValue().size());
    }

    @Test
    void doesNotAcknowledgeAcceptanceWhenBatchTransactionFails() {
        Mockito.when(batchWriter.insertCommitted(Mockito.anyList()))
                .thenThrow(new IllegalStateException("database unavailable"));

        assertThrows(IllegalStateException.class,
                () -> service.accept(1001L, 2L, 7L, false));
    }

    @Test
    void handoffRemovalIsExplicitlyAfterAcceptance() {
        SeckillOrderOutboxEvent event = SeckillOrderOutboxEvent.pending(1001L, 2L, 7L, false);
        Mockito.when(handoffService.buildMember(1001L, 7L, false))
                .thenReturn("1001|7|0");

        service.completeHandoff(event);

        Mockito.verify(handoffService).completeBatch(
                Mockito.eq(2L), Mockito.eq(java.util.Collections.singletonList(event)),
                Mockito.eq(java.util.Collections.singleton("1001|7|0")));
    }

    private Map<Long, SeckillOrderOutboxEvent> indexed(List<SeckillOrderOutboxEvent> events) {
        Map<Long, SeckillOrderOutboxEvent> result = new HashMap<>();
        for (SeckillOrderOutboxEvent event : events) {
            result.put(event.getOrderId(), event);
        }
        return result;
    }
}
