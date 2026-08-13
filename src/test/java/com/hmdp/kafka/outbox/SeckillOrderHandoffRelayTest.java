package com.hmdp.kafka.outbox;

import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.SeckillOrderHandoffService;
import com.hmdp.service.SeckillOrderOutboxBatchWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;

class SeckillOrderHandoffRelayTest {

    private SeckillOrderHandoffService handoffService;
    private SeckillOrderOutboxBatchWriter batchWriter;
    private SeckillOrderHandoffRelay relay;

    @BeforeEach
    void setUp() {
        handoffService = Mockito.mock(SeckillOrderHandoffService.class);
        batchWriter = Mockito.mock(SeckillOrderOutboxBatchWriter.class);
        relay = new SeckillOrderHandoffRelay(
                Mockito.mock(ISeckillVoucherService.class), handoffService, batchWriter,
                Mockito.mock(StringRedisTemplate.class), 100, 20, 100, 5000, 15, 60);
    }

    @Test
    void batchIsRemovedOnlyAfterOutboxCommitReturns() {
        LinkedHashSet<String> members = new LinkedHashSet<>(
                Arrays.asList("1001|7|0", "1002|8|0"));
        Mockito.when(handoffService.findFirst(2L, 100)).thenReturn(members);
        Mockito.when(handoffService.parse(2L, "1001|7|0")).thenReturn(
                SeckillOrderOutboxEvent.pending(1001L, 2L, 7L, false));
        Mockito.when(handoffService.parse(2L, "1002|8|0")).thenReturn(
                SeckillOrderOutboxEvent.pending(1002L, 2L, 8L, false));

        relay.relayOneRound(Collections.singletonList(2L));

        org.mockito.InOrder order = Mockito.inOrder(batchWriter, handoffService);
        order.verify(batchWriter).insertCommitted(Mockito.argThat(events -> events.size() == 2));
        order.verify(handoffService).completeBatch(
                Mockito.eq(2L),
                Mockito.<java.util.List<SeckillOrderOutboxEvent>>argThat(
                        events -> events.size() == 2),
                Mockito.eq(members));
    }

    @Test
    void transientDatabaseFailureLeavesEntireBatchInRedis() {
        LinkedHashSet<String> members = new LinkedHashSet<>(
                Collections.singletonList("1001|7|0"));
        Mockito.when(handoffService.findFirst(2L, 100)).thenReturn(members);
        Mockito.when(handoffService.parse(2L, "1001|7|0")).thenReturn(
                SeckillOrderOutboxEvent.pending(1001L, 2L, 7L, false));
        Mockito.when(batchWriter.insertCommitted(Mockito.anyList()))
                .thenThrow(new IllegalStateException("database unavailable"));

        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> relay.relayOneRound(Collections.singletonList(2L)));
        Mockito.verify(handoffService, Mockito.never())
                .completeBatch(Mockito.anyLong(), Mockito.anyList(), Mockito.anySet());
    }
}
