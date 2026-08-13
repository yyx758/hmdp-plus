package com.hmdp.kafka.outbox;

import com.hmdp.mapper.SeckillOrderOutboxMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@Transactional
@TestPropertySource(properties = {
        "hmdp.kafka.seckill-order.consumer-group=hmdp-batch-mapper-integration-test",
        "hmdp.kafka.seckill-order.dlt-consumer-group=hmdp-batch-mapper-integration-test-dlt"
})
class SeckillOrderOutboxMapperBatchIntegrationTest {

    @Autowired
    private SeckillOrderOutboxMapper mapper;

    @Test
    void executesBatchLeaseSendRetryAndCompletionSql() {
        long suffix = System.nanoTime();
        long firstOrderId = 8_700_000_000_000_000L + suffix % 100_000_000L;
        long secondOrderId = firstOrderId + 1;
        assertEquals(2, mapper.insertIgnoreBatch(Arrays.asList(
                event(firstOrderId), event(secondOrderId))));

        SeckillOrderOutboxEvent first = mapper.findByOrderId(firstOrderId);
        SeckillOrderOutboxEvent second = mapper.findByOrderId(secondOrderId);
        List<Long> ids = Arrays.asList(first.getId(), second.getId());
        String owner = "batch-sql-test-" + suffix;

        assertEquals(2, mapper.claimRelayBatch(
                ids, owner, LocalDateTime.now().plusSeconds(30)));
        assertEquals(2, mapper.findClaimedBatch(ids, owner).size());
        assertEquals(1, mapper.markSentBatch(
                Collections.singletonList(first.getId()), owner,
                LocalDateTime.now().plusSeconds(60)));
        assertEquals(1, mapper.scheduleRetryBatch(
                Collections.singletonList(second.getId()), owner,
                LocalDateTime.now().plusSeconds(1), "test failure"));

        List<Long> orderIds = Arrays.asList(firstOrderId, secondOrderId);
        List<Long> completionIds = mapper.findIdsByOrderIds(orderIds);
        java.util.Collections.sort(completionIds);
        assertEquals(2, mapper.markCompletedBatchByIds(completionIds));
        assertEquals(2, mapper.countCompletedBatchByIds(completionIds));
        assertEquals(2, mapper.findByOrderIds(orderIds).size());
    }

    private SeckillOrderOutboxEvent event(long orderId) {
        return SeckillOrderOutboxEvent.pending(orderId, 987654399L, orderId, false)
                .setNextRetryTime(LocalDateTime.now().minusSeconds(1));
    }
}
