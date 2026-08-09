package com.hmdp.service;

import com.hmdp.exception.OrderIdConflictException;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VoucherOrderDeadLetterTest {

    private VoucherOrderServiceImpl orderService;
    private StringRedisTemplate redisTemplate;
    private IVoucherOrderPersistenceService persistenceService;
    private StreamOperations<String, Object, Object> streamOperations;
    private HashOperations<String, Object, Object> hashOperations;
    private SetOperations<String, String> setOperations;
    private MapRecord<String, Object, Object> record;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        orderService = new VoucherOrderServiceImpl();
        persistenceService = Mockito.mock(IVoucherOrderPersistenceService.class);
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        streamOperations = Mockito.mock(StreamOperations.class);
        hashOperations = Mockito.mock(HashOperations.class);
        setOperations = Mockito.mock(SetOperations.class);
        record = Mockito.mock(MapRecord.class);

        Map<Object, Object> order = new HashMap<>();
        order.put("id", "1001");
        order.put("userId", "7");
        order.put("voucherId", "2");
        Mockito.when(record.getId()).thenReturn(RecordId.of("1-0"));
        Mockito.when(record.getValue()).thenReturn(order);
        Mockito.when(redisTemplate.opsForStream()).thenReturn(streamOperations);
        Mockito.when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        Mockito.when(redisTemplate.opsForSet()).thenReturn(setOperations);
        Mockito.when(setOperations.isMember("seckill:order:2", "7")).thenReturn(true);
        Mockito.when(persistenceService.findOrderId(Mockito.anyLong(), Mockito.anyLong()))
                .thenReturn(null);

        ReflectionTestUtils.setField(orderService, "stringRedisTemplate", redisTemplate);
        ReflectionTestUtils.setField(orderService, "voucherOrderPersistenceService", persistenceService);
    }

    @Test
    void keepsFailedMessagePendingBeforeRetryLimit() {
        Mockito.doThrow(new IllegalStateException("mysql unavailable"))
                .when(persistenceService).createVoucherOrder(Mockito.any());
        Mockito.when(hashOperations.increment("stream.orders:retry", "1-0", 1L)).thenReturn(1L);

        boolean processed = invokeProcessOrderRecord();

        assertFalse(processed);
        Mockito.verify(streamOperations, Mockito.never())
                .acknowledge(Mockito.anyString(), Mockito.anyString(), Mockito.any(RecordId.class));
        Mockito.verify(streamOperations, Mockito.never()).add(Mockito.any(MapRecord.class));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void movesMessageToDeadLetterBeforeAcknowledgingAtRetryLimit() {
        Mockito.doThrow(new IllegalStateException("mysql unavailable"))
                .when(persistenceService).createVoucherOrder(Mockito.any());
        Mockito.when(hashOperations.increment("stream.orders:retry", "1-0", 1L)).thenReturn(3L);
        Mockito.when(streamOperations.add(Mockito.any(MapRecord.class))).thenReturn(RecordId.of("9-0"));

        boolean processed = invokeProcessOrderRecord();

        assertTrue(processed);
        ArgumentCaptor<MapRecord> deadLetterCaptor = ArgumentCaptor.forClass(MapRecord.class);
        Mockito.verify(streamOperations).add(deadLetterCaptor.capture());
        Map<Object, Object> deadLetter = (Map<Object, Object>) deadLetterCaptor.getValue().getValue();
        assertEquals("1-0", deadLetter.get("originalMessageId"));
        assertEquals("3", deadLetter.get("retryCount"));
        assertTrue(deadLetter.get("error").toString().contains("mysql unavailable"));
        assertEquals("CHECKED_ORDER_MISSING", deadLetter.get("consistencyStatus"));
        assertEquals("false", deadLetter.get("mysqlOrderExists"));
        assertEquals("true", deadLetter.get("redisPurchaseMarked"));
        Mockito.verify(streamOperations).acknowledge("stream.orders", "g1", RecordId.of("1-0"));
        Mockito.verify(hashOperations).delete("stream.orders:retry", "1-0");
    }

    @Test
    void doesNotAcknowledgeOriginalMessageWhenDeadLetterWriteFails() {
        Mockito.doThrow(new IllegalStateException("mysql unavailable"))
                .when(persistenceService).createVoucherOrder(Mockito.any());
        Mockito.when(hashOperations.increment("stream.orders:retry", "1-0", 1L)).thenReturn(3L);
        Mockito.when(streamOperations.add(Mockito.any(MapRecord.class))).thenReturn(null);

        boolean processed = invokeProcessOrderRecord();

        assertFalse(processed);
        Mockito.verify(streamOperations, Mockito.never())
                .acknowledge(Mockito.anyString(), Mockito.anyString(), Mockito.any(RecordId.class));
        Mockito.verify(hashOperations, Mockito.never()).delete("stream.orders:retry", "1-0");
    }

    @Test
    void acknowledgesAndClearsRetryCountAfterDatabaseSuccess() {
        boolean processed = invokeProcessOrderRecord();

        assertTrue(processed);
        Mockito.verify(streamOperations).acknowledge("stream.orders", "g1", RecordId.of("1-0"));
        Mockito.verify(hashOperations).delete("stream.orders:retry", "1-0");
        Mockito.verify(streamOperations, Mockito.never()).add(Mockito.any(MapRecord.class));
    }

    @Test
    void retryCounterCleanupFailureDoesNotTurnAcknowledgedOrderIntoFailure() {
        Mockito.doThrow(new IllegalStateException("redis hash unavailable"))
                .when(hashOperations).delete("stream.orders:retry", "1-0");

        boolean processed = invokeProcessOrderRecord();

        assertTrue(processed);
        Mockito.verify(streamOperations).acknowledge("stream.orders", "g1", RecordId.of("1-0"));
        Mockito.verify(hashOperations, Mockito.never())
                .increment(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong());
    }

    @Test
    void processesEveryRecordInBatchAndAcknowledgesEachOne() {
        MapRecord<String, Object, Object> secondRecord = Mockito.mock(MapRecord.class);
        Map<Object, Object> secondOrder = new HashMap<>();
        secondOrder.put("id", "1002");
        secondOrder.put("userId", "8");
        secondOrder.put("voucherId", "2");
        Mockito.when(secondRecord.getId()).thenReturn(RecordId.of("2-0"));
        Mockito.when(secondRecord.getValue()).thenReturn(secondOrder);

        boolean processed = Boolean.TRUE.equals(ReflectionTestUtils.invokeMethod(
                orderService, "processOrderBatch", Arrays.asList(record, secondRecord)
        ));

        assertTrue(processed);
        Mockito.verify(persistenceService).createVoucherOrders(Mockito.argThat(orders -> orders.size() == 2));
        Mockito.verify(streamOperations).acknowledge(
                Mockito.eq("stream.orders"),
                Mockito.eq("g1"),
                Mockito.eq(RecordId.of("1-0")),
                Mockito.eq(RecordId.of("2-0"))
        );
    }

    @Test
    void isolatesBadRecordByBisectingWithoutDowngradingEveryOrderToSingleInsert() {
        MapRecord<String, Object, Object> second = orderRecord("2-0", "1002", "8");
        MapRecord<String, Object, Object> bad = orderRecord("3-0", "1003", "9");
        MapRecord<String, Object, Object> fourth = orderRecord("4-0", "1004", "10");

        Mockito.doAnswer(invocation -> {
            List<com.hmdp.entity.VoucherOrder> orders = invocation.getArgument(0);
            if (orders.stream().anyMatch(order -> Long.valueOf(9L).equals(order.getUserId()))) {
                throw new IllegalArgumentException("bad order");
            }
            return null;
        }).when(persistenceService).createVoucherOrders(Mockito.anyList());
        Mockito.when(hashOperations.increment("stream.orders:retry", "3-0", 1L)).thenReturn(1L);

        boolean processed = Boolean.TRUE.equals(ReflectionTestUtils.invokeMethod(
                orderService, "processOrderBatch", Arrays.asList(record, second, bad, fourth)
        ));

        assertFalse(processed);
        Mockito.verify(persistenceService, Mockito.never()).createVoucherOrder(Mockito.any());
        Mockito.verify(streamOperations).acknowledge(
                Mockito.eq("stream.orders"), Mockito.eq("g1"),
                Mockito.eq(RecordId.of("1-0")), Mockito.eq(RecordId.of("2-0"))
        );
        Mockito.verify(streamOperations).acknowledge(
                "stream.orders", "g1", RecordId.of("4-0")
        );
        Mockito.verify(streamOperations, Mockito.never()).acknowledge(
                "stream.orders", "g1", RecordId.of("3-0")
        );
    }

    @Test
    void doesNotBisectWholeBatchWhenDatabaseIsTemporarilyUnavailable() {
        MapRecord<String, Object, Object> second = orderRecord("2-0", "1002", "8");
        Mockito.doThrow(new TransientDataAccessResourceException("mysql unavailable"))
                .when(persistenceService).createVoucherOrders(Mockito.anyList());
        Mockito.when(hashOperations.increment(Mockito.eq("stream.orders:retry"), Mockito.anyString(), Mockito.eq(1L)))
                .thenReturn(1L);

        boolean processed = Boolean.TRUE.equals(ReflectionTestUtils.invokeMethod(
                orderService, "processOrderBatch", Arrays.asList(record, second)
        ));

        assertFalse(processed);
        Mockito.verify(persistenceService, Mockito.times(1)).createVoucherOrders(Mockito.anyList());
        Mockito.verify(persistenceService, Mockito.never()).createVoucherOrder(Mockito.any());
        Mockito.verify(persistenceService, Mockito.never())
                .findOrderId(Mockito.anyLong(), Mockito.anyLong());
        Mockito.verify(streamOperations, Mockito.never())
                .acknowledge(Mockito.anyString(), Mockito.anyString(), Mockito.any(RecordId.class));
    }

    @Test
    void ackFailureDoesNotIncreaseBusinessRetryCounterOrCreateDeadLetter() {
        Mockito.doThrow(new IllegalStateException("redis unavailable"))
                .when(streamOperations).acknowledge("stream.orders", "g1", RecordId.of("1-0"));

        boolean processed = invokeProcessOrderRecord();

        assertFalse(processed);
        Mockito.verify(hashOperations, Mockito.never())
                .increment(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong());
        Mockito.verify(streamOperations, Mockito.never()).add(Mockito.any(MapRecord.class));
    }

    @Test
    void finalConsistencyCheckAcknowledgesWhenMysqlOrderAlreadyExists() {
        Mockito.doThrow(new IllegalStateException("write response lost"))
                .when(persistenceService).createVoucherOrder(Mockito.any());
        Mockito.when(hashOperations.increment("stream.orders:retry", "1-0", 1L)).thenReturn(3L);
        Mockito.when(persistenceService.findOrderId(7L, 2L)).thenReturn(1001L);

        boolean processed = invokeProcessOrderRecord();

        assertTrue(processed);
        Mockito.verify(streamOperations).acknowledge("stream.orders", "g1", RecordId.of("1-0"));
        Mockito.verify(streamOperations, Mockito.never()).add(Mockito.any(MapRecord.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void compensatesRedisAndReturnsExistingOrderWhenOrderIdsConflict() {
        Mockito.doThrow(new OrderIdConflictException(1001L, 9001L))
                .when(persistenceService).createVoucherOrder(Mockito.any());
        Mockito.when(persistenceService.findOrderId(7L, 2L)).thenReturn(9001L);
        Mockito.when(redisTemplate.execute(
                Mockito.any(RedisScript.class),
                Mockito.eq(Collections.emptyList()),
                Mockito.eq("2"),
                Mockito.eq("7"),
                Mockito.eq("1001"),
                Mockito.eq("9001"),
                Mockito.eq("1-0"),
                Mockito.anyString(),
                Mockito.anyString()
        )).thenReturn(1L);

        boolean processed = invokeProcessOrderRecord();

        assertTrue(processed);
        Mockito.verify(hashOperations, Mockito.never())
                .increment(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong());
        // ACK包含在冲突补偿Lua中，Java侧不能再单独ACK。
        Mockito.verify(streamOperations, Mockito.never())
                .acknowledge(Mockito.anyString(), Mockito.anyString(), Mockito.any(RecordId.class));
    }

    @Test
    void createsStableUniqueConsumerNameForEachWorker() {
        ReflectionTestUtils.setField(orderService, "consumerInstanceId", "instance-18081");

        String first = ReflectionTestUtils.invokeMethod(orderService, "buildConsumerName", 1);
        String second = ReflectionTestUtils.invokeMethod(orderService, "buildConsumerName", 2);

        assertEquals("instance-18081-worker-1", first);
        assertEquals("instance-18081-worker-2", second);
    }

    @Test
    void recognizesNoGroupErrorFromNestedRedisException() {
        RuntimeException error = new RuntimeException(
                "redis read failed",
                new RuntimeException("NOGROUP No such key 'stream.orders' or consumer group 'g1'")
        );

        boolean noGroup = Boolean.TRUE.equals(
                ReflectionTestUtils.invokeMethod(orderService, "isNoGroupException", error)
        );

        assertTrue(noGroup);
    }

    @Test
    void recreatesStreamGroupAfterNoGroupError() {
        ReflectionTestUtils.invokeMethod(orderService, "recoverStreamGroup");

        Mockito.verify(streamOperations).createGroup(
                Mockito.eq("stream.orders"),
                Mockito.any(ReadOffset.class),
                Mockito.eq("g1")
        );
    }

    private boolean invokeProcessOrderRecord() {
        return Boolean.TRUE.equals(ReflectionTestUtils.invokeMethod(
                orderService, "processOrderRecord", record
        ));
    }

    @SuppressWarnings("unchecked")
    private MapRecord<String, Object, Object> orderRecord(String messageId,
                                                          String orderId,
                                                          String userId) {
        MapRecord<String, Object, Object> result = Mockito.mock(MapRecord.class);
        Map<Object, Object> order = new HashMap<>();
        order.put("id", orderId);
        order.put("userId", userId);
        order.put("voucherId", "2");
        Mockito.when(result.getId()).thenReturn(RecordId.of(messageId));
        Mockito.when(result.getValue()).thenReturn(order);
        return result;
    }
}
