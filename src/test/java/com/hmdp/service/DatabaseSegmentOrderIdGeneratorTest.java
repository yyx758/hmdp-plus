package com.hmdp.service;

import com.hmdp.service.impl.DatabaseSegmentOrderIdGenerator;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseSegmentOrderIdGeneratorTest {

    private final List<DatabaseSegmentOrderIdGenerator> generators = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (DatabaseSegmentOrderIdGenerator generator : generators) {
            generator.shutdown();
        }
    }

    @Test
    void generatesUniqueIdsConcurrentlyWithoutPerIdRemoteCalls() throws Exception {
        AtomicLong highWaterMark = new AtomicLong(1_000_000L);
        IdSegmentAllocator allocator = Mockito.mock(IdSegmentAllocator.class);
        Mockito.when(allocator.allocate(Mockito.eq("voucher-order"), Mockito.eq(100)))
                .thenAnswer(invocation -> {
                    long start = highWaterMark.getAndAdd(100L);
                    return new IdSegmentAllocator.Range(start, start + 100L);
                });
        DatabaseSegmentOrderIdGenerator generator = generator(allocator, "db-segment", 100, 20);

        int threadCount = 16;
        int idsPerThread = 500;
        Set<Long> ids = Collections.synchronizedSet(new HashSet<>());
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int index = 0; index < threadCount; index++) {
            Thread thread = new Thread(() -> {
                ready.countDown();
                await(start);
                for (int count = 0; count < idsPerThread; count++) {
                    ids.add(generator.nextId());
                }
            });
            threads.add(thread);
            thread.start();
        }
        ready.await();
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(threadCount * idsPerThread, ids.size());
        assertTrue(ids.stream().allMatch(id -> id >= 1_000_000L));
        Mockito.verify(allocator, Mockito.atMost(90))
                .allocate("voucher-order", 100);
    }

    @Test
    void twoInstancesReceiveDisjointRanges() {
        AtomicLong highWaterMark = new AtomicLong(10_000L);
        IdSegmentAllocator allocator = Mockito.mock(IdSegmentAllocator.class);
        Mockito.when(allocator.allocate(Mockito.eq("voucher-order"), Mockito.eq(100)))
                .thenAnswer(invocation -> {
                    long start = highWaterMark.getAndAdd(100L);
                    return new IdSegmentAllocator.Range(start, start + 100L);
                });
        DatabaseSegmentOrderIdGenerator first = generator(allocator, "db-segment", 100, 20);
        DatabaseSegmentOrderIdGenerator second = generator(allocator, "db-segment", 100, 20);

        Set<Long> ids = new HashSet<>();
        for (int index = 0; index < 100; index++) {
            ids.add(first.nextId());
            ids.add(second.nextId());
        }

        assertEquals(200, ids.size());
    }

    @Test
    void redisModePreservesRollbackOption() {
        RedisIdWorker redisIdWorker = Mockito.mock(RedisIdWorker.class);
        Mockito.when(redisIdWorker.nextId("order")).thenReturn(77L);
        DatabaseSegmentOrderIdGenerator generator = new DatabaseSegmentOrderIdGenerator(
                Mockito.mock(IdSegmentAllocator.class), redisIdWorker,
                "redis", 100, 20);
        generators.add(generator);

        assertEquals(77L, generator.nextId());
        Mockito.verify(redisIdWorker).nextId("order");
    }

    private DatabaseSegmentOrderIdGenerator generator(
            IdSegmentAllocator allocator, String mode, int step, int prefetchPercent) {
        DatabaseSegmentOrderIdGenerator generator = new DatabaseSegmentOrderIdGenerator(
                allocator, Mockito.mock(RedisIdWorker.class), mode, step, prefetchPercent);
        generators.add(generator);
        return generator;
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
