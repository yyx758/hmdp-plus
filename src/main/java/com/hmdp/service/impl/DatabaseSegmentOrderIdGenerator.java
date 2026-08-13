package com.hmdp.service.impl;

import com.hmdp.service.IdSegmentAllocator;
import com.hmdp.service.OrderIdGenerator;
import com.hmdp.utils.RedisIdWorker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class DatabaseSegmentOrderIdGenerator implements OrderIdGenerator {

    private static final String BIZ_TAG = "voucher-order";

    private final IdSegmentAllocator allocator;
    private final RedisIdWorker redisIdWorker;
    private final String mode;
    private final int step;
    private final int prefetchThreshold;
    private final ExecutorService prefetchExecutor;
    private final Object refillMonitor = new Object();
    private final AtomicBoolean prefetching = new AtomicBoolean();
    private volatile LocalRange current;
    private volatile LocalRange next;

    public DatabaseSegmentOrderIdGenerator(
            IdSegmentAllocator allocator,
            RedisIdWorker redisIdWorker,
            @Value("${hmdp.seckill.order-id.mode:db-segment}") String mode,
            @Value("${hmdp.seckill.order-id.segment-step:10000}") int step,
            @Value("${hmdp.seckill.order-id.prefetch-percent:20}") int prefetchPercent) {
        this.allocator = allocator;
        this.redisIdWorker = redisIdWorker;
        this.mode = mode;
        this.step = Math.max(100, step);
        int percent = Math.max(1, Math.min(90, prefetchPercent));
        this.prefetchThreshold = Math.max(1, this.step * percent / 100);
        this.prefetchExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "order-id-segment-prefetch");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public long nextId() {
        if ("redis".equalsIgnoreCase(mode)) {
            return redisIdWorker.nextId("order");
        }
        while (true) {
            LocalRange range = current;
            if (range != null) {
                long id = range.next.getAndIncrement();
                if (id < range.endExclusive) {
                    if (range.endExclusive - id <= prefetchThreshold) {
                        prefetch();
                    }
                    return id;
                }
            }
            refill();
        }
    }

    private void refill() {
        synchronized (refillMonitor) {
            LocalRange range = current;
            if (range != null && range.next.get() < range.endExclusive) {
                return;
            }
            if (next != null) {
                current = next;
                next = null;
            } else {
                current = allocateRange();
            }
            prefetch();
        }
    }

    private void prefetch() {
        if (next != null || !prefetching.compareAndSet(false, true)) {
            return;
        }
        prefetchExecutor.execute(() -> {
            try {
                LocalRange allocated = allocateRange();
                synchronized (refillMonitor) {
                    if (next == null) {
                        next = allocated;
                    }
                }
            } catch (RuntimeException e) {
                log.error("Failed to prefetch voucher-order ID segment", e);
            } finally {
                prefetching.set(false);
            }
        });
    }

    private LocalRange allocateRange() {
        IdSegmentAllocator.Range range = allocator.allocate(BIZ_TAG, step);
        return new LocalRange(range.getStartInclusive(), range.getEndExclusive());
    }

    @PreDestroy
    public void shutdown() {
        prefetchExecutor.shutdownNow();
    }

    private static final class LocalRange {
        private final AtomicLong next;
        private final long endExclusive;

        private LocalRange(long startInclusive, long endExclusive) {
            this.next = new AtomicLong(startInclusive);
            this.endExclusive = endExclusive;
        }
    }
}
