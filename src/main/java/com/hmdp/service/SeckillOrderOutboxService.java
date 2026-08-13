package com.hmdp.service;

import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class SeckillOrderOutboxService {

    private final SeckillOrderOutboxBatchWriter batchWriter;
    private final SeckillOrderHandoffService handoffService;
    private final ArrayBlockingQueue<PendingAcceptance> queue;
    private final int batchSize;
    private final long batchWindowNanos;
    private final long awaitTimeoutMillis;
    private final List<Thread> workers;
    private volatile boolean running = true;

    public SeckillOrderOutboxService(
            SeckillOrderOutboxBatchWriter batchWriter,
            SeckillOrderHandoffService handoffService,
            @Value("${hmdp.kafka.seckill-order.outbox.accept-batch-size:100}") int batchSize,
            @Value("${hmdp.kafka.seckill-order.outbox.accept-batch-window-micros:1000}")
            long batchWindowMicros,
            @Value("${hmdp.kafka.seckill-order.outbox.accept-queue-capacity:4096}")
            int queueCapacity,
            @Value("${hmdp.kafka.seckill-order.outbox.accept-timeout-ms:5000}")
            long awaitTimeoutMillis,
            @Value("${hmdp.kafka.seckill-order.outbox.accept-writer-count:1}")
            int writerCount) {
        this.batchWriter = batchWriter;
        this.handoffService = handoffService;
        this.batchSize = Math.max(1, batchSize);
        this.batchWindowNanos = TimeUnit.MICROSECONDS.toNanos(Math.max(0, batchWindowMicros));
        this.queue = new ArrayBlockingQueue<>(Math.max(this.batchSize, queueCapacity));
        this.awaitTimeoutMillis = Math.max(1, awaitTimeoutMillis);
        List<Thread> writerThreads = new ArrayList<>();
        for (int index = 0; index < Math.max(1, writerCount); index++) {
            Thread writer = new Thread(
                    this::runWriter, "seckill-outbox-group-commit-" + index);
            writer.setDaemon(true);
            writer.start();
            writerThreads.add(writer);
        }
        this.workers = Collections.unmodifiableList(writerThreads);
    }

    public SeckillOrderOutboxEvent accept(
            Long orderId, Long voucherId, Long userId, boolean autoIssued) {
        SeckillOrderOutboxEvent event = SeckillOrderOutboxEvent.pending(
                orderId, voucherId, userId, autoIssued);
        PendingAcceptance pending = new PendingAcceptance(event);
        try {
            if (!queue.offer(pending, awaitTimeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("秒杀订单Outbox组提交队列已满");
            }
            return pending.result.get(awaitTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("等待秒杀订单Outbox提交时被中断", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IllegalStateException("秒杀订单Outbox提交失败", cause);
        } catch (TimeoutException e) {
            throw new IllegalStateException("等待秒杀订单Outbox提交超时", e);
        }
    }

    public void completeHandoff(SeckillOrderOutboxEvent event) {
        String member = handoffService.buildMember(
                event.getOrderId(), event.getUserId(),
                Boolean.TRUE.equals(event.getAutoIssued()));
        handoffService.completeBatch(
                event.getVoucherId(), Collections.singletonList(event),
                Collections.singleton(member));
    }

    private void runWriter() {
        while (running || !queue.isEmpty()) {
            List<PendingAcceptance> batch = new ArrayList<>(batchSize);
            try {
                PendingAcceptance first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                long deadline = System.nanoTime() + batchWindowNanos;
                while (batch.size() < batchSize) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        break;
                    }
                    PendingAcceptance next = queue.poll(remaining, TimeUnit.NANOSECONDS);
                    if (next == null) {
                        break;
                    }
                    batch.add(next);
                }
                commit(batch);
            } catch (InterruptedException e) {
                if (running) {
                    Thread.currentThread().interrupt();
                }
            } catch (RuntimeException e) {
                fail(batch, e);
            }
        }
    }

    private void commit(List<PendingAcceptance> batch) {
        List<SeckillOrderOutboxEvent> events = new ArrayList<>(batch.size());
        for (PendingAcceptance pending : batch) {
            events.add(pending.event);
        }
        Map<Long, SeckillOrderOutboxEvent> committed = batchWriter.insertCommitted(events);
        for (PendingAcceptance pending : batch) {
            SeckillOrderOutboxEvent event = committed.get(pending.event.getOrderId());
            if (event == null) {
                throw new IllegalStateException(
                        "秒杀订单Outbox提交结果缺失，orderId=" + pending.event.getOrderId());
            }
            pending.result.complete(event);
        }
    }

    private void fail(List<PendingAcceptance> batch, RuntimeException failure) {
        for (PendingAcceptance pending : batch) {
            pending.result.completeExceptionally(failure);
        }
    }

    @PreDestroy
    public void shutdown() {
        running = false;
        for (Thread worker : workers) {
            worker.interrupt();
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(awaitTimeoutMillis);
        for (Thread worker : workers) {
            try {
                long remainingMillis = TimeUnit.NANOSECONDS.toMillis(
                        Math.max(0L, deadline - System.nanoTime()));
                worker.join(remainingMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        PendingAcceptance pending;
        while ((pending = queue.poll()) != null) {
            pending.result.completeExceptionally(
                    new IllegalStateException("应用关闭，Outbox请求未提交"));
        }
    }

    private static final class PendingAcceptance {
        private final SeckillOrderOutboxEvent event;
        private final CompletableFuture<SeckillOrderOutboxEvent> result =
                new CompletableFuture<>();

        private PendingAcceptance(SeckillOrderOutboxEvent event) {
            this.event = event;
        }
    }
}
