package com.hmdp.kafka.outbox;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.SeckillOrderHandoffService;
import com.hmdp.service.SeckillOrderOutboxBatchWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataIntegrityViolationException;
import com.hmdp.exception.SeckillOutboxEventConflictException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_HANDOFF_QUARANTINE_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_HANDOFF_RELAY_LEASE_KEY;

/** Continuously transfers Redis-accepted orders into the durable MySQL outbox. */
@Slf4j
@Component
public class SeckillOrderHandoffRelay implements SmartLifecycle {

    private static final DefaultRedisScript<Long> RENEW_SCRIPT =
            loadScript("lua/compare_expire.lua");
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT =
            loadScript("lua/compare_delete.lua");
    private static final DefaultRedisScript<Long> QUARANTINE_SCRIPT =
            loadScript("lua/seckill_handoff_quarantine.lua");

    private final ISeckillVoucherService voucherService;
    private final SeckillOrderHandoffService handoffService;
    private final SeckillOrderOutboxBatchWriter batchWriter;
    private final StringRedisTemplate redisTemplate;
    private final int batchSize;
    private final long idlePollMillis;
    private final long initialFailureBackoffMillis;
    private final long maxFailureBackoffMillis;
    private final long leaseMillis;
    private final long voucherRefreshMillis;
    private final String owner = UUID.randomUUID().toString();
    private volatile boolean running;
    private volatile Thread worker;

    public SeckillOrderHandoffRelay(
            ISeckillVoucherService voucherService,
            SeckillOrderHandoffService handoffService,
            SeckillOrderOutboxBatchWriter batchWriter,
            StringRedisTemplate redisTemplate,
            @Value("${hmdp.kafka.seckill-order.handoff.batch-size:100}") int batchSize,
            @Value("${hmdp.kafka.seckill-order.handoff.idle-poll-ms:20}") long idlePollMillis,
            @Value("${hmdp.kafka.seckill-order.handoff.failure-backoff-initial-ms:100}")
            long initialFailureBackoffMillis,
            @Value("${hmdp.kafka.seckill-order.handoff.failure-backoff-max-ms:5000}")
            long maxFailureBackoffMillis,
            @Value("${hmdp.kafka.seckill-order.handoff.relay-lease-seconds:15}")
            long relayLeaseSeconds,
            @Value("${hmdp.kafka.seckill-order.handoff.voucher-refresh-seconds:60}")
            long voucherRefreshSeconds) {
        this.voucherService = voucherService;
        this.handoffService = handoffService;
        this.batchWriter = batchWriter;
        this.redisTemplate = redisTemplate;
        this.batchSize = Math.max(1, batchSize);
        this.idlePollMillis = Math.max(1L, idlePollMillis);
        this.initialFailureBackoffMillis = Math.max(1L, initialFailureBackoffMillis);
        this.maxFailureBackoffMillis = Math.max(
                this.initialFailureBackoffMillis, maxFailureBackoffMillis);
        this.leaseMillis = TimeUnit.SECONDS.toMillis(Math.max(3L, relayLeaseSeconds));
        this.voucherRefreshMillis = TimeUnit.SECONDS.toMillis(
                Math.max(1L, voucherRefreshSeconds));
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        worker = new Thread(this::run, "seckill-handoff-relay");
        worker.setDaemon(true);
        worker.start();
    }

    private void run() {
        List<Long> voucherIds = Collections.emptyList();
        long refreshAt = 0L;
        long failureBackoff = initialFailureBackoffMillis;
        while (running) {
            try {
                if (!acquireOrRenewLease()) {
                    sleep(idlePollMillis);
                    continue;
                }
                long now = System.currentTimeMillis();
                if (now >= refreshAt) {
                    voucherIds = loadVoucherIds();
                    refreshAt = now + voucherRefreshMillis;
                }
                boolean handled = relayOneRound(voucherIds);
                failureBackoff = initialFailureBackoffMillis;
                if (!handled) {
                    sleep(idlePollMillis);
                }
            } catch (RuntimeException e) {
                log.error("Seckill handoff relay failed; batch remains in Redis", e);
                sleep(failureBackoff);
                failureBackoff = Math.min(maxFailureBackoffMillis, failureBackoff * 2L);
            }
        }
        redisTemplate.execute(
                UNLOCK_SCRIPT, Collections.singletonList(SECKILL_ORDER_HANDOFF_RELAY_LEASE_KEY),
                owner);
    }

    boolean relayOneRound(List<Long> voucherIds) {
        boolean handled = false;
        for (Long voucherId : voucherIds) {
            Set<String> members = handoffService.findFirst(voucherId, batchSize);
            if (members == null || members.isEmpty()) {
                continue;
            }
            handled = true;
            List<SeckillOrderOutboxEvent> events = new ArrayList<>(members.size());
            Set<String> validMembers = new java.util.LinkedHashSet<>();
            for (String member : members) {
                SeckillOrderOutboxEvent event = handoffService.parse(voucherId, member);
                if (event == null) {
                    quarantine(voucherId, member, "malformed");
                    continue;
                }
                events.add(event);
                validMembers.add(member);
            }
            persistWithIsolation(voucherId, events, validMembers);
        }
        return handled;
    }

    private void persistWithIsolation(
            Long voucherId, List<SeckillOrderOutboxEvent> events, Set<String> members) {
        if (events.isEmpty()) {
            return;
        }
        try {
            batchWriter.insertCommitted(events);
            handoffService.completeBatch(voucherId, events, members);
        } catch (DataIntegrityViolationException | SeckillOutboxEventConflictException e) {
            if (events.size() == 1) {
                quarantine(voucherId, members.iterator().next(), "data-integrity");
                log.error("Seckill handoff quarantined, voucherId={}, member={}",
                        voucherId, members.iterator().next(), e);
                return;
            }
            int middle = events.size() / 2;
            List<SeckillOrderOutboxEvent> leftEvents = events.subList(0, middle);
            List<SeckillOrderOutboxEvent> rightEvents = events.subList(middle, events.size());
            List<String> orderedMembers = new ArrayList<>(members);
            persistWithIsolation(voucherId, leftEvents,
                    new java.util.LinkedHashSet<>(orderedMembers.subList(0, middle)));
            persistWithIsolation(voucherId, rightEvents,
                    new java.util.LinkedHashSet<>(orderedMembers.subList(middle, orderedMembers.size())));
        }
    }

    private void quarantine(Long voucherId, String member, String reason) {
        redisTemplate.execute(QUARANTINE_SCRIPT,
                Arrays.asList(handoffService.buildKey(voucherId),
                        SECKILL_ORDER_HANDOFF_QUARANTINE_KEY),
                member, String.valueOf(System.currentTimeMillis()), reason);
    }

    private boolean acquireOrRenewLease() {
        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(
                SECKILL_ORDER_HANDOFF_RELAY_LEASE_KEY, owner,
                leaseMillis, TimeUnit.MILLISECONDS);
        if (Boolean.TRUE.equals(acquired)) {
            return true;
        }
        Long renewed = redisTemplate.execute(RENEW_SCRIPT,
                Collections.singletonList(SECKILL_ORDER_HANDOFF_RELAY_LEASE_KEY),
                owner, String.valueOf(leaseMillis));
        return Long.valueOf(1L).equals(renewed);
    }

    private List<Long> loadVoucherIds() {
        List<SeckillVoucher> vouchers = voucherService.list();
        if (vouchers == null || vouchers.isEmpty()) {
            return Collections.emptyList();
        }
        List<Long> ids = new ArrayList<>(vouchers.size());
        for (SeckillVoucher voucher : vouchers) {
            if (voucher.getVoucherId() != null) {
                ids.add(voucher.getVoucherId());
            }
        }
        return ids;
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            if (running) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public synchronized void stop() {
        running = false;
        Thread current = worker;
        if (current != null) {
            current.interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    private static DefaultRedisScript<Long> loadScript(String resource) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource(resource)));
        script.setResultType(Long.class);
        return script;
    }
}
