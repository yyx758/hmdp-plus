package com.hmdp.service;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_RECOVERY_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_RECONCILIATION_LEADER_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_KEY;

/**
 * Redis 秒杀事实投影的恢复与对账。
 *
 * <p>MySQL 订单和 Outbox 是权威数据，Handoff 只覆盖 Lua 成功但 Outbox 尚未落库的窗口。
 * 重建库存时扣除未落单 Outbox 与仅存在于 Handoff 的预留，并同步重建已购用户集合。</p>
 */
@Slf4j
@Service
public class SeckillOrderReconciliationService {

    private static final String STAGING_SENTINEL = "__hmdp_rebuild_staging__";
    private static final DefaultRedisScript<Long> REBUILD_SCRIPT =
            loadScript("lua/seckill_rebuild.lua");
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT =
            loadScript("lua/seckill_recovery_unlock.lua");
    private static final DefaultRedisScript<Long> RENEW_SCRIPT =
            loadScript("lua/seckill_recovery_renew.lua");

    private final ISeckillVoucherService voucherService;
    private final VoucherOrderMapper voucherOrderMapper;
    private final SeckillOrderOutboxMapper outboxMapper;
    private final SeckillOrderHandoffService handoffService;
    private final StringRedisTemplate stringRedisTemplate;
    private final long recoveryLeaseSeconds;
    private final long fullReconciliationLeaseSeconds;
    private final int redisUserBatchSize;

    public SeckillOrderReconciliationService(
            ISeckillVoucherService voucherService,
            VoucherOrderMapper voucherOrderMapper,
            SeckillOrderOutboxMapper outboxMapper,
            SeckillOrderHandoffService handoffService,
            StringRedisTemplate stringRedisTemplate,
            @Value("${hmdp.kafka.seckill-order.reconciliation.recovery-lease-seconds:300}")
            long recoveryLeaseSeconds,
            @Value("${hmdp.kafka.seckill-order.reconciliation.full-lease-seconds:3600}")
            long fullReconciliationLeaseSeconds,
            @Value("${hmdp.kafka.seckill-order.reconciliation.redis-user-batch-size:500}")
            int redisUserBatchSize) {
        this.voucherService = voucherService;
        this.voucherOrderMapper = voucherOrderMapper;
        this.outboxMapper = outboxMapper;
        this.handoffService = handoffService;
        this.stringRedisTemplate = stringRedisTemplate;
        this.recoveryLeaseSeconds = recoveryLeaseSeconds;
        this.fullReconciliationLeaseSeconds = fullReconciliationLeaseSeconds;
        this.redisUserBatchSize = Math.max(1, redisUserBatchSize);
    }

    @Scheduled(
            cron = "${hmdp.kafka.seckill-order.reconciliation.cron:0 0 2,8,14,20 * * ?}",
            zone = "${hmdp.kafka.seckill-order.reconciliation.zone:Asia/Shanghai}")
    public void reconcileAll() {
        String token = UUID.randomUUID().toString();
        Boolean locked = stringRedisTemplate.opsForValue().setIfAbsent(
                SECKILL_RECONCILIATION_LEADER_KEY, token,
                fullReconciliationLeaseSeconds, TimeUnit.SECONDS);
        if (!Boolean.TRUE.equals(locked)) {
            return;
        }
        try {
            reconcileAllVouchers();
        } finally {
            stringRedisTemplate.execute(
                    UNLOCK_SCRIPT, Collections.emptyList(), "all", token);
        }
    }

    private void reconcileAllVouchers() {
        List<SeckillVoucher> vouchers = voucherService.list();
        if (vouchers == null) {
            return;
        }
        for (SeckillVoucher voucher : vouchers) {
            if (voucher.getVoucherId() != null && voucher.getStock() != null) {
                reconcileVoucher(voucher);
            }
        }
    }

    public void reconcileVoucher(SeckillVoucher voucher) {
        Long voucherId = voucher.getVoucherId();
        String token = UUID.randomUUID().toString();
        String recoveryKey = SECKILL_RECOVERY_KEY + voucherId;
        String stagingOrderKey = SECKILL_ORDER_KEY + "rebuild:" + voucherId + ":" + token;
        Boolean locked = stringRedisTemplate.opsForValue().setIfAbsent(
                recoveryKey, token, recoveryLeaseSeconds, TimeUnit.SECONDS);
        if (!Boolean.TRUE.equals(locked)) {
            return;
        }
        try {
            List<SeckillOrderOutboxEvent> unpersisted =
                    outboxMapper.findUnpersistedEvents(voucherId);
            Set<Long> reservedOrderIds = new LinkedHashSet<>();
            Set<Long> purchasedUserIds = new LinkedHashSet<>();
            if (unpersisted != null) {
                for (SeckillOrderOutboxEvent event : unpersisted) {
                    reservedOrderIds.add(event.getOrderId());
                    purchasedUserIds.add(event.getUserId());
                }
            }
            List<Long> activeUsers = voucherOrderMapper.findActiveUserIds(voucherId);
            if (activeUsers != null) {
                purchasedUserIds.addAll(activeUsers);
            }

            int handoffOnlyReservations = mergeHandoffReservations(
                    voucherId, reservedOrderIds, purchasedUserIds);
            int expectedStock = Math.max(0,
                    voucher.getStock() - reservedOrderIds.size() - handoffOnlyReservations);

            renewRecoveryLease(voucherId, token);
            stringRedisTemplate.delete(stagingOrderKey);
            stringRedisTemplate.opsForSet().add(stagingOrderKey, STAGING_SENTINEL);
            stringRedisTemplate.expire(
                    stagingOrderKey, Math.max(60L, recoveryLeaseSeconds * 2L), TimeUnit.SECONDS);
            writeUsersInBatches(voucherId, token, stagingOrderKey, purchasedUserIds);
            Long rebuilt = stringRedisTemplate.execute(
                    REBUILD_SCRIPT, Collections.emptyList(), voucherId.toString(), token,
                    String.valueOf(expectedStock), stagingOrderKey);
            if (!Long.valueOf(1L).equals(rebuilt)) {
                throw new IllegalStateException("Redis 秒杀投影重建租约已失效，voucherId=" + voucherId);
            }
            log.info("秒杀 Redis 对账完成，voucherId={}，mysqlStock={}，reserved={}，stock={}，users={}",
                    voucherId, voucher.getStock(),
                    reservedOrderIds.size() + handoffOnlyReservations,
                    expectedStock, purchasedUserIds.size());
        } catch (RuntimeException e) {
            stringRedisTemplate.delete(stagingOrderKey);
            stringRedisTemplate.execute(
                    UNLOCK_SCRIPT, Collections.emptyList(), voucherId.toString(), token);
            throw e;
        }
    }

    private void writeUsersInBatches(
            Long voucherId, String token, String stagingKey, Set<Long> userIds) {
        List<String> batch = new ArrayList<>(redisUserBatchSize);
        for (Long userId : userIds) {
            batch.add(userId.toString());
            if (batch.size() == redisUserBatchSize) {
                stringRedisTemplate.opsForSet().add(stagingKey, batch.toArray(new String[0]));
                renewRecoveryLease(voucherId, token);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            stringRedisTemplate.opsForSet().add(stagingKey, batch.toArray(new String[0]));
            renewRecoveryLease(voucherId, token);
        }
    }

    private void renewRecoveryLease(Long voucherId, String token) {
        Long renewed = stringRedisTemplate.execute(
                RENEW_SCRIPT, Collections.emptyList(), voucherId.toString(), token,
                String.valueOf(recoveryLeaseSeconds));
        if (!Long.valueOf(1L).equals(renewed)) {
            throw new IllegalStateException(
                    "Redis seckill reconciliation lease expired, voucherId=" + voucherId);
        }
    }

    private int mergeHandoffReservations(
            Long voucherId, Set<Long> reservedOrderIds, Set<Long> purchasedUserIds) {
        Set<String> members = handoffService.findAll(voucherId);
        if (members == null) {
            return 0;
        }
        int handoffOnly = 0;
        for (String member : members) {
            SeckillOrderOutboxEvent event = handoffService.parse(voucherId, member);
            if (event == null) {
                continue;
            }
            purchasedUserIds.add(event.getUserId());
            if (reservedOrderIds.contains(event.getOrderId())) {
                continue;
            }
            // Outbox 已存在但 Handoff 删除失败时不能重复扣减库存。
            if (outboxMapper.findByOrderId(event.getOrderId()) == null) {
                handoffOnly++;
            }
        }
        return handoffOnly;
    }

    private static DefaultRedisScript<Long> loadScript(String resource) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource(resource)));
        script.setResultType(Long.class);
        return script;
    }
}
