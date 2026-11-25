package com.hmdp.service;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.HashSet;
import java.util.Set;

import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_ACCEPTED_AUDIT_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_STOCK_KEY;

/** Read-only stock audit. Redis reservations are never overwritten from MySQL. */
@Slf4j
@Service
public class SeckillInventoryAuditService {

    private final ISeckillVoucherService voucherService;
    private final SeckillOrderOutboxMapper outboxMapper;
    private final VoucherOrderMapper orderMapper;
    private final SeckillOrderHandoffService handoffService;
    private final StringRedisTemplate redisTemplate;

    public SeckillInventoryAuditService(ISeckillVoucherService voucherService,
                                        SeckillOrderOutboxMapper outboxMapper,
                                        VoucherOrderMapper orderMapper,
                                        SeckillOrderHandoffService handoffService,
                                        StringRedisTemplate redisTemplate) {
        this.voucherService = voucherService;
        this.outboxMapper = outboxMapper;
        this.orderMapper = orderMapper;
        this.handoffService = handoffService;
        this.redisTemplate = redisTemplate;
    }

    @Scheduled(cron = "${hmdp.seckill.audit.inventory-cron:0 0 2,8,14,20 * * ?}",
            zone = "${hmdp.seckill.audit.zone:Asia/Shanghai}")
    public void auditAll() {
        try {
            List<SeckillVoucher> vouchers = voucherService.list();
            if (vouchers == null) {
                return;
            }
            for (SeckillVoucher voucher : vouchers) {
                if (voucher.getVoucherId() != null) {
                    auditVoucher(voucher.getVoucherId());
                }
            }
        } catch (RuntimeException e) {
            log.error("Read-only seckill inventory audit failed", e);
        }
    }

    void auditVoucher(Long voucherId) {
        try {
            int unpersisted = outboxMapper.countUnpersistedReservations(voucherId);
            Set<String> handoffs = handoffService.findAll(voucherId);
            Long unresolvedReceipts = redisTemplate.opsForZSet().zCard(
                    SECKILL_ORDER_ACCEPTED_AUDIT_KEY);
            if (unpersisted != 0 || (handoffs != null && !handoffs.isEmpty())
                    || (unresolvedReceipts != null && unresolvedReceipts > 0)) {
                log.info("Seckill inventory audit deferred while reservations are in-flight, "
                                + "voucherId={}, outbox={}, handoff={}, unresolvedReceipts={}",
                        voucherId, unpersisted, handoffs == null ? 0 : handoffs.size(),
                        unresolvedReceipts);
                return;
            }
            SeckillVoucher current = voucherService.getById(voucherId);
            if (current == null || current.getStock() == null) {
                return;
            }
            String redisValue = redisTemplate.opsForValue().get(SECKILL_STOCK_KEY + voucherId);
            if (redisValue == null) {
                log.error("Seckill Redis stock key missing; manual recovery required, voucherId={}",
                        voucherId);
                return;
            }
            long redisStock = Long.parseLong(redisValue);
            if (redisStock != current.getStock()) {
                log.error("Unexplained seckill stock difference; no automatic repair, voucherId={}, "
                                + "redisStock={}, mysqlStock={}",
                        voucherId, redisStock, current.getStock());
            }
            List<Long> activeUsers = orderMapper.findActiveUserIds(voucherId);
            Set<String> expectedUsers = new HashSet<>();
            if (activeUsers != null) {
                for (Long userId : activeUsers) {
                    expectedUsers.add(String.valueOf(userId));
                }
            }
            Set<String> actualUsers = redisTemplate.opsForSet().members(SECKILL_ORDER_KEY + voucherId);
            if (actualUsers == null || !actualUsers.equals(expectedUsers)) {
                log.error("Unexplained seckill purchase-set difference; no automatic repair, "
                                + "voucherId={}, redisUsers={}, mysqlUsers={}",
                        voucherId, actualUsers == null ? -1 : actualUsers.size(), expectedUsers.size());
            }
        } catch (RuntimeException e) {
            log.error("Could not audit seckill voucher inventory, voucherId={}", voucherId, e);
        }
    }
}
