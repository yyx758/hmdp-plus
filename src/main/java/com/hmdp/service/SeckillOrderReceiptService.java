package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_ACCEPTED_EXPIRE_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_ACCEPTED_AUDIT_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_ACCEPTED_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_ACCEPTED_USER_KEY;

/** Retains accepted-order receipts through the final state for MySQL outage queries. */
@Slf4j
@Service
public class SeckillOrderReceiptService {

    private static final DefaultRedisScript<Long> TERMINAL_SCRIPT = loadTerminalScript();
    private static final DefaultRedisScript<Long> CLEANUP_SCRIPT = loadScript(
            "lua/seckill_receipt_cleanup.lua");
    private final StringRedisTemplate redisTemplate;
    private final long retentionMillis;

    public SeckillOrderReceiptService(
            StringRedisTemplate redisTemplate,
            @Value("${hmdp.seckill.receipt.retention-seconds:604800}") long retentionSeconds) {
        this.redisTemplate = redisTemplate;
        this.retentionMillis = TimeUnit.SECONDS.toMillis(Math.max(1L, retentionSeconds));
    }

    public void markSuccess(VoucherOrder order) {
        markTerminal(order, "SUCCESS");
    }

    public void markCancelled(VoucherOrder order) {
        markTerminal(order, "CANCELLED");
    }

    private void markTerminal(VoucherOrder order, String status) {
        if (order == null || order.getId() == null || order.getUserId() == null
                || order.getVoucherId() == null) {
            throw new IllegalArgumentException("订单受理凭证字段不完整");
        }
        Long result = redisTemplate.execute(TERMINAL_SCRIPT,
                Arrays.asList(SECKILL_ORDER_ACCEPTED_KEY,
                        SECKILL_ORDER_ACCEPTED_EXPIRE_KEY,
                        SECKILL_ORDER_ACCEPTED_USER_KEY + order.getVoucherId(),
                        SECKILL_ORDER_ACCEPTED_AUDIT_KEY),
                order.getId().toString(), order.getUserId().toString(),
                order.getVoucherId().toString(), status,
                String.valueOf(System.currentTimeMillis() + retentionMillis));
        if (Long.valueOf(-1L).equals(result)) {
            throw new IllegalStateException("订单受理凭证归属冲突，orderId=" + order.getId());
        }
        if (Long.valueOf(0L).equals(result) && "CANCELLED".equals(status)) {
            log.warn("已取消订单的Redis受理凭证不存在，orderId={}", order.getId());
        }
    }

    @Scheduled(fixedDelayString = "${hmdp.seckill.receipt.cleanup-delay-ms:60000}")
    public void cleanupExpired() {
        long now = System.currentTimeMillis();
        Set<String> expired = redisTemplate.opsForZSet().rangeByScore(
                SECKILL_ORDER_ACCEPTED_EXPIRE_KEY, 0, now, 0, 500);
        if (expired == null || expired.isEmpty()) {
            return;
        }
        List<String> arguments = new ArrayList<>(expired.size() + 1);
        arguments.add(String.valueOf(now));
        arguments.addAll(expired);
        Long removed = redisTemplate.execute(CLEANUP_SCRIPT,
                Arrays.asList(SECKILL_ORDER_ACCEPTED_KEY,
                        SECKILL_ORDER_ACCEPTED_EXPIRE_KEY),
                arguments.toArray(new String[0]));
        log.debug("Cleaned {} expired seckill acceptance receipts", removed);
    }

    private static DefaultRedisScript<Long> loadTerminalScript() {
        return loadScript("lua/seckill_receipt_terminal.lua");
    }

    private static DefaultRedisScript<Long> loadScript(String resource) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(
                new ClassPathResource(resource)));
        script.setResultType(Long.class);
        return script;
    }
}
