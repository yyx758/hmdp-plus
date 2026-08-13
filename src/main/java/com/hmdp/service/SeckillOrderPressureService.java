package com.hmdp.service;

import com.hmdp.config.SeckillRateLimitProperties;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/** 用可靠受理积压量反馈调节入口令牌桶，避免下游越堵入口仍按固定速率放行。 */
@Slf4j
@Service
public class SeckillOrderPressureService {

    private final SeckillOrderOutboxMapper outboxMapper;
    private final SeckillRateLimitProperties properties;

    @Getter
    private volatile long backlog;
    @Getter
    private volatile String level = "NORMAL";
    private volatile double admissionMultiplier = 1.0D;

    public SeckillOrderPressureService(
            SeckillOrderOutboxMapper outboxMapper,
            SeckillRateLimitProperties properties) {
        this.outboxMapper = outboxMapper;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${hmdp.seckill.rate-limit.adaptive.refresh-millis:1000}")
    public void refresh() {
        if (!properties.getAdaptive().isEnabled()) {
            update(0L, "NORMAL", 1.0D);
            return;
        }
        try {
            long current = outboxMapper.countBacklog();
            SeckillRateLimitProperties.Adaptive adaptive = properties.getAdaptive();
            if (current >= adaptive.getCriticalBacklog()) {
                update(current, "CRITICAL", adaptive.getCriticalMultiplier());
            } else if (current >= adaptive.getWarningBacklog()) {
                update(current, "WARNING", adaptive.getWarningMultiplier());
            } else {
                update(current, "NORMAL", 1.0D);
            }
        } catch (RuntimeException e) {
            // 压力采样失败时保持上一次结果，避免监控抖动直接放大入口流量。
            log.warn("秒杀积压压力采样失败，继续使用上一次限流倍率", e);
        }
    }

    public double getAdmissionMultiplier() {
        return Math.max(0.01D, Math.min(1.0D, admissionMultiplier));
    }

    private void update(long currentBacklog, String currentLevel, double multiplier) {
        if (!currentLevel.equals(level)) {
            log.warn("秒杀入口压力等级变化，{} -> {}，backlog={}，admissionMultiplier={}",
                    level, currentLevel, currentBacklog, multiplier);
        }
        backlog = currentBacklog;
        level = currentLevel;
        admissionMultiplier = multiplier;
    }
}
