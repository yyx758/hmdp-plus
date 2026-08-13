package com.hmdp.service.impl;

import com.hmdp.config.SeckillMarketingProperties;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.UserInfoMapper;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.service.ISeckillReminderService;
import com.hmdp.service.ISeckillSubscriptionService;
import com.hmdp.service.ISeckillTopBuyerService;
import com.hmdp.service.IUserNotificationService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_REMINDER_QUEUE;

@Slf4j
@Service
public class SeckillReminderServiceImpl implements ISeckillReminderService {

    @Resource(name = "redissonClient")
    private RedissonClient redissonClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private SeckillVoucherMapper seckillVoucherMapper;

    @Resource
    private VoucherMapper voucherMapper;

    @Resource
    private UserInfoMapper userInfoMapper;

    @Resource
    private ISeckillSubscriptionService seckillSubscriptionService;

    @Resource
    private ISeckillTopBuyerService seckillTopBuyerService;

    @Resource
    private IUserNotificationService userNotificationService;

    @Resource
    private SeckillMarketingProperties properties;

    private RBlockingQueue<String> readyQueue;
    private RDelayedQueue<String> delayedQueue;
    private ExecutorService consumerExecutor;
    private volatile boolean running;

    @PostConstruct
    private void init() {
        if (!properties.isEnabled() || !properties.getReminder().isEnabled()) {
            return;
        }
        readyQueue = redissonClient.getBlockingQueue(SECKILL_REMINDER_QUEUE);
        delayedQueue = redissonClient.getDelayedQueue(readyQueue);
        running = true;
        consumerExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "seckill-reminder-consumer");
            thread.setDaemon(true);
            return thread;
        });
        consumerExecutor.submit(this::consumeLoop);
        try {
            scheduleExistingVouchers();
        } catch (RuntimeException e) {
            log.warn("启动时扫描秒杀预通知失败，后续新增或修改活动仍会正常调度", e);
        }
    }

    @PreDestroy
    private void destroy() {
        running = false;
        if (consumerExecutor != null) {
            consumerExecutor.shutdownNow();
        }
    }

    @Override
    public void schedule(Long voucherId, LocalDateTime beginTime) {
        if (!properties.isEnabled() || !properties.getReminder().isEnabled()
                || voucherId == null || beginTime == null || delayedQueue == null) {
            return;
        }
        LocalDateTime now = LocalDateTime.now();
        if (!beginTime.isAfter(now)) {
            return;
        }
        long beginAt = toEpochMilli(beginTime);
        String scheduledKey = "seckill:reminder:scheduled:" + voucherId + ":" + beginAt;
        long keyTtlMillis = Math.max(
                TimeUnit.HOURS.toMillis(1),
                Duration.between(now, beginTime.plusDays(1)).toMillis());
        Boolean firstSchedule = stringRedisTemplate.opsForValue().setIfAbsent(
                scheduledKey, "1", keyTtlMillis, TimeUnit.MILLISECONDS);
        if (!Boolean.TRUE.equals(firstSchedule)) {
            return;
        }
        long triggerAt = beginAt - TimeUnit.SECONDS.toMillis(
                Math.max(0, properties.getReminder().getLeadSeconds()));
        long delayMillis = Math.max(0L, triggerAt - System.currentTimeMillis());
        delayedQueue.offer(voucherId + "|" + beginAt, delayMillis, TimeUnit.MILLISECONDS);
    }

    private void scheduleExistingVouchers() {
        List<SeckillVoucher> vouchers = seckillVoucherMapper.selectList(null);
        if (vouchers == null) {
            return;
        }
        for (SeckillVoucher voucher : vouchers) {
            if (voucher.getBeginTime() != null && voucher.getEndTime() != null
                    && voucher.getEndTime().isAfter(LocalDateTime.now())) {
                schedule(voucher.getVoucherId(), voucher.getBeginTime());
            }
        }
    }

    private void consumeLoop() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                String message = readyQueue.poll(1, TimeUnit.SECONDS);
                if (message != null) {
                    handleReminder(message);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                log.error("处理秒杀预通知失败", e);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void handleReminder(String message) {
        String[] parts = message.split("\\|", 2);
        if (parts.length != 2) {
            return;
        }
        Long voucherId;
        long scheduledBeginAt;
        try {
            voucherId = Long.valueOf(parts[0]);
            scheduledBeginAt = Long.parseLong(parts[1]);
        } catch (NumberFormatException e) {
            return;
        }
        SeckillVoucher seckillVoucher = seckillVoucherMapper.selectById(voucherId);
        if (seckillVoucher == null || seckillVoucher.getBeginTime() == null
                || toEpochMilli(seckillVoucher.getBeginTime()) != scheduledBeginAt
                || !seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return;
        }
        Voucher voucher = voucherMapper.selectById(voucherId);
        if (voucher == null) {
            return;
        }

        int maxAudience = Math.max(1, properties.getReminder().getMaxAudience());
        Set<Long> audience = new LinkedHashSet<>();
        audience.addAll(seckillSubscriptionService.listSubscriberUserIds(
                voucherId, maxAudience));
        if (voucher.getShopId() != null && audience.size() < maxAudience) {
            audience.addAll(seckillTopBuyerService.queryTopBuyerUserIds(
                    voucher.getShopId(),
                    Math.max(1, properties.getReminder().getTopBuyerDays()),
                    Math.max(1, properties.getReminder().getTopBuyerCount())));
        }
        if (audience.size() < maxAudience) {
            List<Long> vipUserIds = userInfoMapper.selectVipUserIds(
                    Math.max(0, properties.getReminder().getVipMinLevel()),
                    Math.max(1, properties.getReminder().getVipCount()));
            if (vipUserIds != null) {
                audience.addAll(vipUserIds);
            }
        }

        int sent = 0;
        for (Long userId : audience) {
            if (sent >= maxAudience) {
                break;
            }
            if (userNotificationService.publish(
                    userId,
                    "SECKILL_REMINDER",
                    "秒杀即将开始",
                    "你关注的“" + voucher.getTitle() + "”将在2分钟内开抢",
                    voucherId,
                    "reminder:" + voucherId + ":" + scheduledBeginAt)) {
                sent++;
            }
        }
        log.info("秒杀预通知完成，voucherId={}，audience={}，sent={}",
                voucherId, audience.size(), sent);
    }

    private long toEpochMilli(LocalDateTime time) {
        return time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}
