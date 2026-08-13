package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderStatusDTO;
import com.hmdp.dto.SeckillVoucherCacheDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.enums.SeckillOrderStatus;
import com.hmdp.exception.BusinessException;
import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillAccessTokenService;
import com.hmdp.service.ISeckillSubscriptionService;
import com.hmdp.service.ISeckillTopBuyerService;
import com.hmdp.service.IUserNotificationService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.SeckillOrderHandoffService;
import com.hmdp.service.SeckillOrderOutboxService;
import com.hmdp.service.SeckillVoucherCacheService;
import com.hmdp.service.OrderIdGenerator;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.StreamUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;

import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_ACCEPTED_KEY;

@Slf4j
@Service
public class VoucherOrderServiceImpl
        extends ServiceImpl<VoucherOrderMapper, VoucherOrder>
        implements IVoucherOrderService {

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT = loadScript("seckill.lua");
    private static final DefaultRedisScript<Long> CANCEL_SCRIPT =
            loadScript("lua/seckill_cancel.lua");
    private static final DefaultRedisScript<Long> CANCEL_ROLLBACK_SCRIPT =
            loadScript("lua/seckill_cancel_rollback.lua");

    @Resource
    private OrderIdGenerator orderIdGenerator;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private SeckillVoucherCacheService seckillVoucherCacheService;
    @Resource
    private ISeckillAccessTokenService seckillAccessTokenService;
    @Resource
    private SeckillOrderOutboxService outboxService;
    @Resource
    private SeckillOrderOutboxMapper outboxMapper;
    @Resource
    private SeckillOrderHandoffService handoffService;
    @Resource
    private SeckillVoucherMapper seckillVoucherMapper;
    @Resource
    private ISeckillSubscriptionService seckillSubscriptionService;
    @Resource
    private ISeckillTopBuyerService seckillTopBuyerService;
    @Resource
    private IUserNotificationService userNotificationService;
    @Value("${hmdp.seckill.acceptance-mode:redis-handoff}")
    private String acceptanceMode = "redis-handoff";

    @Override
    public Result seckillVoucher(Long voucherId, String accessToken) {
        SeckillVoucherCacheDTO voucher = seckillVoucherCacheService.queryById(voucherId);
        if (voucher == null) {
            return Result.fail("秒杀活动配置不存在");
        }
        LocalDateTime now = LocalDateTime.now();
        if (voucher.getStatus() == null || voucher.getStatus() != 1) {
            return Result.fail("秒杀券已下架或不可用");
        }
        if (voucher.getBeginTime() == null || now.isBefore(voucher.getBeginTime())) {
            return Result.fail("秒杀尚未开始");
        }
        if (voucher.getEndTime() == null || now.isAfter(voucher.getEndTime())) {
            return Result.fail("秒杀已经结束");
        }

        Long userId = UserHolder.getUser().getId();
        long orderId = orderIdGenerator.nextId();
        boolean tokenEnabled = seckillAccessTokenService.isEnabled();
        Long code;
        try {
            code = stringRedisTemplate.execute(
                    SECKILL_SCRIPT,
                    Collections.emptyList(),
                    voucherId.toString(),
                    userId.toString(),
                    String.valueOf(orderId),
                    String.valueOf(System.currentTimeMillis()),
                    accessToken == null ? "" : accessToken,
                    tokenEnabled ? "1" : "0");
        } catch (RuntimeException e) {
            log.error("执行秒杀Lua时Redis连接中断，结果无法确认，orderId={}", orderId, e);
            return Result.fail("抢购结果未确认，请重新获取资格令牌后重试",
                    new SeckillOrderStatusDTO()
                            .setOrderId(orderId)
                            .setStatus(SeckillOrderStatus.PROCESSING.name())
                            .setMessage("Redis执行结果未确认"));
        }
        if (code == null) {
            return Result.fail("秒杀服务繁忙，请稍后重试");
        }
        if (code != 0L) {
            return Result.fail(resolveFailureMessage(code.intValue()));
        }

        if ("mysql-outbox".equalsIgnoreCase(acceptanceMode)) {
            try {
                SeckillOrderOutboxEvent event =
                        outboxService.accept(orderId, voucherId, userId, false);
                outboxService.completeHandoff(event);
            } catch (RuntimeException e) {
                log.error("写入秒杀订单Outbox失败，等待Handoff确认，orderId={}", orderId, e);
                return Result.fail("抢券结果暂未确认，请稍后按订单ID查询",
                        new SeckillOrderStatusDTO()
                                .setOrderId(orderId)
                                .setStatus(SeckillOrderStatus.PROCESSING.name())
                                .setMessage("抢券结果正在确认中"));
            }
        }
        return Result.ok(new SeckillOrderStatusDTO()
                .setOrderId(orderId)
                .setStatus(SeckillOrderStatus.SUCCESS.name())
                .setMessage("抢券成功"));
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        return seckillVoucher(voucherId, null);
    }

    @Override
    public Result querySeckillOrderStatus(Long orderId) {
        if (orderId == null) {
            return Result.fail("订单ID不能为空");
        }
        Long userId = UserHolder.getUser().getId();
        VoucherOrder order = getById(orderId);
        if (order != null) {
            if (!userId.equals(order.getUserId())) {
                return Result.fail("订单处理结果不存在");
            }
            boolean cancelled = Integer.valueOf(4).equals(order.getStatus());
            return Result.ok(status(orderId,
                    cancelled ? SeckillOrderStatus.CANCELLED : SeckillOrderStatus.SUCCESS,
                    cancelled ? "订单已取消" : "订单创建成功"));
        }

        SeckillOrderOutboxEvent outbox = outboxMapper.findByOrderId(orderId);
        if (outbox != null) {
            if (!userId.equals(outbox.getUserId())) {
                return Result.fail("订单处理结果不存在");
            }
            if (SeckillOrderOutboxEvent.STATUS_MANUAL_REVIEW.equals(outbox.getStatus())) {
                return Result.ok(status(orderId, SeckillOrderStatus.MANUAL_REVIEW,
                        "订单处理异常，已进入人工核对"));
            }
            return Result.ok(status(orderId, SeckillOrderStatus.PROCESSING,
                    "订单正在排队处理中"));
        }

        Object acceptedValue = stringRedisTemplate.opsForHash().get(
                SECKILL_ORDER_ACCEPTED_KEY, orderId.toString());
        String accepted = acceptedValue == null ? null : acceptedValue.toString();
        if (accepted != null) {
            String[] parts = accepted.split("\\|", -1);
            if (parts.length != 2 || !userId.toString().equals(parts[0])) {
                return Result.fail("订单处理结果不存在");
            }
            return Result.ok(status(orderId, SeckillOrderStatus.PROCESSING,
                    "订单正在排队处理中"));
        }

        return Result.fail("订单处理结果不存在或尚未完成可靠受理");
    }

    private SeckillOrderStatusDTO status(
            Long orderId, SeckillOrderStatus status, String message) {
        return new SeckillOrderStatusDTO()
                .setOrderId(orderId)
                .setStatus(status.name())
                .setMessage(message);
    }

    @Override
    public Result queryActiveOrderId(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        VoucherOrder order = getBaseMapper().selectActiveOrder(userId, voucherId);
        return Result.ok(order == null ? null : order.getId());
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Result cancelVoucherOrder(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        VoucherOrder order = getBaseMapper().selectActiveOrder(userId, voucherId);
        if (order == null) {
            return Result.fail("可取消的秒杀订单不存在");
        }
        if (getBaseMapper().cancelActiveOrder(order.getId(), userId) != 1) {
            return Result.fail("订单已经取消，请勿重复操作");
        }
        if (seckillVoucherMapper.adjustStock(voucherId, 1) != 1) {
            throw new BusinessException("秒杀券库存回流失败");
        }
        Long redisResult = stringRedisTemplate.execute(
                CANCEL_SCRIPT,
                Arrays.asList("seckill:stock:" + voucherId, "seckill:order:" + voucherId),
                userId.toString());
        if (redisResult == null || redisResult < 0) {
            throw new BusinessException("Redis库存回流失败，请稍后重试");
        }
        order.setStatus(4);
        registerCancellationSynchronization(order);
        return Result.ok(true);
    }

    private void registerCancellationSynchronization(VoucherOrder order) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCompletion(int transactionStatus) {
                if (transactionStatus == TransactionSynchronization.STATUS_COMMITTED) {
                    afterOrderCancelled(order);
                    return;
                }
                Long compensated = stringRedisTemplate.execute(
                        CANCEL_ROLLBACK_SCRIPT,
                        Arrays.asList("seckill:stock:" + order.getVoucherId(),
                                "seckill:order:" + order.getVoucherId()),
                        order.getUserId().toString());
                if (!Long.valueOf(1L).equals(compensated)) {
                    log.error("取消订单事务回滚后恢复Redis库存失败，orderId={}", order.getId());
                }
            }
        });
    }

    private void afterOrderCancelled(VoucherOrder order) {
        try {
            seckillTopBuyerService.rollbackCancelledOrder(order);
            seckillSubscriptionService.clearPurchased(order);
            userNotificationService.publish(
                    order.getUserId(), "ORDER_CANCELLED", "订单已取消",
                    "优惠券库存已成功回流",
                    order.getVoucherId(), "cancel:" + order.getId());
        } catch (RuntimeException e) {
            log.error("取消订单提交后的营销链路处理失败，orderId={}", order.getId(), e);
        }
    }

    private String resolveFailureMessage(int code) {
        switch (code) {
            case 1: return "库存不足";
            case 2: return "不能重复下单";
            case 3: return "秒杀活动配置不存在";
            case 4: return "秒杀尚未开始";
            case 5: return "秒杀已经结束";
            case 6: return "秒杀券已下架或不可用";
            case 7: return "资格令牌无效或已过期，请重新获取";
            case 8: return "秒杀数据正在恢复，请稍后重试";
            default: return "秒杀失败";
        }
    }

    private static DefaultRedisScript<Long> loadScript(String resourceName) {
        try {
            String text = StreamUtils.copyToString(
                    new ClassPathResource(resourceName).getInputStream(), StandardCharsets.UTF_8);
            return new DefaultRedisScript<>(text, Long.class);
        } catch (IOException e) {
            throw new IllegalStateException("加载Lua脚本失败：" + resourceName, e);
        }
    }
}
