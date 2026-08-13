package com.hmdp.config;

import com.hmdp.cache.SeckillVoucherBloomFilter;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherService;
import com.hmdp.service.SeckillVoucherRedisSynchronizer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/** 启动时恢复秒杀活动元数据，并通过订单、Outbox、Handoff 重建 Redis 交易投影。 */
@Slf4j
@Component
public class SeckillVoucherStockInitializer implements ApplicationRunner {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private IVoucherService voucherService;
    @Resource
    private SeckillVoucherRedisSynchronizer redisSynchronizer;
    @Resource
    private SeckillVoucherBloomFilter bloomFilter;

    @Override
    public void run(ApplicationArguments args) {
        try {
            restoreRedisProjection();
        } catch (RuntimeException e) {
            log.error("恢复秒杀 Redis 投影失败，应用继续启动但秒杀入口会失败关闭", e);
        }
    }

    void restoreRedisProjection() {
        List<SeckillVoucher> vouchers = seckillVoucherService.list();
        if (vouchers == null || vouchers.isEmpty()) {
            bloomFilter.initialize(Collections.emptyList());
            return;
        }
        List<Long> voucherIds = vouchers.stream()
                .map(SeckillVoucher::getVoucherId)
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toList());
        Map<Long, Voucher> voucherMap = voucherIds.isEmpty()
                ? Collections.emptyMap()
                : voucherService.listByIds(voucherIds).stream()
                        .collect(Collectors.toMap(Voucher::getId, Function.identity()));
        bloomFilter.initialize(voucherIds);
        for (SeckillVoucher voucher : vouchers) {
            Voucher voucherInfo = voucherMap.get(voucher.getVoucherId());
            if (voucherInfo == null || voucherInfo.getStatus() == null) {
                continue;
            }
            redisSynchronizer.synchronizeMetadata(voucher, voucherInfo);
        }
        // 库存与已购集合由低频周期对账恢复，避免每个滚动发布实例都扫描全库。
        log.info("秒杀 Redis 元数据和本地缓存初始化完成，总数={}", vouchers.size());
    }
}
