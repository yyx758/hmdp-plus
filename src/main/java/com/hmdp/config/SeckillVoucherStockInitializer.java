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

@Slf4j
@Component
public class SeckillVoucherStockInitializer implements ApplicationRunner {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private IVoucherService voucherService;

    @Resource
    private SeckillVoucherRedisSynchronizer seckillVoucherRedisSynchronizer;

    @Resource
    private SeckillVoucherBloomFilter seckillVoucherBloomFilter;

    @Override
    public void run(ApplicationArguments args) {
        try {
            restoreMissingStockKeys();
        } catch (RuntimeException e) {
            log.error("恢复秒杀券Redis库存失败，应用将继续启动", e);
        }
    }

    void restoreMissingStockKeys() {
        List<SeckillVoucher> vouchers = seckillVoucherService.list();
        if (vouchers == null || vouchers.isEmpty()) {
            log.info("秒杀券Redis库存检查完成，总数=0，恢复=0");
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
        // 先装入全部合法ID，初始化完成后布隆过滤器才开始拒绝非法请求。
        seckillVoucherBloomFilter.initialize(voucherIds);
        int restored = 0;
        for (SeckillVoucher voucher : vouchers) {
            if (voucher.getVoucherId() == null || voucher.getStock() == null) {
                continue;
            }
            Voucher voucherInfo = voucherMap.get(voucher.getVoucherId());
            if (voucherInfo == null || voucherInfo.getStatus() == null) {
                log.warn("秒杀券缺少对应优惠券状态，跳过Redis初始化，voucherId={}", voucher.getVoucherId());
                continue;
            }
            try {
                if (seckillVoucherRedisSynchronizer.initializeVoucher(voucher, voucherInfo)) {
                    restored++;
                }
            } catch (IllegalArgumentException e) {
                log.warn("秒杀券配置不合法，跳过Redis初始化，voucherId={}，原因={}",
                        voucher.getVoucherId(), e.getMessage());
            }
        }
        log.info("秒杀券Redis库存检查完成，总数={}，恢复={}", vouchers.size(), restored);
    }
}
