package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.cache.SeckillVoucherBloomFilter;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillVoucherUpdateDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherService;
import com.hmdp.service.SeckillVoucherRedisSynchronizer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher> implements IVoucherService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private SeckillVoucherRedisSynchronizer seckillVoucherRedisSynchronizer;
    @Resource
    private SeckillVoucherBloomFilter seckillVoucherBloomFilter;

    @Override
    public Result queryVoucherOfShop(Long shopId) {
        // 查询优惠券信息
        List<Voucher> vouchers = getBaseMapper().queryVoucherOfShop(shopId);
        // 返回结果
        return Result.ok(vouchers);
    }

    @Override
    @Transactional
    public void addSeckillVoucher(Voucher voucher) {
        validateNewSeckillVoucher(voucher);
        if (voucher.getStatus() == null) {
            voucher.setStatus(1);
        }
        // 保存优惠券
        if (!save(voucher)) {
            throw new IllegalStateException("保存优惠券失败");
        }
        // 保存秒杀信息
        SeckillVoucher seckillVoucher = new SeckillVoucher();
        seckillVoucher.setVoucherId(voucher.getId());
        seckillVoucher.setStock(voucher.getStock());
        seckillVoucher.setBeginTime(voucher.getBeginTime());
        seckillVoucher.setEndTime(voucher.getEndTime());
        if (!seckillVoucherService.save(seckillVoucher)) {
            throw new IllegalStateException("保存秒杀券失败");
        }
        // 数据库写入和 Redis 同步处于同一业务调用中；Redis 失败会抛出异常并回滚数据库事务。
        registerRollbackCompensation(
                () -> seckillVoucherRedisSynchronizer.deleteVoucher(seckillVoucher.getVoucherId()));
        seckillVoucherRedisSynchronizer.synchronizeNewVoucher(seckillVoucher, voucher);
        seckillVoucherBloomFilter.put(voucher.getId());
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Result updateSeckillVoucher(SeckillVoucherUpdateDTO update) {
        if (update == null || update.getVoucherId() == null) {
            return Result.fail("秒杀券id不能为空");
        }
        Long voucherId = update.getVoucherId();
        Voucher oldVoucher = getById(voucherId);
        SeckillVoucher oldSeckillVoucher = seckillVoucherService.getById(voucherId);
        if (oldVoucher == null || oldSeckillVoucher == null) {
            return Result.fail("秒杀券不存在");
        }

        LocalDateTime beginTime = update.getBeginTime() == null
                ? oldSeckillVoucher.getBeginTime() : update.getBeginTime();
        LocalDateTime endTime = update.getEndTime() == null
                ? oldSeckillVoucher.getEndTime() : update.getEndTime();
        if (beginTime == null || endTime == null || !beginTime.isBefore(endTime)) {
            return Result.fail("秒杀开始时间必须早于结束时间");
        }
        if (update.getStatus() != null && (update.getStatus() < 1 || update.getStatus() > 3)) {
            return Result.fail("秒杀券状态只能是1、2或3");
        }

        Voucher voucherPatch = buildVoucherPatch(update);
        if (hasVoucherChanges(update) && !updateById(voucherPatch)) {
            throw new IllegalStateException("修改优惠券失败");
        }

        if (update.getBeginTime() != null || update.getEndTime() != null) {
            SeckillVoucher seckillPatch = new SeckillVoucher()
                    .setVoucherId(voucherId)
                    .setBeginTime(update.getBeginTime())
                    .setEndTime(update.getEndTime());
            if (!seckillVoucherService.updateById(seckillPatch)) {
                throw new IllegalStateException("修改秒杀活动时间失败");
            }
        }

        SeckillVoucher updatedSeckillVoucher = new SeckillVoucher()
                .setVoucherId(voucherId)
                .setBeginTime(beginTime)
                .setEndTime(endTime);
        Integer updatedStatus = update.getStatus() == null
                ? oldVoucher.getStatus() : update.getStatus();
        Voucher updatedVoucherInfo = new Voucher()
                .setId(voucherId)
                .setShopId(oldVoucher.getShopId())
                .setStatus(updatedStatus);
        // 普通修改只同步活动元数据，不覆盖 Redis 中正在变化的实时库存。
        SeckillVoucher rollbackMetadata = new SeckillVoucher()
                .setVoucherId(voucherId)
                .setBeginTime(oldSeckillVoucher.getBeginTime())
                .setEndTime(oldSeckillVoucher.getEndTime());
        registerRollbackCompensation(
                () -> seckillVoucherRedisSynchronizer.synchronizeMetadata(
                        rollbackMetadata, oldVoucher));
        seckillVoucherRedisSynchronizer.synchronizeMetadata(
                updatedSeckillVoucher, updatedVoucherInfo);
        return Result.ok();
    }

    private Voucher buildVoucherPatch(SeckillVoucherUpdateDTO update) {
        return new Voucher()
                .setId(update.getVoucherId())
                .setTitle(update.getTitle())
                .setSubTitle(update.getSubTitle())
                .setRules(update.getRules())
                .setPayValue(update.getPayValue())
                .setActualValue(update.getActualValue())
                .setStatus(update.getStatus());
    }

    private boolean hasVoucherChanges(SeckillVoucherUpdateDTO update) {
        return update.getTitle() != null
                || update.getSubTitle() != null
                || update.getRules() != null
                || update.getPayValue() != null
                || update.getActualValue() != null
                || update.getStatus() != null;
    }

    private void validateNewSeckillVoucher(Voucher voucher) {
        if (voucher == null) {
            throw new IllegalArgumentException("秒杀券不能为空");
        }
        if (voucher.getStock() == null || voucher.getStock() < 0) {
            throw new IllegalArgumentException("秒杀券库存不能为空且不能小于0");
        }
        if (voucher.getBeginTime() == null || voucher.getEndTime() == null
                || !voucher.getBeginTime().isBefore(voucher.getEndTime())) {
            throw new IllegalArgumentException("秒杀开始时间必须早于结束时间");
        }
    }

    private void registerRollbackCompensation(Runnable compensation) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()
                || !TransactionSynchronizationManager.isSynchronizationActive()) {
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCompletion(int status) {
                if (status == TransactionSynchronization.STATUS_COMMITTED) {
                    return;
                }
                try {
                    compensation.run();
                } catch (RuntimeException e) {
                    log.error("秒杀券事务回滚后恢复Redis数据失败，需要人工核对", e);
                }
            }
        });
    }
}
