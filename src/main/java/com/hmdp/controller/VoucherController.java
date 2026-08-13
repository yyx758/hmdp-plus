package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillVoucherUpdateDTO;
import com.hmdp.dto.SeckillVoucherStockUpdateDTO;
import com.hmdp.dto.VoucherSubscribeBatchDTO;
import com.hmdp.dto.VoucherSubscribeDTO;
import com.hmdp.entity.Voucher;
import com.hmdp.service.ISeckillTopBuyerService;
import com.hmdp.service.IVoucherService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 */
@RestController
@RequestMapping("/voucher")
public class VoucherController {

    @Resource
    private IVoucherService voucherService;

    @Resource
    private ISeckillTopBuyerService seckillTopBuyerService;

    /**
     * 新增秒杀券
     * @param voucher 优惠券信息，包含秒杀信息
     * @return 优惠券id
     */
    @PostMapping("seckill")
    public Result addSeckillVoucher(@RequestBody Voucher voucher) {
        voucherService.addSeckillVoucher(voucher);
        return Result.ok(voucher.getId());
    }

    /**
     * 修改秒杀券活动信息并同步 Redis 元数据。
     * 实时库存不通过该接口覆盖。
     */
    @PutMapping("seckill")
    public Result updateSeckillVoucher(@RequestBody SeckillVoucherUpdateDTO update) {
        return voucherService.updateSeckillVoucher(update);
    }

    @PostMapping("update/seckill/stock")
    public Result updateSeckillVoucherStock(@RequestBody SeckillVoucherStockUpdateDTO update) {
        return voucherService.updateSeckillVoucherStock(update);
    }

    /**
     * 新增普通券
     * @param voucher 优惠券信息
     * @return 优惠券id
     */
    @PostMapping
    public Result addVoucher(@RequestBody Voucher voucher) {
        voucherService.save(voucher);
        return Result.ok(voucher.getId());
    }


    /**
     * 查询店铺的优惠券列表
     * @param shopId 店铺id
     * @return 优惠券列表
     */
    @GetMapping("/list/{shopId}")
    public Result queryVoucherOfShop(@PathVariable("shopId") Long shopId) {
       return voucherService.queryVoucherOfShop(shopId);
    }

    @PostMapping("subscribe")
    public Result subscribe(@RequestBody VoucherSubscribeDTO request) {
        return voucherService.subscribe(request == null ? null : request.getVoucherId());
    }

    @PostMapping("unsubscribe")
    public Result unsubscribe(@RequestBody VoucherSubscribeDTO request) {
        return voucherService.unsubscribe(request == null ? null : request.getVoucherId());
    }

    @PostMapping("get/subscribe/status")
    public Result getSubscribeStatus(@RequestBody VoucherSubscribeDTO request) {
        return voucherService.getSubscribeStatus(
                request == null ? null : request.getVoucherId());
    }

    @PostMapping("get/subscribe/status/batch")
    public Result getSubscribeStatusBatch(@RequestBody VoucherSubscribeBatchDTO request) {
        return voucherService.getSubscribeStatusBatch(request);
    }

    @GetMapping("top-buyers/{shopId}")
    public Result queryTopBuyers(
            @PathVariable("shopId") Long shopId,
            @RequestParam(value = "days", defaultValue = "1") int days,
            @RequestParam(value = "limit", defaultValue = "10") int limit) {
        return Result.ok(seckillTopBuyerService.queryTopBuyers(
                shopId, Math.max(1, Math.min(days, 30)), Math.max(1, Math.min(limit, 100))));
    }
}
