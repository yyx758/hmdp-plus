package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.dto.CancelVoucherOrderDTO;
import com.hmdp.enums.SeckillRateLimitScene;
import com.hmdp.service.ISeckillAccessTokenService;
import com.hmdp.service.ISeckillRateLimitService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.SeckillOrderPressureService;
import com.hmdp.utils.ClientIpResolver;
import com.hmdp.utils.UserHolder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private ISeckillAccessTokenService seckillAccessTokenService;

    @Resource
    private ISeckillRateLimitService seckillRateLimitService;

    @Resource
    private ClientIpResolver clientIpResolver;

    @Resource
    private SeckillOrderPressureService seckillOrderPressureService;

    @GetMapping("seckill/token/{id}")
    public Result issueSeckillAccessToken(
            @PathVariable("id") Long voucherId,
            HttpServletRequest request) {
        UserDTO user = UserHolder.getUser();
        seckillRateLimitService.check(
                voucherId,
                user,
                clientIpResolver.resolve(request),
                SeckillRateLimitScene.ISSUE_SECKILL_ACCESS_TOKEN);
        return Result.ok(seckillAccessTokenService.issueAccessToken(voucherId, user.getId()));
    }

    @PostMapping("seckill/{id}")
    public Result seckillVoucher(
            @PathVariable("id") Long voucherId,
            @RequestParam(value = "accessToken", required = false) String accessToken,
            HttpServletRequest request) {
        UserDTO user = UserHolder.getUser();
        seckillRateLimitService.check(
                voucherId,
                user,
                clientIpResolver.resolve(request),
                SeckillRateLimitScene.SECKILL_ORDER);
        return voucherOrderService.seckillVoucher(voucherId, accessToken);
    }

    @GetMapping("seckill/status/{orderId}")
    public Result querySeckillOrderStatus(@PathVariable("orderId") Long orderId) {
        return voucherOrderService.querySeckillOrderStatus(orderId);
    }

    @GetMapping("seckill/pressure")
    public Result querySeckillPressure() {
        java.util.Map<String, Object> pressure = new java.util.LinkedHashMap<>();
        pressure.put("level", seckillOrderPressureService.getLevel());
        pressure.put("backlog", seckillOrderPressureService.getBacklog());
        pressure.put("admissionMultiplier",
                seckillOrderPressureService.getAdmissionMultiplier());
        return Result.ok(pressure);
    }

    @GetMapping("voucher/{voucherId}")
    public Result queryActiveOrderId(@PathVariable("voucherId") Long voucherId) {
        return voucherOrderService.queryActiveOrderId(voucherId);
    }

    @PostMapping("cancel")
    public Result cancelVoucherOrder(@RequestBody CancelVoucherOrderDTO request) {
        if (request == null || request.getVoucherId() == null) {
            return Result.fail("优惠券ID不能为空");
        }
        return voucherOrderService.cancelVoucherOrder(request.getVoucherId());
    }
}
