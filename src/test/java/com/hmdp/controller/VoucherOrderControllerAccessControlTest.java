package com.hmdp.controller;

import com.hmdp.config.WebExceptionAdvice;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.enums.SeckillRateLimitScene;
import com.hmdp.exception.SeckillRateLimitException;
import com.hmdp.service.ISeckillAccessTokenService;
import com.hmdp.service.ISeckillRateLimitService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.ClientIpResolver;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import javax.servlet.http.HttpServletRequest;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class VoucherOrderControllerAccessControlTest {

    private MockMvc mockMvc;
    private IVoucherOrderService voucherOrderService;
    private ISeckillAccessTokenService accessTokenService;
    private ISeckillRateLimitService rateLimitService;

    @BeforeEach
    void setUp() {
        VoucherOrderController controller = new VoucherOrderController();
        voucherOrderService = Mockito.mock(IVoucherOrderService.class);
        accessTokenService = Mockito.mock(ISeckillAccessTokenService.class);
        rateLimitService = Mockito.mock(ISeckillRateLimitService.class);
        ClientIpResolver clientIpResolver = Mockito.mock(ClientIpResolver.class);

        ReflectionTestUtils.setField(controller, "voucherOrderService", voucherOrderService);
        ReflectionTestUtils.setField(controller, "seckillAccessTokenService", accessTokenService);
        ReflectionTestUtils.setField(controller, "seckillRateLimitService", rateLimitService);
        ReflectionTestUtils.setField(controller, "clientIpResolver", clientIpResolver);
        Mockito.when(clientIpResolver.resolve(Mockito.any(HttpServletRequest.class)))
                .thenReturn("10.0.0.8");

        UserDTO user = new UserDTO();
        user.setId(7L);
        user.setLevel(1);
        user.setCredits(1200);
        UserHolder.saveUser(user);

        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new WebExceptionAdvice())
                .build();
    }

    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
    }

    @Test
    void issuesTokenOnlyAfterRateLimitAllowsRequest() throws Exception {
        Mockito.when(accessTokenService.issueAccessToken(2L, 7L)).thenReturn("access-token");

        mockMvc.perform(get("/voucher-order/seckill/token/2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data").value("access-token"));

        Mockito.verify(rateLimitService).check(
                Mockito.eq(2L),
                Mockito.argThat(user -> user != null && Long.valueOf(7L).equals(user.getId())),
                Mockito.eq("10.0.0.8"),
                Mockito.eq(SeckillRateLimitScene.ISSUE_SECKILL_ACCESS_TOKEN));
    }

    @Test
    void missingAccessTokenIsCheckedInsideAtomicDeductionChain() throws Exception {
        Mockito.when(voucherOrderService.seckillVoucher(2L, null))
                .thenReturn(Result.fail("资格令牌无效或已过期，请重新获取"));

        mockMvc.perform(post("/voucher-order/seckill/2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(false));

        Mockito.verify(voucherOrderService).seckillVoucher(2L, null);
        Mockito.verify(accessTokenService, Mockito.never())
                .validateAndConsume(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString());
    }

    @Test
    void validSingleUseTokenAllowsSeckillServiceCall() throws Exception {
        Mockito.when(voucherOrderService.seckillVoucher(2L, "valid-token"))
                .thenReturn(Result.ok(1001L));

        mockMvc.perform(post("/voucher-order/seckill/2")
                        .param("accessToken", "valid-token"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data").value(1001));

        Mockito.verify(voucherOrderService).seckillVoucher(2L, "valid-token");
        Mockito.verify(accessTokenService, Mockito.never())
                .validateAndConsume(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString());
    }

    @Test
    void rateLimitRejectionUsesHttp429() throws Exception {
        Mockito.doThrow(new SeckillRateLimitException("操作过于频繁，请稍后重试"))
                .when(rateLimitService)
                .check(
                        Mockito.eq(2L),
                        Mockito.any(UserDTO.class),
                        Mockito.eq("10.0.0.8"),
                        Mockito.eq(SeckillRateLimitScene.ISSUE_SECKILL_ACCESS_TOKEN));

        mockMvc.perform(get("/voucher-order/seckill/token/2"))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.errorMsg").value("操作过于频繁，请稍后重试"));
    }
}
