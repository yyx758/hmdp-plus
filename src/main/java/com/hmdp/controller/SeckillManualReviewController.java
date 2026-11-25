package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillManualRetryDTO;
import com.hmdp.service.SeckillAcceptedOrderRecoveryService;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Disabled until an operator token is configured; also requires normal login. */
@RestController
@RequestMapping("/internal/seckill-orders")
public class SeckillManualReviewController {

    private final SeckillAcceptedOrderRecoveryService recoveryService;
    private final String operatorToken;

    public SeckillManualReviewController(
            SeckillAcceptedOrderRecoveryService recoveryService,
            @Value("${hmdp.seckill.manual-review.operator-token:}") String operatorToken) {
        this.recoveryService = recoveryService;
        this.operatorToken = operatorToken;
    }

    @GetMapping("/manual-review")
    public Result listManualReview(
            @RequestHeader(value = "X-Seckill-Operator-Token", required = false)
            String suppliedToken,
            @RequestParam(value = "limit", defaultValue = "100") int limit) {
        requireOperator(suppliedToken);
        return Result.ok(recoveryService.listManualReview(limit));
    }

    @PostMapping("/{orderId}/retry")
    public Result retry(@PathVariable Long orderId,
                        @RequestHeader(value = "X-Seckill-Operator-Token", required = false)
                        String suppliedToken,
                        @RequestBody SeckillManualRetryDTO request) {
        Long operatorUserId = requireOperator(suppliedToken);
        recoveryService.retryManualReview(orderId, operatorUserId,
                request == null ? null : request.getReason());
        return Result.ok(true);
    }

    @PostMapping("/{orderId}/confirm-completed")
    public Result confirmCompleted(@PathVariable Long orderId,
                        @RequestHeader(value = "X-Seckill-Operator-Token", required = false)
                        String suppliedToken,
                        @RequestBody SeckillManualRetryDTO request) {
        Long operatorUserId = requireOperator(suppliedToken);
        recoveryService.confirmCompleted(orderId, operatorUserId,
                request == null ? null : request.getReason());
        return Result.ok(true);
    }

    @PostMapping("/{orderId}/request-failure-review")
    public Result requestFailureReview(@PathVariable Long orderId,
                        @RequestHeader(value = "X-Seckill-Operator-Token", required = false)
                        String suppliedToken,
                        @RequestBody SeckillManualRetryDTO request) {
        Long operatorUserId = requireOperator(suppliedToken);
        recoveryService.requestFailureReview(orderId, operatorUserId,
                request == null ? null : request.getReason());
        return Result.ok(true);
    }

    private Long requireOperator(String suppliedToken) {
        if (operatorToken == null || operatorToken.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
        if (suppliedToken == null || !MessageDigest.isEqual(
                operatorToken.getBytes(StandardCharsets.UTF_8),
                suppliedToken.getBytes(StandardCharsets.UTF_8))) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN);
        }
        if (UserHolder.getUser() == null || UserHolder.getUser().getId() == null) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
        return UserHolder.getUser().getId();
    }
}
