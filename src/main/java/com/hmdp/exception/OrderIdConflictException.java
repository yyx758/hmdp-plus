package com.hmdp.exception;

import lombok.Getter;

@Getter
public class OrderIdConflictException extends IllegalStateException {

    private final Long currentOrderId;
    private final Long existingOrderId;

    public OrderIdConflictException(Long currentOrderId, Long existingOrderId) {
        super("同一用户和秒杀券已存在其他订单，currentOrderId=" + currentOrderId
                + "，existingOrderId=" + existingOrderId);
        this.currentOrderId = currentOrderId;
        this.existingOrderId = existingOrderId;
    }
}
