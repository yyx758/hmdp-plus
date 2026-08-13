package com.hmdp.enums;

public enum VoucherSubscribeStatus {

    UNSUBSCRIBED(0),
    SUBSCRIBED(1),
    ISSUED(2);

    private final int code;

    VoucherSubscribeStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
