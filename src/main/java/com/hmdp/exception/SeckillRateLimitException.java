package com.hmdp.exception;

public class SeckillRateLimitException extends RuntimeException {

    public SeckillRateLimitException(String message) {
        super(message);
    }

    public SeckillRateLimitException(String message, Throwable cause) {
        super(message, cause);
    }
}
