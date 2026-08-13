package com.hmdp.exception;

/** A permanent mismatch for an idempotency key already present in the outbox. */
public class SeckillOutboxEventConflictException extends RuntimeException {

    public SeckillOutboxEventConflictException(String message) {
        super(message);
    }
}
