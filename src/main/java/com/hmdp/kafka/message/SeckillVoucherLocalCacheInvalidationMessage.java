package com.hmdp.kafka.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Cross-instance command that invalidates only the JVM-local seckill voucher metadata cache. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeckillVoucherLocalCacheInvalidationMessage {

    private String eventId;
    private Long voucherId;
    private String reason;
    private long occurredAtEpochMillis;
}
