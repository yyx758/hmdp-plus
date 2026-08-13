package com.hmdp.kafka.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeckillOrderMessage {

    private String eventId;
    private Long outboxId;
    private Long orderId;
    private Long voucherId;
    private Long userId;
    private Boolean autoIssued;
    private long occurredAtEpochMillis;
}
