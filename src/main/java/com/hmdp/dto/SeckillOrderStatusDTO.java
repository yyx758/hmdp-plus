package com.hmdp.dto;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class SeckillOrderStatusDTO {

    private Long orderId;
    private String status;
    private Long existingOrderId;
    private String message;
}
