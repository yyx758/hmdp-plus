package com.hmdp.dto;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class SeckillOrderStatusDTO {

    @JsonSerialize(using = ToStringSerializer.class)
    private Long orderId;
    private String status;

    @JsonSerialize(using = ToStringSerializer.class)
    private Long existingOrderId;
    private String message;
}
