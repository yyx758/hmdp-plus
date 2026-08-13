package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class VoucherSubscribeStatusDTO {

    private Long voucherId;

    private Integer subscribeStatus;
}
