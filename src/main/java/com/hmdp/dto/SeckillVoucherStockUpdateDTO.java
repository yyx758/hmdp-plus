package com.hmdp.dto;

import lombok.Data;

@Data
public class SeckillVoucherStockUpdateDTO {

    private Long voucherId;

    /**
     * 正数补充库存，负数扣减库存。
     */
    private Integer changeStock;
}
