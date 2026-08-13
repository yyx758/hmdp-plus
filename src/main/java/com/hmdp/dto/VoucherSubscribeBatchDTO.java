package com.hmdp.dto;

import lombok.Data;

import java.util.List;

@Data
public class VoucherSubscribeBatchDTO {

    private List<Long> voucherIdList;
}
