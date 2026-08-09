package com.hmdp.exception;

/**
 * Redis已经完成库存预扣，但数据库库存无法按批次扣减。
 * 这属于整批一致性异常，继续拆小批次只会制造部分成功，必须保留Pending等待恢复或补偿。
 */
public class DatabaseStockMismatchException extends IllegalStateException {

    public DatabaseStockMismatchException(Long voucherId, int amount) {
        super("数据库库存批量扣减失败，voucherId=" + voucherId + "，amount=" + amount);
    }
}
