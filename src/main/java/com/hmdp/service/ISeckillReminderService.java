package com.hmdp.service;

import java.time.LocalDateTime;

public interface ISeckillReminderService {

    void schedule(Long voucherId, LocalDateTime beginTime);
}
