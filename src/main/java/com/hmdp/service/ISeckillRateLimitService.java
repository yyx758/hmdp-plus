package com.hmdp.service;

import com.hmdp.dto.UserDTO;
import com.hmdp.enums.SeckillRateLimitScene;

public interface ISeckillRateLimitService {

    void check(
            Long voucherId,
            UserDTO user,
            String clientIp,
            SeckillRateLimitScene scene);
}
