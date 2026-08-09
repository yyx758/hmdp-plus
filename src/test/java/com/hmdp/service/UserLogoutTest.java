package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.service.impl.UserServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserLogoutTest {

    private UserServiceImpl userService;
    private StringRedisTemplate redisTemplate;

    @BeforeEach
    void setUp() {
        userService = new UserServiceImpl();
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        ReflectionTestUtils.setField(userService, "stringRedisTemplate", redisTemplate);
    }

    @Test
    void deletesCurrentLoginToken() {
        Result result = userService.logout("test-token");

        assertTrue(result.getSuccess());
        Mockito.verify(redisTemplate).delete(LOGIN_USER_KEY + "test-token");
    }
}
