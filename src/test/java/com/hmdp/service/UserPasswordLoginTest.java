package com.hmdp.service;

import cn.hutool.crypto.digest.BCrypt;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.PasswordUpdateDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.impl.UserServiceImpl;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_INDEX_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserPasswordLoginTest {

    private UserServiceImpl userService;
    private UserMapper userMapper;
    private StringRedisTemplate redisTemplate;
    private HashOperations<String, Object, Object> hashOperations;
    private ValueOperations<String, String> valueOperations;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        userService = new UserServiceImpl();
        userMapper = Mockito.mock(UserMapper.class);
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        hashOperations = Mockito.mock(HashOperations.class);
        valueOperations = Mockito.mock(ValueOperations.class);

        Mockito.when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        Mockito.when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        Mockito.when(valueOperations.get(Mockito.anyString())).thenReturn("sms-code");
        ReflectionTestUtils.setField(userService, "baseMapper", userMapper);
        ReflectionTestUtils.setField(userService, "stringRedisTemplate", redisTemplate);

        UserDTO currentUser = new UserDTO();
        currentUser.setId(9L);
        UserHolder.saveUser(currentUser);
    }

    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
    }

    @Test
    void logsInWithPhoneAndPassword() {
        User user = new User();
        user.setId(9L);
        user.setPhone("13800138000");
        user.setNickName("密码用户");
        user.setPassword(BCrypt.hashpw("correct-pass", BCrypt.gensalt()));
        Mockito.when(userMapper.selectOne(Mockito.any())).thenReturn(user);

        LoginFormDTO form = new LoginFormDTO();
        form.setPhone("13800138000");
        form.setPassword("correct-pass");

        Result result = userService.login(form);

        assertTrue(result.getSuccess());
        assertNotNull(result.getData());
        String token = result.getData().toString();
        Mockito.verify(valueOperations).getAndDelete(LOGIN_USER_INDEX_KEY + user.getId());
        Mockito.verify(valueOperations).set(
                LOGIN_USER_INDEX_KEY + user.getId(),
                token,
                LOGIN_USER_TTL,
                TimeUnit.MINUTES
        );
        Mockito.verify(hashOperations).putAll(
                Mockito.eq(LOGIN_USER_KEY + token),
                Mockito.anyMap()
        );
        Mockito.verify(redisTemplate).expire(
                LOGIN_USER_KEY + token,
                LOGIN_USER_TTL,
                TimeUnit.MINUTES
        );
    }

    @Test
    void verificationCodeLoginDeletesOldTokenAndUsesSharedTokenCreation() {
        User user = new User();
        user.setId(9L);
        user.setPhone("13800138000");
        user.setNickName("verification-user");
        Mockito.when(userMapper.selectOne(Mockito.any())).thenReturn(user);
        Mockito.when(valueOperations.get("login:code:13800138000")).thenReturn("123456");
        Mockito.when(valueOperations.getAndDelete(LOGIN_USER_INDEX_KEY + user.getId()))
                .thenReturn("old-token");

        LoginFormDTO form = new LoginFormDTO();
        form.setPhone("13800138000");
        form.setCode("123456");

        Result result = userService.login(form);

        assertTrue(result.getSuccess());
        Mockito.verify(redisTemplate).delete(LOGIN_USER_KEY + "old-token");
        Mockito.verify(valueOperations).set(
                LOGIN_USER_INDEX_KEY + user.getId(),
                result.getData().toString(),
                LOGIN_USER_TTL,
                TimeUnit.MINUTES
        );
    }

    @Test
    void rejectsWrongPassword() {
        User user = new User();
        user.setId(9L);
        user.setPhone("13800138000");
        user.setPassword(BCrypt.hashpw("correct-pass", BCrypt.gensalt()));
        Mockito.when(userMapper.selectOne(Mockito.any())).thenReturn(user);

        LoginFormDTO form = new LoginFormDTO();
        form.setPhone("13800138000");
        form.setPassword("wrong-pass");

        Result result = userService.login(form);

        assertEquals(false, result.getSuccess());
        assertEquals("手机号或密码错误", result.getErrorMsg());
    }

    @Test
    void setsFirstPasswordUsingBcrypt() {
        User user = new User();
        user.setId(9L);
        user.setPassword("");
        Mockito.when(userMapper.selectById(9L)).thenReturn(user);
        Mockito.when(userMapper.updateById(Mockito.any(User.class))).thenReturn(1);

        PasswordUpdateDTO form = new PasswordUpdateDTO();
        form.setNewPassword("new-pass");

        Result result = userService.updatePassword(form);

        assertTrue(result.getSuccess());
        ArgumentCaptor<User> captor = ArgumentCaptor.forClass(User.class);
        Mockito.verify(userMapper).updateById(captor.capture());
        assertTrue(BCrypt.checkpw("new-pass", captor.getValue().getPassword()));
    }

    @Test
    void requiresOldPasswordWhenChangingExistingPassword() {
        User user = new User();
        user.setId(9L);
        user.setPassword(BCrypt.hashpw("old-pass", BCrypt.gensalt()));
        Mockito.when(userMapper.selectById(9L)).thenReturn(user);

        PasswordUpdateDTO form = new PasswordUpdateDTO();
        form.setOldPassword("wrong-pass");
        form.setNewPassword("new-pass");

        Result result = userService.updatePassword(form);

        assertEquals(false, result.getSuccess());
        assertEquals("原密码错误", result.getErrorMsg());
        Mockito.verify(userMapper, Mockito.never()).updateById(Mockito.any(User.class));
    }
}
