package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.dto.UserProfileUpdateDTO;
import com.hmdp.entity.User;
import com.hmdp.entity.UserInfo;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.impl.UserServiceImpl;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDate;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserServiceProfileTest {

    private UserServiceImpl userService;
    private UserMapper userMapper;
    private IUserInfoService userInfoService;
    private HashOperations<String, Object, Object> hashOperations;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        userService = new UserServiceImpl();
        userMapper = Mockito.mock(UserMapper.class);
        userInfoService = Mockito.mock(IUserInfoService.class);
        StringRedisTemplate redisTemplate = Mockito.mock(StringRedisTemplate.class);
        hashOperations = Mockito.mock(HashOperations.class);

        Mockito.when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        Mockito.when(userMapper.updateById(Mockito.any(User.class))).thenReturn(1);
        Mockito.when(userInfoService.save(Mockito.any(UserInfo.class))).thenReturn(true);

        ReflectionTestUtils.setField(userService, "baseMapper", userMapper);
        ReflectionTestUtils.setField(userService, "userInfoService", userInfoService);
        ReflectionTestUtils.setField(userService, "stringRedisTemplate", redisTemplate);

        UserDTO currentUser = new UserDTO();
        currentUser.setId(7L);
        currentUser.setNickName("旧昵称");
        UserHolder.saveUser(currentUser);
    }

    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
    }

    @Test
    void updatesUserAndCreatesMissingUserInfo() {
        UserProfileUpdateDTO profile = new UserProfileUpdateDTO();
        profile.setNickName("新昵称");
        profile.setIcon("/imgs/avatar.png");
        profile.setCity("上海");
        profile.setIntroduce("你好");
        profile.setGender(true);
        profile.setBirthday(LocalDate.of(2000, 1, 2));

        Result result = userService.updateProfile(profile, "test-token");

        assertTrue(result.getSuccess());

        ArgumentCaptor<User> userCaptor = ArgumentCaptor.forClass(User.class);
        Mockito.verify(userMapper).updateById(userCaptor.capture());
        assertEquals(7L, userCaptor.getValue().getId());
        assertEquals("新昵称", userCaptor.getValue().getNickName());
        assertEquals("/imgs/avatar.png", userCaptor.getValue().getIcon());

        ArgumentCaptor<UserInfo> infoCaptor = ArgumentCaptor.forClass(UserInfo.class);
        Mockito.verify(userInfoService).save(infoCaptor.capture());
        assertEquals(7L, infoCaptor.getValue().getUserId());
        assertEquals("上海", infoCaptor.getValue().getCity());
        assertEquals("你好", infoCaptor.getValue().getIntroduce());
        assertEquals(true, infoCaptor.getValue().getGender());
        assertEquals(LocalDate.of(2000, 1, 2), infoCaptor.getValue().getBirthday());

        Mockito.verify(hashOperations).putAll(
                Mockito.eq("login:token:test-token"),
                Mockito.<Map<Object, Object>>any());
    }

    @Test
    void rejectsBlankNickname() {
        UserProfileUpdateDTO profile = new UserProfileUpdateDTO();
        profile.setNickName("  ");

        Result result = userService.updateProfile(profile, "test-token");

        assertEquals(false, result.getSuccess());
        assertEquals("昵称不能为空", result.getErrorMsg());
        Mockito.verifyNoInteractions(userMapper, userInfoService);
    }
}
