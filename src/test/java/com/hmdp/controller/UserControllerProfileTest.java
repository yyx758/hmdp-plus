package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.dto.PasswordUpdateDTO;
import com.hmdp.dto.UserProfileUpdateDTO;
import com.hmdp.service.IUserInfoService;
import com.hmdp.service.IUserService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class UserControllerProfileTest {

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        UserController controller = new UserController();
        IUserService userService = Mockito.mock(IUserService.class);
        Mockito.when(userService.updateProfile(
                        Mockito.any(UserProfileUpdateDTO.class),
                        Mockito.eq("test-token")))
                .thenReturn(Result.ok());
        Mockito.when(userService.updatePassword(Mockito.any(PasswordUpdateDTO.class)))
                .thenReturn(Result.ok());
        Mockito.when(userService.logout("test-token")).thenReturn(Result.ok());
        ReflectionTestUtils.setField(controller, "userService", userService);
        ReflectionTestUtils.setField(controller, "userInfoService", Mockito.mock(IUserInfoService.class));
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    void updatesCurrentUsersProfile() throws Exception {
        mockMvc.perform(put("/user/profile")
                        .header("authorization", "test-token")
                        .contentType("application/json")
                        .content("{\"nickName\":\"新昵称\",\"city\":\"上海\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));
    }

    @Test
    void updatesCurrentUsersPassword() throws Exception {
        mockMvc.perform(put("/user/password")
                        .contentType("application/json")
                        .content("{\"oldPassword\":\"old-pass\",\"newPassword\":\"new-pass\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));
    }

    @Test
    void logsOutCurrentToken() throws Exception {
        mockMvc.perform(post("/user/logout")
                        .header("authorization", "test-token"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));
    }
}
