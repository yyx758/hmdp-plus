package com.hmdp.controller;

import com.hmdp.service.IUserInfoService;
import com.hmdp.service.IUserService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

class UserControllerSessionTest {

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        UserController controller = new UserController();
        ReflectionTestUtils.setField(controller, "userService", Mockito.mock(IUserService.class));
        ReflectionTestUtils.setField(controller, "userInfoService", Mockito.mock(IUserInfoService.class));
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    void sendingLoginCodeDoesNotCreateHttpSession() throws Exception {
        MvcResult result = mockMvc.perform(post("/user/code").param("phone", "13800138000"))
                .andReturn();

        assertNull(result.getRequest().getSession(false));
    }

    @Test
    void loggingInDoesNotCreateHttpSession() throws Exception {
        MvcResult result = mockMvc.perform(post("/user/login")
                        .contentType("application/json")
                        .content("{\"phone\":\"13800138000\",\"code\":\"123456\"}"))
                .andReturn();

        assertNull(result.getRequest().getSession(false));
    }
}
