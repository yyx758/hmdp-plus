package com.hmdp.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UserInfoMappingTest {

    @Test
    void userIdMustBeIncludedInInsertStatements() throws NoSuchFieldException {
        Field userId = UserInfo.class.getDeclaredField("userId");
        TableId tableId = userId.getAnnotation(TableId.class);

        assertEquals(IdType.INPUT, tableId.type());
    }
}
