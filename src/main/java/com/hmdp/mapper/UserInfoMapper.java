package com.hmdp.mapper;

import com.hmdp.entity.UserInfo;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-24
 */
public interface UserInfoMapper extends BaseMapper<UserInfo> {

    @Select("SELECT user_id FROM tb_user_info WHERE level >= #{minLevel} " +
            "ORDER BY level DESC, credits DESC, user_id ASC LIMIT #{limit}")
    List<Long> selectVipUserIds(
            @Param("minLevel") int minLevel,
            @Param("limit") int limit);
}
