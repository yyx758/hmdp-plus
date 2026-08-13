package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.digest.BCrypt;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.PasswordUpdateDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.dto.UserProfileUpdateDTO;
import com.hmdp.entity.User;
import com.hmdp.entity.UserInfo;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserInfoService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserInfoService userInfoService;

    @Override
    public Result sendCode(String phone) {
        // 1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2.如果不符合，返回错误信息
            return Result.fail("手机号格式错误！");
        }
        // 3.符合，生成验证码
        String code = RandomUtil.randomNumbers(6);

        // 4.保存验证码到 session
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);

        // 5.发送验证码
        log.debug("发送短信验证码成功，验证码：{}", code);
        // 返回ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm) {
        // 1.校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2.如果不符合，返回错误信息
            return Result.fail("手机号格式错误！");
        }
        // 3.从redis获取验证码并校验
        if (StrUtil.isNotBlank(loginForm.getPassword())) {
            return loginWithPassword(phone, loginForm.getPassword());
        }
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (cacheCode == null || !cacheCode.equals(code)) {
            // 不一致，报错
            return Result.fail("验证码错误");
        }

        // 4.一致，根据手机号查询用户 select * from tb_user where phone = ?
        User user = query().eq("phone", phone).one();

        // 5.判断用户是否存在
        if (user == null) {
            // 6.不存在，创建新用户并保存
            user = createUserWithPhone(phone);
        }

        // 7.保存用户信息到 Redis 并返回登录令牌
        return createLoginToken(user);
    }

    private Result loginWithPassword(String phone, String password) {
        User user = query().eq("phone", phone).one();
        if (user == null || StrUtil.isBlank(user.getPassword()) || !passwordMatches(password, user.getPassword())) {
            return Result.fail("手机号或密码错误");
        }

        return createLoginToken(user);
    }

    private Result createLoginToken(User user) {
        String userTokenKey = LOGIN_USER_INDEX_KEY + user.getId();
        String oldToken = stringRedisTemplate.opsForValue().getAndDelete(userTokenKey);
        if (StrUtil.isNotBlank(oldToken)) {
            stringRedisTemplate.delete(LOGIN_USER_KEY + oldToken);
            log.debug("旧用户Token已删除，userId={}，token={}", user.getId(), oldToken);
        }

        String newToken = UUID.randomUUID().toString(true);
        stringRedisTemplate.opsForValue().set(
                userTokenKey,
                newToken,
                LOGIN_USER_TTL,
                TimeUnit.MINUTES
        );

        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        UserInfo userInfo = userInfoService.getById(user.getId());
        userDTO.setCredits(userInfo == null || userInfo.getCredits() == null
                ? 0 : userInfo.getCredits());
        userDTO.setLevel(userInfo == null || userInfo.getLevel() == null
                ? 0 : userInfo.getLevel());
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        String tokenKey = LOGIN_USER_KEY + newToken;
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);
        return Result.ok(newToken);
    }

    private boolean passwordMatches(String rawPassword, String encodedPassword) {
        try {
            return BCrypt.checkpw(rawPassword, encodedPassword);
        } catch (RuntimeException e) {
            return false;
        }
    }

    @Override
    public Result sign() {
        // 1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        // 2.获取日期
        LocalDateTime now = LocalDateTime.now();
        // 3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        // 4.获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        // 5.写入Redis SETBIT key offset 1
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        // 1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        // 2.获取日期
        LocalDateTime now = LocalDateTime.now();
        // 3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        // 4.获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        // 5.获取本月截止今天为止的所有的签到记录，返回的是一个十进制的数字 BITFIELD sign:5:202203 GET u14 0
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
        );
        if (result == null || result.isEmpty()) {
            // 没有任何签到结果
            return Result.ok(0);
        }
        Long num = result.get(0);
        if (num == null || num == 0) {
            return Result.ok(0);
        }
        // 6.循环遍历
        int count = 0;
        while (true) {
            // 6.1.让这个数字与1做与运算，得到数字的最后一个bit位  // 判断这个bit位是否为0
            if ((num & 1) == 0) {
                // 如果为0，说明未签到，结束
                break;
            }else {
                // 如果不为0，说明已签到，计数器+1
                count++;
            }
            // 把数字右移一位，抛弃最后一个bit位，继续下一个bit位
            num >>>= 1;
        }
        return Result.ok(count);
    }

    @Override
    @Transactional
    public Result updateProfile(UserProfileUpdateDTO profile, String token) {
        if (profile == null || StrUtil.isBlank(profile.getNickName())) {
            return Result.fail("昵称不能为空");
        }

        String nickName = profile.getNickName().trim();
        if (nickName.length() > 32) {
            return Result.fail("昵称不能超过32个字符");
        }
        if (profile.getIcon() != null && profile.getIcon().length() > 255) {
            return Result.fail("头像地址过长");
        }
        if (profile.getCity() != null && profile.getCity().length() > 64) {
            return Result.fail("城市名称不能超过64个字符");
        }
        if (profile.getIntroduce() != null && profile.getIntroduce().length() > 128) {
            return Result.fail("个人介绍不能超过128个字符");
        }
        if (profile.getBirthday() != null && profile.getBirthday().isAfter(java.time.LocalDate.now())) {
            return Result.fail("生日不能晚于今天");
        }

        UserDTO currentUser = UserHolder.getUser();
        Long userId = currentUser.getId();

        User user = new User();
        user.setId(userId);
        user.setNickName(nickName);
        user.setIcon(profile.getIcon());
        if (!updateById(user)) {
            return Result.fail("用户不存在");
        }

        UserInfo userInfo = userInfoService.getById(userId);
        boolean newUserInfo = userInfo == null;
        if (newUserInfo) {
            userInfo = new UserInfo();
            userInfo.setUserId(userId);
        }
        userInfo.setCity(profile.getCity());
        userInfo.setIntroduce(profile.getIntroduce());
        userInfo.setGender(profile.getGender());
        userInfo.setBirthday(profile.getBirthday());
        if (newUserInfo) {
            userInfoService.save(userInfo);
        } else {
            userInfoService.updateById(userInfo);
        }

        currentUser.setNickName(nickName);
        currentUser.setIcon(profile.getIcon());
        if (StrUtil.isNotBlank(token)) {
            Map<String, Object> userMap = BeanUtil.beanToMap(currentUser, new HashMap<>(),
                    CopyOptions.create()
                            .setIgnoreNullValue(true)
                            .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
            stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY + token, userMap);
        }
        return Result.ok();
    }

    @Override
    public Result updatePassword(PasswordUpdateDTO passwordUpdate) {
        if (passwordUpdate == null || StrUtil.isBlank(passwordUpdate.getNewPassword())) {
            return Result.fail("新密码不能为空");
        }
        String newPassword = passwordUpdate.getNewPassword();
        if (newPassword.length() < 6 || newPassword.length() > 20) {
            return Result.fail("密码长度必须为6到20位");
        }

        Long userId = UserHolder.getUser().getId();
        User user = getById(userId);
        if (user == null) {
            return Result.fail("用户不存在");
        }
        if (StrUtil.isNotBlank(user.getPassword())
                && (StrUtil.isBlank(passwordUpdate.getOldPassword())
                || !passwordMatches(passwordUpdate.getOldPassword(), user.getPassword()))) {
            return Result.fail("原密码错误");
        }

        User update = new User();
        update.setId(userId);
        update.setPassword(BCrypt.hashpw(newPassword, BCrypt.gensalt()));
        if (!updateById(update)) {
            return Result.fail("密码保存失败");
        }
        return Result.ok();
    }

    @Override
    public Result logout(String token) {
        if (StrUtil.isBlank(token)) {
            return Result.ok();
        }
        stringRedisTemplate.delete(LOGIN_USER_KEY + token);
        return Result.ok();
    }

    private User createUserWithPhone(String phone) {
        // 1.创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        // 2.保存用户
        save(user);
        return user;
    }
}
