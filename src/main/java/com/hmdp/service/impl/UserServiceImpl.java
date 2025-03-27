package com.hmdp.service.impl;

import cn.hutool.captcha.generator.RandomGenerator;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;

import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Override
    public Result sendCode(String phone, HttpSession session) {
        if (!RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("Phone number is invalid.");
        }

        String code = RandomUtil.randomNumbers(6);

        session.setAttribute("code", code);

        log.debug("Send code: {}", code);

        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        if (!RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("Phone number is invalid.");
        }

        String cacheCode = (String) session.getAttribute("code");
        String code = loginForm.getCode();
        if(cacheCode == null || !cacheCode.equals(code)) {
            return Result.fail("Wrong code.");
        }

        User user = query().eq("phone", phone).one();

        if(user == null) {
            user = createUserWithPhone(phone);
        }

        session.setAttribute("user", user);

        return Result.ok();
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomNumbers(10));
        save(user);
        return user;
    }
}
