package com.hmdp.utils;

import com.hmdp.dto.UserDTO;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * 登录拦截器
 * @author HH
 * @version 1.0
 */
public class LoginInterceptor implements HandlerInterceptor {


    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

        UserDTO user = UserHolder.getUser();
        // 3.判断用户是否存在
        if(user == null)    {
            // 4. 不存在直接拦截 401 未授权
            response.setStatus(401);
            return false;
        }

        return true;
    }


}
