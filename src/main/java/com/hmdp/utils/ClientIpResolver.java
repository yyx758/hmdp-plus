package com.hmdp.utils;

import com.hmdp.config.SeckillRateLimitProperties;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;

@Component
public class ClientIpResolver {

    private final SeckillRateLimitProperties properties;

    public ClientIpResolver(SeckillRateLimitProperties properties) {
        this.properties = properties;
    }

    /*
    * 解析http请求
    * */
    public String resolve(HttpServletRequest request) {
        //请求为null,返回unknow
        if (request == null) {
            return "unknown";
        }
        if (properties.isTrustForwardedHeaders()) {
            //如果相信传入的请求头
            String forwardedFor = request.getHeader("X-Forwarded-For");
            if (forwardedFor != null) {
                String first = forwardedFor.split(",", 2)[0].trim();
                if (!first.isEmpty()) {
                    return first;
                }
            }
            String realIp = request.getHeader("X-Real-IP");
            if (realIp != null && !realIp.trim().isEmpty()) {
                return realIp.trim();
            }
        }
        String remoteAddress = request.getRemoteAddr();
        return remoteAddress == null || remoteAddress.trim().isEmpty()
                ? "unknown"
                : remoteAddress.trim();
    }
}
