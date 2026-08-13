package com.hmdp.utils;

import com.hmdp.config.SeckillRateLimitProperties;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClientIpResolverTest {

    @Test
    void forwardedHeaderIsIgnoredUnlessExplicitlyTrusted() {
        SeckillRateLimitProperties properties = new SeckillRateLimitProperties();
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeader("X-Forwarded-For")).thenReturn("203.0.113.8, 10.0.0.1");
        Mockito.when(request.getRemoteAddr()).thenReturn("10.0.0.2");

        ClientIpResolver resolver = new ClientIpResolver(properties);
        assertEquals("10.0.0.2", resolver.resolve(request));

        properties.setTrustForwardedHeaders(true);
        assertEquals("203.0.113.8", resolver.resolve(request));
    }
}
