package com.flashsale.ratelimit;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Tells Spring MVC which routes go through RateLimitInterceptor: everything under /api except the admin API.
 * (Actuator's /actuator/** is outside /api, so health checks are never limited.)
 */
@Configuration(proxyBeanMethods = false)
class RateLimitWebConfig implements WebMvcConfigurer {

    private final RateLimitInterceptor rateLimitInterceptor;

    RateLimitWebConfig(RateLimitInterceptor rateLimitInterceptor) {
        this.rateLimitInterceptor = rateLimitInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(rateLimitInterceptor)
                .addPathPatterns("/api/**")
                .excludePathPatterns("/api/admin/**");
    }
}
