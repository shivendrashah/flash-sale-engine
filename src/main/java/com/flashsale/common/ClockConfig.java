package com.flashsale.common;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Gives the whole app one shared clock (UTC).
 *
 * Why not just call Instant.now() everywhere? Because tests need to control time. Tests replace this bean with a
 * MutableClock (src/test/.../support/MutableClock.java) and jump "11 minutes later" instantly to test expiry.
 */
@Configuration(proxyBeanMethods = false)
public class ClockConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
