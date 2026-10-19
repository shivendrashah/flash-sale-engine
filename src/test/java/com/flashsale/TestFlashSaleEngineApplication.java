package com.flashsale;

import org.springframework.boot.SpringApplication;

/**
 *          (here: the containers from TestcontainersConfiguration).
 */
public class TestFlashSaleEngineApplication {

    public static void main(String[] args) {
        SpringApplication.from(FlashSaleEngineApplication::main).with(TestcontainersConfiguration.class).run(args);
    }
}
