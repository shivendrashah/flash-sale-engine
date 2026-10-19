package com.flashsale;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * The starting point of the whole app. Running main() starts the web server, connects to PostgreSQL and
 * Redis, and creates every object the app needs.
 *
 *           (HikariCP), Flyway (runs the SQL in db/migration), Hibernate, the Redis client, JSON (Jackson), and an
 *           embedded Tomcat web server.
 *           package (com.flashsale) and its sub-packages, creates one object of each ("a bean"), and passes them
 *           into each other's constructors ("dependency injection").
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class FlashSaleEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlashSaleEngineApplication.class, args);
    }
}
