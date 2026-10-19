package com.flashsale.hold;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;

/**
 * All of CartTest again with the Redis stock gate switched OFF, so the database strategies alone must give
 * the same answers: no half carts, PARTIAL holds what is there, no deadlocks, no oversells.
 */
class CartWithoutGateTest extends CartTest {

    @Override
    protected Boolean stockGate() {
        return false; // what the header "X-Stock-Gate: off" does over HTTP
    }

    @AfterEach
    void redisWasNeverAsked() {
        assertThat(redis.keys("stock:*")).as("no gate counter was created").isEmpty();
    }
}
