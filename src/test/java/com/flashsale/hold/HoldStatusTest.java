package com.flashsale.hold;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Plain unit test of the state machine table: no Spring, no database, runs in milliseconds.
 */
class HoldStatusTest {

    @ParameterizedTest(name = "{0} -> {1} allowed: {2}")
    @CsvSource({
            "HELD,            PAYMENT_PENDING, true",
            "HELD,            EXPIRED,         true",
            "HELD,            CANCELLED,       true",
            "HELD,            CONFIRMED,       false",
            "PAYMENT_PENDING, CONFIRMED,       true",
            "PAYMENT_PENDING, HELD,            true",
            "PAYMENT_PENDING, EXPIRED,         false",
            "PAYMENT_PENDING, CANCELLED,       false",
            "CONFIRMED,       CANCELLED,       false",
            "EXPIRED,         HELD,            false",
            "CANCELLED,       HELD,            false"
    })
    void onlyTheMovesInTheTableAreAllowed(HoldStatus from, HoldStatus to, boolean allowed) {
        assertThat(from.canMoveTo(to)).isEqualTo(allowed);
    }

    @ParameterizedTest
    @EnumSource(value = HoldStatus.class, names = {"CONFIRMED", "EXPIRED", "CANCELLED"})
    void finishedHoldsNeverChangeAgain(HoldStatus finished) {
        assertThat(finished.isFinal()).isTrue();
        for (HoldStatus next : HoldStatus.values()) {
            assertThat(finished.canMoveTo(next)).isFalse();
        }
    }
}
