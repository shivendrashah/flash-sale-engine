package com.flashsale.inventory;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * Hands out the ReservationPolicy for a ReservationMode.
 */
@Component
public class ReservationPolicyFactory {

    private final Map<ReservationMode, ReservationPolicy> byMode = new EnumMap<>(ReservationMode.class);

    public ReservationPolicyFactory(List<ReservationPolicy> policies) {
        for (ReservationPolicy policy : policies) {
            if (byMode.put(policy.mode(), policy) != null) {
                throw new IllegalStateException("Two ReservationPolicies registered for " + policy.mode());
            }
        }
        // Fail at startup, not at 10:00 AM during the sale, if a mode has no policy.
        for (ReservationMode mode : ReservationMode.values()) {
            if (!byMode.containsKey(mode)) {
                throw new IllegalStateException("No ReservationPolicy registered for " + mode);
            }
        }
    }

    public ReservationPolicy get(ReservationMode mode) {
        return byMode.get(mode);
    }
}
