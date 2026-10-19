package com.flashsale.common;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.stream.Collectors;

/**
 * Turns the important parts of a request into a fixed-length SHA-256 hash (64 hex characters).
 * WHY:   We store it next to the Idempotency-Key. When the same key comes back, we compare fingerprints:
 * - same fingerprint      = the same request retried, so replay the first answer;
 * - different fingerprint = the client reused a key for a different request, a client bug, so 422.
 */
public final class RequestFingerprint {

    private RequestFingerprint() {
    }

    public static String of(Object... parts) {
        // e.g. of(42, STRIPE, "tok_success") -> "42|STRIPE|tok_success"
        String canonical = Arrays.stream(parts).map(String::valueOf).collect(Collectors.joining("|"));
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash); // bytes -> "9a60179..." (64 characters)
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is always available in the JDK", e);
        }
    }
}
