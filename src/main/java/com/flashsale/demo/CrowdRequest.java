package com.flashsale.demo;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

/**
 * How many fake buyers to send, and over how many seconds they arrive.
 * {"buyers": 500, "seconds": 5} = 100 new buyers a second for 5 seconds.
 */
public record CrowdRequest(
        @Min(1) @Max(2000) int buyers,
        @Min(0) @Max(60) int seconds) {
}
