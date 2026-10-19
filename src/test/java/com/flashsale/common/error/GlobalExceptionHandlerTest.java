package com.flashsale.common.error;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;

/**
 * A bug (an exception nobody planned for) becomes 500 INTERNAL_ERROR in the usual JSON shape, and the
 * exception's own message never reaches the client.
 */
class GlobalExceptionHandlerTest {

    @Test
    void aBugIsA500WithACodeAndNoInternals() {
        ResponseEntity<ProblemDetail> response = new GlobalExceptionHandler()
                .handleUnexpected(new IllegalStateException("column \"secret_token\" does not exist"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody().getProperties()).containsEntry("code", "INTERNAL_ERROR");
        assertThat(response.getBody().getDetail()).doesNotContain("secret_token");
    }
}
