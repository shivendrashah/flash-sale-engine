package com.flashsale.common.error;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.TypeMismatchException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.validation.FieldError;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/**
 * Turns exceptions into JSON error responses, all in the same standard shape ("Problem Details", RFC 9457):
 *   {"title": "Conflict", "status": 409, "detail": "Sale item 1 does not have 1 unit(s) left",
 *    "instance": "/api/holds", "code": "SOLD_OUT"}
 *
 *       a missing header, a failed @Valid check, an unknown enum value... handleExceptionInternal() below adds our
 *       "code" field to those as well, so EVERY error a client sees has one:
 *         our business errors     the exception's own code: SOLD_OUT, RATE_LIMITED, PURCHASE_LIMIT_EXCEEDED...
 *         Spring's own 4xx errors MISSING_HEADER, UNREADABLE_BODY, INVALID_VALUE, METHOD_NOT_ALLOWED, NOT_FOUND...
 *         a bug (anything else)   500 INTERNAL_ERROR, with the details only in the log
 */
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    ResponseEntity<ProblemDetail> handleApiException(ApiException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(ex.status(), ex.getMessage());
        problem.setProperty("code", ex.code()); // extra field clients can switch on, e.g. "SOLD_OUT"
        ResponseEntity.BodyBuilder response = ResponseEntity.status(ex.status());
        if (ex.retryAfterSeconds() != null) {
            response.header(HttpHeaders.RETRY_AFTER, String.valueOf(ex.retryAfterSeconds()));
        }
        return response.body(problem);
    }

    @ExceptionHandler(ObjectOptimisticLockingFailureException.class)
    ResponseEntity<ProblemDetail> handleConcurrentUpdate(ObjectOptimisticLockingFailureException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT,
                "The resource was changed by another request at the same time. Please retry.");
        problem.setProperty("code", "CONCURRENT_UPDATE");
        return ResponseEntity.status(HttpStatus.CONFLICT).body(problem);
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException ex,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        List<String> errors = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .toList();
        return validationProblem(status, errors);
    }

    @Override
    protected ResponseEntity<Object> handleHandlerMethodValidationException(HandlerMethodValidationException ex,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        List<String> errors = ex.getParameterValidationResults().stream()
                .flatMap(result -> result.getResolvableErrors().stream()
                        .map(error -> (error instanceof FieldError fieldError
                                ? fieldError.getField()
                                : result.getMethodParameter().getParameterName())
                                + ": " + error.getDefaultMessage()))
                .toList();
        return validationProblem(status, errors);
    }

    @Override
    protected ResponseEntity<Object> handleExceptionInternal(Exception ex, Object body, HttpHeaders headers,
            HttpStatusCode statusCode, WebRequest request) {
        ResponseEntity<Object> response = super.handleExceptionInternal(ex, body, headers, statusCode, request);
        if (response != null && response.getBody() instanceof ProblemDetail problem
                && (problem.getProperties() == null || !problem.getProperties().containsKey("code"))) {
            problem.setProperty("code", codeFor(ex, statusCode));
        }
        return response;
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ProblemDetail> handleUnexpected(Exception ex) {
        log.error("Unexpected error", ex);
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR,
                "Something went wrong on our side. Please retry later.");
        problem.setProperty("code", "INTERNAL_ERROR");
        return ResponseEntity.internalServerError().body(problem);
    }

    /** The code for an error Spring raised itself. Unknown ones get the status's name, e.g. NOT_ACCEPTABLE. */
    private static String codeFor(Exception ex, HttpStatusCode statusCode) {
        return switch (ex) {
            case MissingRequestHeaderException missing -> "MISSING_HEADER";         // no X-User-Id, no Idempotency-Key
            case MissingServletRequestParameterException missing -> "MISSING_PARAMETER";
            case HttpMessageNotReadableException unreadable -> "UNREADABLE_BODY";   // broken JSON, wrong JSON type
            case TypeMismatchException mismatch -> "INVALID_VALUE";                 // X-Concurrency-Strategy: FASTEST
            case HttpRequestMethodNotSupportedException method -> "METHOD_NOT_ALLOWED";
            case HttpMediaTypeNotSupportedException mediaType -> "UNSUPPORTED_MEDIA_TYPE";
            case NoResourceFoundException notFound -> "NOT_FOUND";                  // same code as NotFoundException
            default -> {
                HttpStatus status = HttpStatus.resolve(statusCode.value());
                yield status != null ? status.name() : "ERROR";
            }
        };
    }

    private static ResponseEntity<Object> validationProblem(HttpStatusCode status, List<String> errors) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, "The request is not valid");
        problem.setProperty("code", "VALIDATION_FAILED");
        problem.setProperty("errors", errors);
        return ResponseEntity.status(status).body(problem);
    }
}
