package io.debezium.performance.testsuite.exception;

public class DmtException extends RuntimeException {
    public DmtException(String message) {
        super(message);
    }

    public DmtException(String message, Throwable cause) {
        super(message, cause);
    }
}
