package com.yolt.accountsandtransactions;

public class ValidationException extends RuntimeException {
    public ValidationException(final String message) {
        super(message);
    }
}
