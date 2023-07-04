package com.yolt.accountsandtransactions.datetime.exception;

public class DateIntervalParseException extends RuntimeException {
    public DateIntervalParseException(final String message) {
        super(message);
    }

    public DateIntervalParseException(final Exception e) {
        super(e.getMessage(), e);
    }
}
