package com.yolt.accountsandtransactions.transactions;


public class InvalidPagingStateException extends RuntimeException {
    public InvalidPagingStateException(final String message) {
        super(message);
    }
}
