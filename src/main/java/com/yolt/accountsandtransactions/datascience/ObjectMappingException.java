package com.yolt.accountsandtransactions.datascience;

class ObjectMappingException extends RuntimeException {

    public ObjectMappingException(final String message, final Exception exception) {
        super(message, exception);
    }
}
