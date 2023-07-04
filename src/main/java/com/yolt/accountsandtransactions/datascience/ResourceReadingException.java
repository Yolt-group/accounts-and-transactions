package com.yolt.accountsandtransactions.datascience;

class ResourceReadingException extends RuntimeException {
    public ResourceReadingException(final String message, final Exception exception) {
        super(message, exception);
    }
}
