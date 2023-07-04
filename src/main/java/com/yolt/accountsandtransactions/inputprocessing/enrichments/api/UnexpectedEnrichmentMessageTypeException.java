package com.yolt.accountsandtransactions.inputprocessing.enrichments.api;

public class UnexpectedEnrichmentMessageTypeException extends RuntimeException {
    private static final String ERROR_MESSAGE_TEMPLATE = "Received unexpected EnrichmentMessageType: %s";

    public UnexpectedEnrichmentMessageTypeException(String value) {
        super(String.format(ERROR_MESSAGE_TEMPLATE, value));
    }
}
