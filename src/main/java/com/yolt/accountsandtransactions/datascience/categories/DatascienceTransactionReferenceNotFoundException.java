package com.yolt.accountsandtransactions.datascience.categories;

import lombok.Getter;

import java.util.UUID;

import static java.lang.String.format;

public final class DatascienceTransactionReferenceNotFoundException extends RuntimeException {

    @Getter
    private final transient UUID accountId;
    @Getter
    private final transient String transactionId;

    public DatascienceTransactionReferenceNotFoundException(String message, UUID accountId, String transactionId) {
        super(message);
        this.accountId = accountId;
        this.transactionId = transactionId;
    }

    @Override
    public String getMessage() {
        return format("Reference transaction %s (for account %s) not found. Message %s", transactionId, accountId, super.getMessage());
    }
}
