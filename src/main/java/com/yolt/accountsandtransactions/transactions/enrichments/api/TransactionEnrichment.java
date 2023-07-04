package com.yolt.accountsandtransactions.transactions.enrichments.api;

import lombok.Getter;
import lombok.NonNull;

import java.time.LocalDate;
import java.util.UUID;

@Getter
public abstract class TransactionEnrichment {
    protected UUID userId;
    protected UUID accountId;
    protected LocalDate date;
    protected String transactionId;

    public TransactionEnrichment(@NonNull UUID userId, @NonNull UUID accountId, @NonNull LocalDate date, @NonNull String transactionId) {
        this.userId = userId;
        this.accountId = accountId;
        this.date = date;
        this.transactionId = transactionId;
    }
}
