package com.yolt.accountsandtransactions.transactions.enrichments.api;

import lombok.Getter;
import lombok.NonNull;

import java.time.LocalDate;
import java.util.Set;
import java.util.UUID;

@Getter
public class LabelsTransactionEnrichment extends TransactionEnrichment {
    private Set<String> labels;

    public LabelsTransactionEnrichment(@NonNull UUID userId, @NonNull UUID accountId, @NonNull LocalDate date, @NonNull String transactionId, @NonNull Set<String> labels) {
        super(userId, accountId, date, transactionId);
        this.labels = labels;
    }
}
