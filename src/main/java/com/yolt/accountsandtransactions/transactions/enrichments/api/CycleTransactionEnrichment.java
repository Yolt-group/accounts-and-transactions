package com.yolt.accountsandtransactions.transactions.enrichments.api;

import lombok.Getter;
import lombok.NonNull;

import java.time.LocalDate;
import java.util.UUID;

@Getter
public class CycleTransactionEnrichment extends TransactionEnrichment {
    private UUID cycleId;

    public CycleTransactionEnrichment(@NonNull UUID userId, @NonNull UUID accountId, @NonNull LocalDate date, @NonNull String transactionId, @NonNull UUID cycleId) {
        super(userId, accountId, date, transactionId);
        this.cycleId = cycleId;
    }
}
