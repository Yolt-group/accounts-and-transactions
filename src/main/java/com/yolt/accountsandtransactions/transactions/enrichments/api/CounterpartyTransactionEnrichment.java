package com.yolt.accountsandtransactions.transactions.enrichments.api;

import lombok.Getter;
import lombok.NonNull;

import java.time.LocalDate;
import java.util.UUID;

@Getter
public class CounterpartyTransactionEnrichment extends TransactionEnrichment {
    private final String counterparty;
    private final boolean isKnownMerchant;

    public CounterpartyTransactionEnrichment(@NonNull UUID userId, @NonNull UUID accountId, @NonNull LocalDate date, @NonNull String transactionId, @NonNull String counterparty, boolean isKnownMerchant) {
        super(userId, accountId, date, transactionId);
        this.counterparty = counterparty;
        this.isKnownMerchant = isKnownMerchant;
    }
}
