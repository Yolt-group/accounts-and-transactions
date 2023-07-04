package com.yolt.accountsandtransactions.transactions.enrichments.api;

import lombok.Getter;
import lombok.NonNull;

import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

@Getter
public class CategoryTransactionEnrichment extends TransactionEnrichment {

    @NonNull
    private final String category;

    @NonNull
    private final Optional<String> categorySME;

    @NonNull
    private final Optional<String> categoryPersonal;

    public CategoryTransactionEnrichment(@NonNull UUID userId, @NonNull UUID accountId, @NonNull LocalDate date, @NonNull String transactionId, @NonNull String category, @NonNull Optional<String> categorySME, @NonNull Optional<String> categoryPersonal) {
        super(userId, accountId, date, transactionId);
        this.category = category;
        this.categorySME = categorySME;
        this.categoryPersonal = categoryPersonal;
    }
}
