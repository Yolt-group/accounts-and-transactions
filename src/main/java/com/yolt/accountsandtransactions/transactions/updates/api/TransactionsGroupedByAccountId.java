package com.yolt.accountsandtransactions.transactions.updates.api;

import lombok.Builder;
import lombok.Value;

import javax.validation.constraints.NotNull;
import java.util.Set;
import java.util.UUID;

@Value
@Builder
public class TransactionsGroupedByAccountId {
    @NotNull
    UUID accountId;
    @NotNull
    Set<String> transactionIds;
}