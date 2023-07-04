package com.yolt.accountsandtransactions.transactions.updates.api;

import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.util.Set;

@Value
@RequiredArgsConstructor
@Builder
@Schema
public class SimilarTransactionGroupDTO {
    @Schema(description = "Selector for the transaction group", required = true)
    String groupSelector;

    @Schema(description = "Description for the transaction group", required = true)
    @NonNull
    String groupDescription;

    @Schema(description = "Count of similar transactions in this group", required = true)
    int count;

    @ArraySchema(arraySchema = @Schema(description = "List of the similar transaction-ids grouped by their account-id", required = true))
    Set<TransactionsGroupedByAccountId> transactions;
}
