package com.yolt.accountsandtransactions.datascience.categories.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.time.LocalDate;
import java.util.UUID;

@Getter
@RequiredArgsConstructor
@Builder(toBuilder = true)
@Schema
public class DsCategoriesTransactionKeyDTO {

    @NonNull
    @Schema(description = "An unique identifier for an user", required = true)
    private final UUID userId;

    @NonNull
    @Schema(description = "An unique identifier for an account", required = true)
    private final UUID accountId;

    @NonNull
    @Schema(description = "An identifier for a transaction", required = true)
    private final String transactionId;

    @NonNull
    @Schema(description = "The transaction type (REGULAR or PENDING)", required = true)
    private final String transactionType;

    @NonNull
    @Schema(description = "The date of the transaction", required = true)
    private final LocalDate date;
}
