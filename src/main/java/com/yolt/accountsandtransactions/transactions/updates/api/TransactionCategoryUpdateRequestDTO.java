package com.yolt.accountsandtransactions.transactions.updates.api;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;

import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import java.time.LocalDate;
import java.util.UUID;

@Data
@RequiredArgsConstructor
@Schema
public class TransactionCategoryUpdateRequestDTO {
    @Schema(description = "Account ID of the transaction.", required = true)
    @NonNull
    private final UUID accountId;

    @Schema(description = "Unique ID of the transaction within the account.", required = true)
    @NonNull
    @Size(min = 1, max = 100)
    private final String id;

    @NonNull
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    @Schema(description = "Date of the transaction.", required = true)
    private final LocalDate date;

    @Schema(description = "New category of the transaction, for valid values, please see https://developer.yolt.com/docs/transaction-enrichments.")
    @Pattern(regexp = "^[A-Za-z][A-Za-z ]{0,30}[A-Za-z]$")
    private String category;
}
