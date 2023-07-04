package com.yolt.accountsandtransactions.transactions.updates.api;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

import javax.validation.constraints.NotNull;
import java.time.LocalDate;
import java.util.UUID;

@Data
@Builder
@ToString(onlyExplicitlyIncluded = true)
@AllArgsConstructor
@NoArgsConstructor
@Schema
public class SeedTransactionKey {
    @ToString.Include
    @NotNull
    @Schema(description = "Account ID of the transaction", required = true)
    private UUID accountId;

    @ToString.Include
    @NotNull
    @Schema(description = "Unique ID of the transaction within the account", required = true)
    private String id;

    @ToString.Include
    @NotNull
    @Schema(description = "Date of the transaction", required = true)
    private LocalDate date;
}