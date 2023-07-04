package com.yolt.accountsandtransactions.transactions.updates.api;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.time.LocalDate;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Schema
public class TransactionCounterpartyUpdateRequestDTO {
    @Schema(description = "Account ID of the transaction", required = true)
    @NotNull
    private UUID accountId;

    @Schema(description = "Unique ID of the transaction within the account", required = true)
    @NotNull
    @Size(min = 1, max = 100)
    private String id;

    @NotNull
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    @Schema(description = "Date of the transaction", required = true)
    private LocalDate date;

    @NotNull
    @Size(max = 200)
    @Schema(description = "New counterparty name (use empty string for no counterparty)")
    private String counterpartyName;
}