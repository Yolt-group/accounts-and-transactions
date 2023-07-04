package com.yolt.accountsandtransactions.transactions.updates.api;

import com.yolt.accountsandtransactions.validation.NotZero;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.util.Optional;


@Getter
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
@Schema
public class TransactionCyclesCreateRequest {

    @NonNull
    @NotNull
    @Schema(description = "The seed transaction identifying this transaction-cycle.", required = true)
    private final SeedTransactionKey transactionKey;

    @NonNull
    @NotNull
    @NotZero
    @Schema(description = "The recurring monetary amount.", required = true, example = "10.01")
    private final BigDecimal amount;

    @NonNull
    @NotNull
    @Schema(description = "The ISO-8601 period of the recurrence.", required = true, example = "P7D")
    private final String period;

    @NonNull
    @NotNull
    @Schema(description = "A type hint for the cycle.", required = true, example = "salary")
    private final Optional<String> label;
}
