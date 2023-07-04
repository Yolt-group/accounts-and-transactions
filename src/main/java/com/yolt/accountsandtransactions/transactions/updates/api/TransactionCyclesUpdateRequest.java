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
public class TransactionCyclesUpdateRequest {

    @NonNull
    @NotNull
    @NotZero
    @Schema(description = "The recurring monetary amount. The amount cannot be zero.", required = true, example = "10.01")
    private final BigDecimal amount;

    @NonNull
    @NotNull
    @Schema(description = "The ISO-8601 period of the recurrence.", required = true, example = "P7D")
    private final String period;

    @NonNull
    @NotNull
    @Schema(description = "A type hint for the cycle.", required = true, example = "Tesco")
    private final Optional<String> label;
}
