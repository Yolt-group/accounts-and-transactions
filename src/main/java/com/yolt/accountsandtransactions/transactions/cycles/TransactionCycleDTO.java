package com.yolt.accountsandtransactions.transactions.cycles;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yolt.accountsandtransactions.transactions.TransactionDTO.DeprecatedTransactionCycleDTO;
import com.yolt.accountsandtransactions.transactions.updates.TransactionCyclesFeedbackController.TransactionCyclesFeedbackResponse.CreatedOrUpdatedTransactionCycleDTO;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

@Getter
@Builder
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Schema(description = "The cycle to which transaction belongs. This refers to a recurring transaction.",
        subTypes = {CreatedOrUpdatedTransactionCycleDTO.class, DeprecatedTransactionCycleDTO.class})
public class TransactionCycleDTO {

    @NonNull
    @JsonProperty(required = true)
    @Schema(description = "The identifier of the cycle.", required = true, example = "ecd4c298-b918-46d4-afe2-7df5aac410dd")
    private final UUID cycleId;

    @NonNull
    @JsonProperty(required = true)
    @Schema(description = "The type of cycle (i.e. debit or credit)", required = true, example = "CREDITS")
    private final CycleType cycleType;

    @NonNull
    @JsonProperty(required = true)
    @Schema(description = "The amount of the cycle.", required = true, example = "-22.33")
    private final BigDecimal amount;

    @NonNull
    @JsonProperty(required = true)
    @Schema(description = "The currency of the cycle.", required = true, example = "EUR")
    private final String currency;

    @NonNull
    @JsonProperty(required = true)
    @Schema(description = "The ISO-8601 period of the recurrence.", required = true, example = "P7D")
    private final String period;

    @NonNull
    @Schema(description = "The cycle properties as detected by our model.")
    private final Optional<ModelParameters> detected;

    /**
     * The type here is a {@link String} because a {@link LocalDate} breaks springfox' swagger generation and we couldn't
     * find another way to solve this problem.
     */
    @NonNull
    @JsonProperty(required = true)
    @ArraySchema(arraySchema = @Schema(description = "Set of local dates on which the transaction in this cycle might occur.", required = true, example = "[\"2030-12-31\", \"...\"]"))
    private final Set<String> predictedOccurrences;

    @NonNull
    @Schema(description = "A label describing the cycle.", required = true, example = "salary")
    private final Optional<String> label;

    @JsonProperty(required = true)
    @Schema(description = "Indication of whether or not this cycle is a subscription.", required = true, example = "true")
    private final boolean subscription;

    @NonNull
    @Schema(description = "The counterpart of transaction that is part of the cycle.", required = true, example = "Tesco")
    @JsonProperty(required = true)
    private final String counterparty;

    @JsonProperty(required = true)
    @Schema(description = "A flag to indicate that this cycle existed at some point in time, but is no longer detected at this moment. This cycle may be re-activated in the future.", required = true, example = "true")
    private final boolean expired;

    @Builder
    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    @Schema
    public static class ModelParameters {

        @NonNull
        @JsonProperty(required = true)
        @Schema(description = "The detected amount of the cycle.", required = true, example = "-22.33")
        private final BigDecimal amount;

        @NonNull
        @JsonProperty(required = true)
        @Schema(description = "The detected currency of the cycle.", required = true, example = "EUR")
        private final String currency;

        @NonNull
        @JsonProperty(required = true)
        @Schema(description = "The detected ISO-8601 period of the recurrence.", required = true, example = "P7D")
        private final String period;
    }

    public static TransactionCycleDTO fromTransactionCycle(TransactionCycle transactionCycle) {
        return builder()
                .cycleId(transactionCycle.getCycleId())
                .amount(transactionCycle.getAmount())
                .counterparty(transactionCycle.getCounterparty())
                .currency(transactionCycle.getCurrency())
                .cycleType(transactionCycle.getCycleType())
                .label(Optional.ofNullable(transactionCycle.getLabel()))
                .period(transactionCycle.getPeriod())
                .detected(getModelParameters(transactionCycle))
                .predictedOccurrences(
                        Optional.ofNullable(transactionCycle.getPredictedOccurrences()).orElse(emptySet()).stream()
                                .map(LocalDate::toString)
                                .collect(toSet()))
                .subscription(transactionCycle.isSubscription())
                .expired(transactionCycle.isExpired())
                .build();
    }

    private static Optional<ModelParameters> getModelParameters(TransactionCycle transactionCycle) {
        return Optional.ofNullable(transactionCycle.getModelAmount())
                .flatMap(amount -> Optional.ofNullable(transactionCycle.getModelCurrency())
                        .flatMap(currency -> Optional.ofNullable((transactionCycle.getModelPeriod()))
                                .map(period -> ModelParameters.builder()
                                        .amount(amount)
                                        .currency(currency)
                                        .period(period)
                                        .build())));
    }
}
