package com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Period;
import java.util.Currency;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Getter
@Builder(toBuilder = true)
@EqualsAndHashCode
@RequiredArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DsTransactionCycle {
    @NonNull
    private final UUID id;

    @NonNull
    private final BigDecimal amount;

    @NonNull
    private final Currency currency;

    @NonNull
    private final Period period;

    @NonNull
    private final Optional<ModelParameters> modelParameters;

    @NonNull
    private final Set<LocalDate> predictedOccurrences;

    @NonNull
    private final Optional<String> label;

    @JsonProperty("isSubscription")
    private final boolean subscription;

    @NonNull
    private final String counterparty;

    @Override
    public String toString() {
        return "Cycle";
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ModelParameters {
        @NonNull
        private final BigDecimal amount;
        @NonNull
        private final Currency currency;
        @NonNull
        private final Period period;

        @Override
        public String toString() {
            return "ModelParameters";
        }
    }
}
