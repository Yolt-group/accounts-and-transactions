package com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NonNull;

import java.time.LocalDate;
import java.util.UUID;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class EnrichedTransactionKey {
    @NonNull
    @JsonProperty(value = "accountId", required = true)
    private final UUID accountId;

    @NonNull
    @JsonProperty(value = "userId", required = true)
    private final UUID userId;

    @NonNull
    @JsonProperty(value = "transactionId", required = true)
    private final String transactionId;

    @NonNull
    @JsonProperty(value = "date", required = true)
    private final LocalDate date;

    @Override
    public String toString() {
        return "EnrichedTransactionKey";
    }
}