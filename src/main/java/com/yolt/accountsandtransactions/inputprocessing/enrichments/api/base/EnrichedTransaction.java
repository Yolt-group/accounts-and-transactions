package com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NonNull;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class EnrichedTransaction {
    @NonNull
    @JsonProperty(value = "key", required = true)
    private final EnrichedTransactionKey key;

    @Override
    public String toString() {
        return "EnrichedTransaction";
    }
}
