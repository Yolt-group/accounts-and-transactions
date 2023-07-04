package com.yolt.accountsandtransactions.inputprocessing.enrichments.api.labels;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichedTransaction;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichedTransactionKey;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.util.Set;

@Data
public class LabelsEnrichedTransaction extends EnrichedTransaction {
    @NonNull
    private final Set<String> labels;

    @Builder
    @JsonCreator
    public LabelsEnrichedTransaction(
            @NonNull @JsonProperty("key") final EnrichedTransactionKey key,
            @NonNull @JsonProperty("labels") final Set<String> labels) {
        super(key);
        this.labels = labels;
    }

    @Override
    public String toString() {
        return "LabelsEnrichedTransaction";
    }

}
