package com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichedTransaction;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichedTransactionKey;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.util.UUID;

@Data
public class CyclesEnrichedTransaction extends EnrichedTransaction {
    @NonNull
    private final UUID cycleId;

    @Builder
    @JsonCreator
    public CyclesEnrichedTransaction(
            @NonNull @JsonProperty("key") final EnrichedTransactionKey key,
            @NonNull @JsonProperty("cycleId") final UUID cycleId) {
        super(key);
        this.cycleId = cycleId;
    }

    @Override
    public String toString() {
        return "CyclesEnrichedTransaction";
    }
}
