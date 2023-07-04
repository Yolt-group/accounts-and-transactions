package com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NonNull;

import java.util.List;

@Data
public class DsTransactionCycles {
    @NonNull
    @JsonProperty(value = "credits", required = true)
    private final List<DsTransactionCycle> credits;

    @NonNull
    @JsonProperty(value = "debits", required = true)
    private final List<DsTransactionCycle> debits;

    @Override
    public String toString() {
        return "Cycles";
    }
}
