package com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NonNull;

import java.util.UUID;

@Data
public class EnrichmentMessageKey {
    @NonNull
    @JsonProperty(value = "userId", required = true)
    private final UUID userId;
    /**
     * @deprecated tracing is handled by sleuth. This requestTraceId should not be read or propagated.
     */
    @Deprecated(forRemoval = true)
    @JsonProperty("requestTraceId")
    private final UUID requestTraceId;

    @Override
    public String toString() {
        return "EnrichmentMessageKey";
    }
}
