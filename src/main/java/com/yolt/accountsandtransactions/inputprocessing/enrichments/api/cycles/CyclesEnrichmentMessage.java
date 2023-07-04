package com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.EnrichmentMessageType;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichmentMessageKey;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import org.springframework.lang.Nullable;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import static com.yolt.accountsandtransactions.inputprocessing.enrichments.api.EnrichmentMessageType.TRANSACTION_CYCLES;

@Data
@EqualsAndHashCode(callSuper = true)
public class CyclesEnrichmentMessage extends EnrichmentMessage {
    public static final EnrichmentMessageType MESSAGE_TYPE = TRANSACTION_CYCLES;

    @NonNull
    private final List<CyclesEnrichedTransaction> transactions;

    @NonNull
    private final DsTransactionCycles cycles;

    @Builder
    @JsonCreator
    public CyclesEnrichmentMessage(
            @JsonProperty("version") final long version,
            @NonNull @JsonProperty("activityId") final UUID activityId,
            @NonNull @JsonProperty("time") final ZonedDateTime time,
            @NonNull @JsonProperty("key") final EnrichmentMessageKey key,
            @NonNull @JsonProperty("transactions") final List<CyclesEnrichedTransaction> transactions,
            @NonNull @JsonProperty("transactionCycles") final DsTransactionCycles cycles,
            @Nullable @JsonProperty("messageIndex") final Integer messageIndex,
            @Nullable @JsonProperty("messageTotal") final Integer messageTotal) {
        super(MESSAGE_TYPE, version, activityId, time, key, messageIndex, messageTotal);
        this.transactions = transactions;
        this.cycles = cycles;
    }

    @Override
    public String toString() {
        return "CyclesEnrichmentMessage";
    }
}

