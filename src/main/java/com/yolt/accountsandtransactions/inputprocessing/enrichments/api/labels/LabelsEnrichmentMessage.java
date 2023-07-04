package com.yolt.accountsandtransactions.inputprocessing.enrichments.api.labels;

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

import static com.yolt.accountsandtransactions.inputprocessing.enrichments.api.EnrichmentMessageType.LABELS;

@Data
@EqualsAndHashCode(callSuper = true)
public class LabelsEnrichmentMessage extends EnrichmentMessage {
    public static final EnrichmentMessageType MESSAGE_TYPE = LABELS;

    @NonNull
    private final List<LabelsEnrichedTransaction> transactions;

    @Builder
    @JsonCreator
    public LabelsEnrichmentMessage(
            @NonNull @JsonProperty("version") final long version,
            @NonNull @JsonProperty("activityId") final UUID activityId,
            @NonNull @JsonProperty("time") final ZonedDateTime time,
            @NonNull @JsonProperty("key") final EnrichmentMessageKey key,
            @NonNull @JsonProperty("transactions") final List<LabelsEnrichedTransaction> transactions,
            @Nullable @JsonProperty("messageIndex") final Integer messageIndex,
            @Nullable @JsonProperty("messageTotal") final Integer messageTotal) {
        super(MESSAGE_TYPE, version, activityId, time, key, messageIndex, messageTotal);
        this.transactions = transactions;
    }

    @Override
    public String toString() {
        return "LabelsEnrichmentMessage";
    }
}
