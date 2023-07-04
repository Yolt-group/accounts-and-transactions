package com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.EnrichmentMessageType;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.categories.CategoriesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.counterparties.CounterpartiesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.cycles.CyclesEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.labels.LabelsEnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.preprocessing.PreprocessingEnrichmentMessage;
import lombok.Data;
import lombok.NonNull;

import java.time.ZonedDateTime;
import java.util.UUID;

@Data
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "domain")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PreprocessingEnrichmentMessage.class, name = "PreprocessingEnrichment"),
        @JsonSubTypes.Type(value = CategoriesEnrichmentMessage.class, name = "CategoriesEnrichment"),
        @JsonSubTypes.Type(value = CounterpartiesEnrichmentMessage.class, name = "CounterpartiesEnrichment"),
        @JsonSubTypes.Type(value = CyclesEnrichmentMessage.class, name = "TransactionCyclesEnrichment"),
        @JsonSubTypes.Type(value = LabelsEnrichmentMessage.class, name = "LabelsEnrichment"),
})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class EnrichmentMessage {

    @NonNull
    @JsonProperty(value = "domain", required = true)
    private final EnrichmentMessageType domain;

    @JsonProperty(value = "version", required = true)
    private final long version;

    @NonNull
    @JsonProperty(value = "activityId", required = true)
    private final UUID activityId;

    @NonNull
    @JsonProperty(value = "time", required = true)
    private final ZonedDateTime time;

    @NonNull
    @JsonProperty(value = "key", required = true)
    private final EnrichmentMessageKey messageKey;

    /**
     * TODO Make required once DS provides these fields.
     */
    private final Integer messageIndex;
    /**
     * TODO Make required once DS provides these fields.
     */
    private final Integer messageTotal;

    public boolean isLastPage() {
        //TODO remove the nullcheck once DS provides the field.
        return messageTotal == null || messageTotal.equals(messageIndex + 1);
    }
    @Override
    public String toString() {
        return "EnrichmentMessage";
    }
}

