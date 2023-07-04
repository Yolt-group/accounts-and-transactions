package com.yolt.accountsandtransactions.inputprocessing.enrichments.activities;

import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.EnrichmentMessageType;

import java.util.List;

import static com.yolt.accountsandtransactions.inputprocessing.enrichments.api.EnrichmentMessageType.*;
import static java.util.Collections.emptyList;

/**
 * The ActivityEnrichmentType is used when starting an ActivityEnrichment. It indicates the type of enrichment. Each type
 * contains a list of **additional** enrichment messages that are expected for the ActivityEnrichmentType. These
 * **additional** enrichment messages are expected on-top-of the ones that are expected regularly (and allows for more
 * than one enrichment message of a single type).
 */
public enum ActivityEnrichmentType {
    REFRESH(emptyList()),
    FEEDBACK_CATEGORIES(List.of(CATEGORIES)),
    FEEDBACK_COUNTERPARTIES(List.of(COUNTER_PARTIES)),
    FEEDBACK_TRANSACTION_CYCLES(List.of(TRANSACTION_CYCLES));

    public final List<EnrichmentMessageType> additionalEnrichments;

    ActivityEnrichmentType(List<EnrichmentMessageType> additionalEnrichments) {
        this.additionalEnrichments = additionalEnrichments;
    }
}
