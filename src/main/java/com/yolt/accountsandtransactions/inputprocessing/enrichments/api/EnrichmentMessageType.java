package com.yolt.accountsandtransactions.inputprocessing.enrichments.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;

/*
 * This enumeration lists the available types of enrichment messages that data-science can send (in their namespace known as "domain").
 * An enrichment message contains an attribute that indicates its type which should be one of the valid values in this enumeration.
 *
 * The ordinal of each enum value is used to calculate a checksum that is e.g. used to determine if an ActivityEnrichment has
 * finished. The ordering of the enum values should remain as is (appending new values is allowed). Reordering can be done if needed.
 * In that case be aware that **non-finished** ActivityEnrichments will be affected.
 *
 * The checksum for each enum value is calculated based on the ordinal value of the enum value. This allows for overall sum of a number
 * of checksums to indicate which (and how many, max 9) values are present in the overall sum.
 * E.g. an overall sum of 201 indicates, 1 CATEGORIES, 0 COUNTER_PARTIES, 2 TRANSACTION_CYCLES and 0 LABELS.
 *
 * NOTE: The ordinal number of each enum value is used to calculate its checksum. New enum values should be appended to
 * the end of the enumeration.
 */
public enum EnrichmentMessageType {
    CATEGORIES("CategoriesEnrichment"),
    COUNTER_PARTIES("CounterpartiesEnrichment"),
    TRANSACTION_CYCLES("TransactionCyclesEnrichment"),
    LABELS("LabelsEnrichment"),
    PREPROCESSING("PreprocessingEnrichment");

    /**
     * Value used to represent this enum in JSON.
     */
    public final String value;

    EnrichmentMessageType(String value) {
        this.value = value;
    }

    /**
     * Convert from JSON.
     */
    @JsonCreator
    public EnrichmentMessageType fromJson(String value) {
        return Arrays.stream(values())
                .filter(v -> v.value.equals(value))
                .findFirst()
                .orElseThrow(() -> new UnexpectedEnrichmentMessageTypeException(value));
    }

    /**
     * Convert to JSON.
     */
    @JsonValue
    public String toJson() {
        return value;
    }

    @JsonIgnore
    public long checksumValue() {
        return (long) Math.pow(10, ordinal() + 1) / 10;
    }
}
