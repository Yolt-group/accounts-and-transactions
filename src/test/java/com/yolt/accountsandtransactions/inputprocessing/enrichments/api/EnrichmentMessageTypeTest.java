package com.yolt.accountsandtransactions.inputprocessing.enrichments.api;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.yolt.accountsandtransactions.inputprocessing.enrichments.api.EnrichmentMessageType.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.of;


public class EnrichmentMessageTypeTest {
    @ParameterizedTest
    @MethodSource("provideEnrichmentMessageTypes")
    public void testOrdinalAndChecksum(EnrichmentMessageType enrichmentMessageType, int ordinalValue, int checksumValue) {
        assertThat(enrichmentMessageType.ordinal()).isEqualTo(ordinalValue);
        assertThat(enrichmentMessageType.checksumValue()).isEqualTo(checksumValue);
    }

    private static Stream<Arguments> provideEnrichmentMessageTypes() {
        return Stream.of(
                of(CATEGORIES, 0, 1),
                of(COUNTER_PARTIES, 1, 10),
                of(TRANSACTION_CYCLES, 2, 100),
                of(LABELS, 3, 1000));
    }
}