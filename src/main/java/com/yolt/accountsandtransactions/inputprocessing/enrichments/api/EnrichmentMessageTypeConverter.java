package com.yolt.accountsandtransactions.inputprocessing.enrichments.api;

import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import static java.util.Arrays.stream;

@Component
public class EnrichmentMessageTypeConverter implements Converter<String, EnrichmentMessageType> {
    @Override
    public EnrichmentMessageType convert(String value) {
        return stream(EnrichmentMessageType.values())
                .filter(messageType -> messageType.value.equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(() -> new UnexpectedEnrichmentMessageTypeException(value));
    }
}
