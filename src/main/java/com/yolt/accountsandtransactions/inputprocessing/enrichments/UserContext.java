package com.yolt.accountsandtransactions.inputprocessing.enrichments;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.UUID;

/**
 * This is a 'fake' user context.
 * We don't rely on a real user-context anymore. However there are 2 cases in which we still need it:
 * 1) the DS pipeline still relies on it. It only needs the fields userId, preferredCurrency, countryCode, enabledFeatures (always an empty list)
 * 2) We get an {@link nl.ing.lovebird.activityevents.events.TransactionsEnrichmentFinishedEvent} from A&T. Unfortunately, in case of a timeout,
 * we don't have a clientUserToken because A&T is checking for timeouts in a batch. In the future, A&T and SM should be merged to 'ais'. From that
 * moment we don't need it because we can request a clientUserToken.
 */
@Data
@AllArgsConstructor
@Builder(toBuilder = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserContext {
    public static final String USER_CONTEXT_HEADER_KEY = "X-user-context";
    private static final ObjectMapper USER_CONTEXT_OBJECT_MAPPER = new ObjectMapper();

    UUID clientId;
    UUID userId;

    public String toJson() {
        try {
            return USER_CONTEXT_OBJECT_MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException var2) {
            throw new IllegalArgumentException("Could not serialize user-context", var2);
        }
    }
}
