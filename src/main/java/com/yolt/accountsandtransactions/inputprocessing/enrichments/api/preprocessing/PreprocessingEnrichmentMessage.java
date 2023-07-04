package com.yolt.accountsandtransactions.inputprocessing.enrichments.api.preprocessing;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.EnrichmentMessageType;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichmentMessage;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichmentMessageKey;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import org.springframework.lang.Nullable;

import java.time.ZonedDateTime;
import java.util.UUID;

@Data
@EqualsAndHashCode(callSuper = true)
public class PreprocessingEnrichmentMessage extends EnrichmentMessage {

    public PreprocessingEnrichmentMessage(@NonNull EnrichmentMessageType domain,
                                          long version,
                                          @NonNull UUID activityId,
                                          @NonNull ZonedDateTime time,
                                          @NonNull EnrichmentMessageKey messageKey,
                                          @Nullable Integer messageIndex,
                                          @Nullable Integer messageTotal) {
        super(domain, version, activityId, time, messageKey, messageIndex, messageTotal);
    }
}

