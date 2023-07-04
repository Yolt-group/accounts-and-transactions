package com.yolt.accountsandtransactions.summary;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * A helper object that contains, for a given usersite:
 * - the timestamp at which the next transaction retrieval should start
 */
@Value
@Builder
public class UserSiteTransactionStatusSummary {
    @NonNull
    UUID userSiteId;

    @NonNull
    @Builder.Default
    Optional<Instant> transactionRetrievalLowerBoundTimestamp = Optional.empty();
}
