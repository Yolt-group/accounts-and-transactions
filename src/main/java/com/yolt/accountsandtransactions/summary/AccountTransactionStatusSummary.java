package com.yolt.accountsandtransactions.summary;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * A helper object that contains, for a given account:
 * - the timestamp at which the oldest pending transaction took place
 * - the timestamp at which the most recent booked transaction took place
 * - the timestamp at which the most recent booked transaction before all pending transactions took place
 */
@Value
@Builder
public class AccountTransactionStatusSummary {
    @NonNull
    UUID accountId;

    @NonNull
    @Builder.Default
    Optional<Instant> oldestPendingTrxTimestamp = Optional.empty();

    @NonNull
    @Builder.Default
    Optional<Instant> mostRecentBookedTrxTimestamp = Optional.empty();

    @NonNull
    @Builder.Default
    Optional<Instant> mostRecentBookedBeforeAllPendingTrxTimestamp = Optional.empty();

    public boolean hasPendingWithoutPrecedingBookedTransaction() {
        return oldestPendingTrxTimestamp.isPresent() && mostRecentBookedBeforeAllPendingTrxTimestamp.isEmpty();
    }
}
