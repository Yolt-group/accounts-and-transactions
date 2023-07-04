package com.yolt.accountsandtransactions.metrics;

import com.yolt.accountsandtransactions.inputprocessing.DurationLogger;
import com.yolt.accountsandtransactions.inputprocessing.TransactionReconciliationResultMetrics;
import com.yolt.accountsandtransactions.legacyaccounts.AccountMatcher.AccountMatchType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import nl.ing.lovebird.extendeddata.account.BalanceType;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.AccountType;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static com.yolt.accountsandtransactions.legacyaccounts.AccountMatcher.AccountMatchType.IBAN_ACCOUNT_NUMBER_AND_CURRENCY;
import static com.yolt.accountsandtransactions.legacyaccounts.AccountMatcher.AccountMatchType.SORTCODE_ACCOUNT_NUMBER_AND_CURRENCY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Service
@RequiredArgsConstructor
public class AccountsAndTransactionMetrics {

    private final Clock clock;
    private final MeterRegistry meterRegistry;

    public enum Cardinality {
        SINGLE,
        BULK;
    }

    public enum BalanceAccuracy {
        ACCURATE,
        INACCURATE,
        CANT_RECONCILE_TRXS,
        NOT_PRESENT;
    }

    /**
     * Keep a statistic that checks if we can trust the transactions that a bank sends us to add up to the
     * balance assigned to the account of those transactions.
     */
    public void updateBalanceAccuracyStatistics(
            String provider,
            String balanceName,
            BalanceAccuracy result) {
        meterRegistry.counter("acc_trx_balance_accuracy", "provider", provider, "balance", balanceName, "result", result.name()).increment();
    }

    /**
     * Keeps track of which balances we receive from which provider
     */
    public void updateBalanceTypePresenceStatistics(String provider, Set<BalanceType> balanceTypes) {
        final Function<String, Counter> counter = s -> meterRegistry.counter("acc_trx_balance_type_presence", "provider", provider, "balance_type", s);
        balanceTypes.forEach(balance -> counter.apply(balance.name()).increment());
        if (balanceTypes.isEmpty()) {
            // Keep track of the case where we have no BalanceTypes available.
            counter.apply("NONE").increment();
        }
    }

    /**
     * This metric keeps track of the reconciliation performance (i.e.: how well can we match stored transactions with incoming transactions)
     * after we have determined what the set of stored transactions is that we should compare against.
     */
    public void updateTransactionReconciliationMatchingStatistics(TransactionReconciliationResultMetrics metrics) {
        final Function<String, Counter> counter = s -> meterRegistry.counter("acc_trx_reconciliation", "provider", metrics.getProvider(), "type", s);
        // Informational: incoming transaction statistics.
        counter.apply("upstream_total").increment(metrics.getUpstreamTotal());
        counter.apply("upstream_new").increment(metrics.getUpstreamNew());
        counter.apply("upstream_update").increment(metrics.getStoredPrimaryKeyUpdated());
        counter.apply("upstream_unchanged").increment(metrics.getUpstreamUnchanged());
        // Performance monitoring: reconciliation.
        counter.apply("stored_total").increment(metrics.getStoredTotal());
        counter.apply("stored_match_extid").increment(metrics.getStoredMatchedByExternalId());
        counter.apply("stored_match_uniq_attrs").increment(metrics.getStoredMatchedByAttributesUnique());
        counter.apply("stored_match_attrs").increment(metrics.getStoredMatchedByAttributesOptimistic());
        counter.apply("stored_no_match_booked").increment(metrics.getStoredBookedNotMatched());
        counter.apply("stored_no_match_pending").increment(metrics.getStoredPendingNotMatched());
        counter.apply("stored_pending_to_booked").increment(metrics.getStoredPendingToBooked());
        counter.apply("stored_booked_to_pending").increment(metrics.getStoredBookedToPending());
        // Data quality monitoring.
        counter.apply("quality_extid_missing").increment(metrics.getUpstreamQualityMissingExternalIds());
        counter.apply("quality_extid_dupes").increment(metrics.getUpstreamQualityDuplicateExternalIds());
    }

    public void updateTransactionProcessingDurationMetrics(DurationLogger perAccountIngestionDurationLogger) {
        perAccountIngestionDurationLogger.getEntries().forEach(
                e -> meterRegistry.timer("acc_trx_reconciliation_duration", "stage", e.getName())
                        .record(e.getDuration(), MILLISECONDS)
        );
    }

    public void incrementTransactionWithFutureDate(String provider, boolean newTransaction, TransactionStatus transactionStatus) {
        meterRegistry.counter("accounts_transactions_trx_with_future_date",
                        "provider", provider,
                        "new-transaction", Boolean.toString(newTransaction),
                        "status", transactionStatus.toString())
                .increment();
    }

    public void updateClientEnrichment(String clientId, String enrichmentDomain) {
        meterRegistry.counter("acc_trx_client_enrichments", "client-id", clientId, "enrichment-domain", enrichmentDomain).increment();
    }

    public void updateClientEnrichmentDuration(String clientId, String enrichmentDomain, Duration duration) {
        meterRegistry.timer("acc_trx_enrichment_duration", "client-id", clientId, "enrichment-domain", enrichmentDomain).record(duration);
    }

    public void incrementSingleCounterpartyCount() {
        meterRegistry.counter("transactions_merchant_update",
                        "cardinality", Cardinality.SINGLE.name())
                .increment();
    }

    public void incrementBulkCounterpartyCount() {
        meterRegistry.counter("transactions_merchant_update",
                        "cardinality", Cardinality.BULK.name())
                .increment();
    }

    public void incrementSingleCounterpartyFailureCount(final Throwable throwable) {
        meterRegistry.counter("transactions_merchant_update_failure",
                        "cardinality", Cardinality.SINGLE.name(),
                        "exception", throwable.getClass().getSimpleName())
                .increment();
    }

    public void incrementBulkCounterpartyFailureCount(Throwable throwable) {
        meterRegistry.counter("transactions_merchant_update_failure",
                        "cardinality", Cardinality.BULK.name(),
                        "exception", throwable.getClass().getSimpleName())
                .increment();
    }

    public void incrementSingleRecategorizationCount() {
        meterRegistry.counter("transactions_category_update",
                        "cardinality", Cardinality.SINGLE.name())
                .increment();
    }

    public void incrementSingleRecategorizationFailureCount(final Throwable throwable) {
        meterRegistry.counter("transactions_category_update_failure",
                        "cardinality", Cardinality.SINGLE.name(),
                        "exception", throwable.getClass().getSimpleName())
                .increment();
    }

    public void incrementBulkRecategorizationCount() {
        meterRegistry.counter("transactions_category_update",
                        "cardinality", Cardinality.BULK.name())
                .increment();
    }

    public void incrementBulkRecategorizationFailureCount(final Throwable throwable) {
        meterRegistry.counter("transactions_category_update_failure",
                        "cardinality", Cardinality.BULK.name(),
                        "exception", throwable.getClass().getSimpleName())
                .increment();
    }

    public void incrementActivityEnrichmentTimedOutCounter() {
        meterRegistry.counter("accounts_transactions_activity_enrichment_timeout").increment();
    }

    public void measureAccountMatchingStatistics(String provider, AccountMatchType accountMatchType, boolean isAccountNormalized) {
        var accountNormalization = (IBAN_ACCOUNT_NUMBER_AND_CURRENCY == accountMatchType || SORTCODE_ACCOUNT_NUMBER_AND_CURRENCY == accountMatchType) ? String.valueOf(isAccountNormalized) : "not_relevant";
        meterRegistry.counter("accounts_transactions_account_matcher",
                        "provider", Optional.ofNullable(provider).orElse("unknown_provider"),
                        "match_type", accountMatchType.name(),
                        "account_number_normalized", accountNormalization)
                .increment();
    }

    public void incrementPendingTransactionsIngested(@NonNull String provider, long daysInPast) {
        assert daysInPast >= 0;
        meterRegistry.counter("pending_transactions_ingested",
                "provider", provider,
                "days_in_past", daysInPast + ""
        ).increment();
    }

    public void incrementComputeZeroBalanceResult(String provider, BalanceType balanceType, boolean succeeded, String transactionsType) {
        meterRegistry.counter("accounts_transactions_zero_balance_compute_result",
                        "provider", provider,
                        "balance_type", balanceType.getName(),
                        "succeeded", Boolean.toString(succeeded),
                        "transactions_type", transactionsType)
                .increment();
    }

    public void incrementTransactionWithStructuredAndUnstructuredRemittanceInformation(@NonNull String provider) {
        meterRegistry.counter("transaction_with_structured_and_unstructured",
                        "provider", provider)
                .increment();
    }

    public void incrementTransactionWithoutStructuredAndUnstructuredRemittanceInformation(@NonNull String provider) {
        meterRegistry.counter("transaction_without_structured_and_unstructured",
                        "provider", provider)
                .increment();
    }

    /**
     * Measure the oldest pending transaction that we encounter in practice.  Grouped by Site and AccountType.
     *
     * @param siteId                the site
     * @param type                  the AccountType (e.g. current or credit card)
     * @param pendingTransactionAge point in time the pending transaction occurred
     */
    public void measureAgeOfPendingTransaction(@NonNull UUID siteId, @NonNull AccountType type, @NonNull Instant pendingTransactionAge) {
        var days = pendingTransactionAge.until(Instant.now(clock), ChronoUnit.DAYS);
        DistributionSummary.builder("pending_trx_age")
                .tags("site_account_type", siteId.toString() + "_" + type.name())
                .minimumExpectedValue(1d)
                .maximumExpectedValue(90d) // upperbound = 90 days
                .publishPercentileHistogram()
                .register(meterRegistry)
                .record(days);
    }

    /**
     * In theory the sets of accounts in the A&T keyspace should be a strict subset of the accounts stored in the
     * accounts service.  We are finding out that this is not the case in practice.  This metric keeps track of
     * any inconsistencies between the sets of accounts in A&T vs. the accounts service that  we encounter during
     * normal operation of this service to get a feel for what is wrong (what scale, which accounts, etc.)
     */
    public void measureAccountDataConsistency(@NonNull String provider, @NonNull AccountDataConsistency result, int count) {
        meterRegistry.counter("internal_account_data_consistency",
                "provider", provider,
                "result", result.name()
        ).increment();
    }

    /**
     * This measurement was added because we have an increasing number of clients requesting this information
     * to be present in the transaction information, which makes sense. A transaction should always have a
     * debtor and a creditor. Either one of these fields should always be equal to the account holder.
     * The output is added to the log report so we can determine which providers need to be improved.
     */
    public void incrementDebtorOrCreditorNameIsPresent(@NonNull String provider,
                                                       boolean isIncoming,
                                                       boolean creditorNamePresent,
                                                       boolean debtorNamePresent) {
        meterRegistry.counter("trx_debtor_creditor_field_presence",
                        "provider", provider,
                        "incoming_trx", Boolean.toString(isIncoming),
                        "debtor_name", Boolean.toString(debtorNamePresent),
                        "creditor_name", Boolean.toString(creditorNamePresent))
                .increment();
    }

    enum ReconciliationStatus {
        FAILURE,
        SUCCESS
    }

    public void incrementReconciliationFailure(final @NonNull String provider) {
        meterRegistry.counter("matcher.attr.reconciliation.result",
                "provider", provider,
                "result", ReconciliationStatus.FAILURE.name()
        ).increment();
    }

    public enum AccountDataConsistency {
        /**
         * Account data is consistent between accounts and accounts-and-transactions.
         */
        CONSISTENT,
        /**
         * One or more accounts are missing from accounts that we have in our own database.
         */
        ACCOUNTS_MISSING,
        /**
         * One or more accounts are invalid in accounts.
         */
        ACCOUNTS_INVALID,
        /**
         * One or more accounts are missing from our own database.
         * <p>
         * This is not unexpected on app-prd and yfb-prd.  On yfb-ext-prd this would be unexpected.
         */
        AT_MISSING,
        /**
         * One or more accounts are invalid in our own database.
         */
        AT_INVALID,
    }

}
