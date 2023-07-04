package com.yolt.accountsandtransactions.summary;

import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountRepository;
import com.yolt.accountsandtransactions.metrics.AccountsAndTransactionMetrics;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@Slf4j
public class SummaryService {
    /**
     * Horizon beyond which we don't look at transactions.
     */
    private static final int USER_SITE_TRANSACTION_STATUS_SUMMARY_HORIZON_DAYS = 90;

    private static final UUID SITE_ID_MONZO = UUID.fromString("82c16668-4d59-4be8-be91-1d52792f48e3");

    private final Clock clock;
    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;
    private final AccountsAndTransactionMetrics metrics;


    /**
     * Compute a summary that contains, for every UserSite (for which we have transaction information in the db):
     * - the timestamp at which the oldest pending transaction happened
     * - the timestamp at which the most recent booked transaction happened
     * - the timestamp at which the most recent booked transaction before all pending transactions took place
     * - the timestamp that should be used as the lower bound for retrieving transactions
     */
    public List<UserSiteTransactionStatusSummary> getUserSiteTransactionStatusSummary(@NonNull UUID userId) {
        // Limit the list of transactions we retrieve (for performance reasons).
        final LocalDate horizon = LocalDate.now(clock).minusDays(USER_SITE_TRANSACTION_STATUS_SUMMARY_HORIZON_DAYS);

        // List the accounts for the user first, we require this to access the transactions table efficiently.
        var accounts = accountRepository.getAccounts(userId);

        return accounts.stream().collect(Collectors.groupingBy(Account::getUserSiteId))
                .entrySet().stream()
                .map(entry -> {
                    var summaries = entry.getValue().stream()
                            .map(account -> getAccountTransactionStatusSummary(account, horizon))
                            .collect(Collectors.toList());

                    var oldestPendingTrxTimestamp = getOldestPendingTrxTimestamp(summaries);
                    var mostRecentBookedTrxTimestamp = getMostRecentBookedTrxTimestamp(summaries);
                    var mostRecentBookedBeforeAllPendingTrxTimestamp = getMostRecentBookedBeforeAllPendingTrxTimestamp(summaries);

                    // Hacky: there will always be at least one entry, given the `groupingBy`. Each of the accounts in this
                    // grouping will share the same site id.
                    var siteId = entry.getValue().get(0).getSiteId();
                    var transactionRetrievalLowerBoundTimestamp = getTransactionRetrievalLowerBoundTimestamp(siteId,
                            oldestPendingTrxTimestamp, mostRecentBookedTrxTimestamp, mostRecentBookedBeforeAllPendingTrxTimestamp);

                    return UserSiteTransactionStatusSummary.builder()
                            .userSiteId(entry.getKey())
                            .transactionRetrievalLowerBoundTimestamp(transactionRetrievalLowerBoundTimestamp)
                            .build();
                })
                .collect(Collectors.toList());
    }

    private AccountTransactionStatusSummary getAccountTransactionStatusSummary(Account account, LocalDate horizon) {
        var trxs = transactionRepository.getStatusAndTimestampForTrxsOnOrAfter(account.getUserId(), account.getId(), horizon);

        // Find the oldest pending transaction.
        var oldestPendingTrxTime = trxs.stream()
                .filter(t -> t.getLeft() == TransactionStatus.PENDING)
                .map(Pair::getRight)
                .min(Instant::compareTo);
        oldestPendingTrxTime.ifPresent(time -> metrics.measureAgeOfPendingTransaction(account.getSiteId(), account.getType(), time));

        // Find the most recent booked transactions.
        var mostRecentBookedTrxTime = trxs.stream()
                .filter(t -> t.getLeft() == TransactionStatus.BOOKED)
                .map(Pair::getRight)
                .max(Instant::compareTo);

        // Find the transaction before the oldest pending transaction
        var mostRecentBookedBeforeAllPendingTrxTime = oldestPendingTrxTime.flatMap(upperBound -> trxs.stream()
                .filter(t -> t.getLeft() == TransactionStatus.BOOKED)
                .filter(t -> !t.getRight().isAfter(upperBound))
                .map(Pair::getRight)
                .max(Instant::compareTo));

        return AccountTransactionStatusSummary.builder()
                .accountId(account.getId())
                .oldestPendingTrxTimestamp(oldestPendingTrxTime)
                .mostRecentBookedTrxTimestamp(mostRecentBookedTrxTime)
                .mostRecentBookedBeforeAllPendingTrxTimestamp(mostRecentBookedBeforeAllPendingTrxTime)
                .build();
    }

    private Optional<Instant> getOldestPendingTrxTimestamp(List<AccountTransactionStatusSummary> summaries) {
        return summaries.stream()
                .map(AccountTransactionStatusSummary::getOldestPendingTrxTimestamp)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .min(Instant::compareTo);
    }

    private Optional<Instant> getMostRecentBookedTrxTimestamp(List<AccountTransactionStatusSummary> summaries) {
        return summaries.stream()
                .map(AccountTransactionStatusSummary::getMostRecentBookedTrxTimestamp)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .max(Instant::compareTo);
    }

    private Optional<Instant> getMostRecentBookedBeforeAllPendingTrxTimestamp(List<AccountTransactionStatusSummary> summaries) {
        if (summaries.stream().noneMatch(AccountTransactionStatusSummary::hasPendingWithoutPrecedingBookedTransaction)) {
            return summaries.stream()
                    .map(AccountTransactionStatusSummary::getMostRecentBookedBeforeAllPendingTrxTimestamp)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .min(Instant::compareTo);
        } else {
            // An account with a pending transaction without preceding booked transaction exists, so open-ended is the actual minimum
            return Optional.empty();
        }
    }

    /**
     * When retrieving transactions, this lower bound is used to make sure we don't fetch more data than needed in most
     * cases, while making sure we fetch all the data that we minimally need to not miss any transactions and perform
     * reconciliation when processing the retrieved data.
     *
     * @see com.yolt.accountsandtransactions.datascience.TransactionSyncService
     */
    private Optional<Instant> getTransactionRetrievalLowerBoundTimestamp(
            UUID siteId,
            Optional<Instant> oldestPendingTrxTimestamp,
            Optional<Instant> mostRecentBookedTrxTimestamp,
            Optional<Instant> mostRecentBookedBeforeAllPendingTrxTimestamp) {
        final Optional<Instant> relevantTimestamp;

        if (mostRecentBookedBeforeAllPendingTrxTimestamp.isPresent()) {
            // If we have a pending transaction in the db we need the returned transactions to include that trx to check
            // if the pending status has changed (pending trxs are not immutable). We take the booked transaction before
            // the pending transaction for this, to handle situations where the pending transaction's timestamp changes.

            // Monzo regularly returns transactions that have been pending for months. This results in large time windows
            // to reconcile against. Because we have many tombstone records in the `transactions` table, we get read
            // timeouts from Cassandra when fetching transactions for a large time period for customers with many
            // transactions. Given the decommissioning of Yolt and the fact that we don't want to do  "risky" changes,
            // we don't go back further than a month of pending transactions for Monzo.
            if (SITE_ID_MONZO.equals(siteId)) {
                var mostRecentBookedBeforeAllPendingTrxInstant = mostRecentBookedBeforeAllPendingTrxTimestamp.get();
                var oneMonthAgo = ZonedDateTime.now(clock).minus(1, ChronoUnit.MONTHS).toInstant();

                if (mostRecentBookedBeforeAllPendingTrxInstant.isBefore(oneMonthAgo)) {
                    log.info("Overriding lower fetch bound for Monzo user from '{}' to '{}', as user has pending transaction(s) older than a month", mostRecentBookedBeforeAllPendingTrxInstant, oneMonthAgo);
                    return Optional.of(oneMonthAgo);
                }
            }

            relevantTimestamp = mostRecentBookedBeforeAllPendingTrxTimestamp;
        } else if (oldestPendingTrxTimestamp.isPresent()) {
            // We have a pending trx without a preceding booked trx. In case this pending trx's timestamp changes, we get in trouble.
            // So just get as much data as possible in the hopes of finding a transaction with a stable timestamp.
            relevantTimestamp = Optional.empty();
        } else {
            // If we have no pending trx but we do have a booked trx we assume that our database is up-to-date *up to*
            // (but not including) the day on which the most recent trx took place.
            // If there are no transactions, this optional is empty and there is no determined lower bound.
            relevantTimestamp = mostRecentBookedTrxTimestamp;
        }

        // Reason for subtracting a day: we send a timestamp to providers and ask for transactions >= or > this
        // timestamp (this is unspecified). Providers reinterprets the timestamp as a date in many/most cases, and
        // since it's unspecified if providers sends a request for transactions >= or > the provided value, we
        // subtract a whole day.
        return relevantTimestamp.map(timestamp -> timestamp.minus(1, ChronoUnit.DAYS));
    }

}
