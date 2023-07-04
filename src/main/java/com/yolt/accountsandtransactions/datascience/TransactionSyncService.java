package com.yolt.accountsandtransactions.datascience;

import com.yolt.accountsandtransactions.inputprocessing.ProviderTransactionWithId;
import com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy;
import com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy.Instruction;
import com.yolt.accountsandtransactions.inputprocessing.TransactionReconciliationResultMetrics;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Optional.empty;
import static java.util.stream.Collectors.*;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.BOOKED;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.PENDING;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionSyncService {

    private final TransactionRepository transactionRepository;
    private final Clock clock;

    public Instruction reconcile(final ClientUserToken clientUserToken,
                                 final UUID accountId,
                                 final List<ProviderTransactionDTO> upstreamTransactions,
                                 final String provider) {
        if (upstreamTransactions.isEmpty()) {
            return TransactionInsertionStrategy.EMPTY_INSTRUCTION;
        }

        var userId = clientUserToken.getUserIdClaim();
        // Retrieve the transactions that we have in the database for the same time-period as upstreamTransactions.
        final List<Transaction> storedTransactions = retrieveStoredTransactionsInSameTimeWindow(
                provider, upstreamTransactions,
                (LocalDate earliestDate) -> transactionRepository.getTransactionsInAccountFromDate(userId, accountId, earliestDate)
        );

        var instruction = reconcileUpstreamTransactionsWithPersisted(storedTransactions, upstreamTransactions, provider, clock);

        var numberOfBookedTransactionsToDelete = instruction.getTransactionsToDelete().stream()
                .filter(tx -> tx.getStatus() == BOOKED)
                .count();
        if (numberOfBookedTransactionsToDelete > 0) {
            log.warn("Transaction sync reconciliation for account {} with provider {} resulted in {} booked transaction(s) to delete",
                    accountId, provider, numberOfBookedTransactionsToDelete);
        }

        return instruction;
    }

    /**
     * This function is complex, hopefully the comments will help explain what is going on.
     * <p>
     * Globally this function operates on two sets of transactions and tries to 'reconcile' them, the sets are:
     * - upstreamTransactions (list of new transactions sent to this pod by providers)
     * - storedTransactions (transactions already in our database over the same time-period as those in upstreamTransactions)
     * <p>
     * 'Reconciling' means:
     * - figure out which storedTransactions need to be deleted from the database because they are no longer present in upstream
     * - figure out which upstreamTransactions need to be upserted into the database (which are 'new')
     * <p>
     * <p>
     * The return value of this method is a data structure that contains metrics, that metrics object contains
     * a lot of useful Javadoc, make sure to check that out too: {@link TransactionReconciliationResultMetrics}.
     */
    static Instruction reconcileUpstreamTransactionsWithPersisted(final List<Transaction> storedTransactions,
                                                                  final List<ProviderTransactionDTO> originalUpstreamTransactions,
                                                                  final String provider,
                                                                  final Clock clock) {

        // Counters that keep track of what happens to stored transactions (those already in our database) because
        // of the new batch of incoming transactions.
        long matchedByExternalId = 0;           // # of transactions we could match to an incoming one using externalId (updated)
        long matchedByAttributesUnique = 0;     // # of transactions we could match to an incoming one using attributes (updated)
        long matchedByAttributesOptimistic = 0; // # of transactions we could match to an incoming one using attributes (updated)
        long unmatchedBooked = 0;               // # of transactions we could *not* match that have status booked
        long unmatchedPending = 0;              // # of transactions we could *not* match that have status pending
        long primaryKeyUpdates = 0;             // # of transactions we could match, but that we are deleting + inserting because of a change in the PK
        long bookedToPending = 0;               // # of transactions whose status was changed from booked to pending, this shouldn't happen
        long pendingToBooked = 0;               // # of transactions whose status was changed from pending to booked

        // Our job is to fill these collections.
        List<Transaction> transactionsToDelete = new ArrayList<>();
        List<ProviderTransactionWithId> transactionsToUpdate = new ArrayList<>();
        List<ProviderTransactionWithId> transactionsToInsert;
        List<ProviderTransactionWithId> transactionsToIgnore = new ArrayList<>();

        // This data structure contains a copy of the upstream transactions, it gives us the ability to do efficient
        // lookups / deletions.
        TransactionIndex upstreamTransactions = new TransactionIndex(originalUpstreamTransactions);

        // Counters to monitor bank behaviour with respect to identifiers
        final long upstreamTrxWithDuplicateExternalIds = countDuplicateTransactions(upstreamTransactions.byExternalId().values());
        final long upstreamTrxWithMissingExternalIds = originalUpstreamTransactions.stream()
                .map(ProviderTransactionDTO::getExternalId)
                .filter(StringUtils::isEmpty)
                .count();

        for (Transaction storedTrx : storedTransactions) {
            final Optional<ProviderTransactionDTO> optionalMatch;
            //
            // First try to find matches for a transaction by looking at the externalId field.  We use the externalId
            // field for this because this field is the (optional) identifier a bank assigns to a transaction.
            //
            // Banks are known to do any of the following:
            // a) not assign an id to any transaction (e.g. Barclays)
            // b) assign a unique id to *most* but not all transactions (e.g. ABN Amro, doesn't assign an id to ATM trxs)
            // c) assign an id to all transactions, with the caveat that it's not unique
            // d) assign a unique identifier to all transactions
            //
            // In a perfect world all banks would do d), but because they don't the if-statement that follows has an else branch.
            //
            if (storedTrx.getExternalId() != null && upstreamTransactions.byExternalId().containsKey(storedTrx.getExternalId())) {
                final List<ProviderTransactionDTO> candidates = upstreamTransactions.byExternalId().get(storedTrx.getExternalId());
                if (candidates.size() == 1) {
                    // Happy path, category d) bank.
                    optionalMatch = Optional.of(candidates.get(0));
                } else {
                    // Bank is in category c) and sends duplicate externalIds, use some heuristics to match.
                    optionalMatch = candidates.stream()
                            .filter(trx -> toLocalDate(trx.getDateTime()).equals(storedTrx.getDate()))
                            // fixme this is broken, trx.getAmount() is always positive, storedTrx.getAmount() has a sign.
                            .filter(trx -> trx.getAmount().compareTo(storedTrx.getAmount()) == 0)
                            .findFirst()
                            // If that fails, search for one with only the same date.
                            .or(() -> candidates.stream()
                                    .filter(trx -> toLocalDate(trx.getDateTime()).equals(storedTrx.getDate()))
                                    .findFirst());
                }
                if (optionalMatch.isPresent()) {
                    ++matchedByExternalId;
                }
            } else {
                //
                // No match was found by using the externalId.  Check if we can find a matching transaction by comparing
                // the amount and date.
                //
                // Note: the below heuristics have no 'scientific' research behind them.  If you are looking at this
                //       code because it has problems: feel free to (carefully) refine these heuristics.
                //
                List<ProviderTransactionDTO> upstreamCandidatesByAmountAndDate = upstreamTransactions.findByAmount(storedTrx).stream()
                        .filter(trx -> toLocalDate(trx.getDateTime()).equals(storedTrx.getDate()))
                        // If the stored transaction has status booked, we must not consider pending
                        // upstream transactions.
                        .filter(trx -> storedTrx.getStatus() != BOOKED || trx.getStatus() != PENDING)
                        .collect(Collectors.toList());
                if (upstreamCandidatesByAmountAndDate.size() > 1) {
                    // If we have more than one match, add the description as a discriminating feature.
                    upstreamCandidatesByAmountAndDate = upstreamCandidatesByAmountAndDate.stream()
                            .filter(trx -> trx.getDescription().equals(storedTrx.getDescription()))
                            .collect(Collectors.toList());
                }

                if (upstreamCandidatesByAmountAndDate.size() >= 1) {
                    // If there are >= 2 candidates we can just pick one of them at random since the transactions
                    // are so alike that for the purposes of classification etc. it's not important (or even possible)
                    // to distinguish between them.  Note that if size is >= 2 at this point, that means the stored and
                    // matching transaction have identical date, amount, and description.
                    optionalMatch = Optional.of(upstreamCandidatesByAmountAndDate.get(0));
                    if (upstreamCandidatesByAmountAndDate.size() >= 2) {
                        ++matchedByAttributesOptimistic;
                    } else {
                        ++matchedByAttributesUnique;
                    }
                } else {
                    // Failed to find the stored transaction in the incoming batch, it will be deleted.  It might be
                    // the case that the transaction is in fact present in the upstream batch, but that our logic has
                    // failed to find it because, if it is present it won't disappear, it will merely be deleted and
                    // re-inserted again.  This does have one consequence: the (internal) transactionId will change.
                    optionalMatch = empty();
                }
            }
            if (optionalMatch.isPresent()) {
                if (storedTrx.getStatus() == BOOKED && optionalMatch.get().getStatus() == PENDING) {
                    // Keep track of instances where we alter a transactions status from BOOKED to PENDING: this is unusual.
                    // In the world of banking moving from BOOKED back to PENDING isn't possible, however, we are consuming
                    // APIs that don't necessarily reflect 'the truth' so we have to account for this and cannot simply
                    // prevent it.  We have to trust what a bank tells us and should not apply our own business rules.
                    ++bookedToPending;
                } else if (storedTrx.getStatus() == PENDING && optionalMatch.get().getStatus() == BOOKED) {
                    // The transaction moved from PENDING to BOOKED.  Keep an informational statistic.
                    ++pendingToBooked;
                }
            }
            if (optionalMatch.isEmpty()) {
                // No match found, delete the stored transaction.
                transactionsToDelete.add(storedTrx);
                // Update statistics.
                if (BOOKED == storedTrx.getStatus()) {
                    ++unmatchedBooked;
                } else {
                    ++unmatchedPending;
                }
                continue;
            }

            // We have a matching transaction.
            final ProviderTransactionDTO match = optionalMatch.get();

            // Remove the matching transaction from the index we use to check for matches.  It cannot be used more than once.
            upstreamTransactions.removeFromIndex(match);

            // If we have found a match on a different date we need to delete the transaction from the database and
            // re-insert it after.  The reason is that date is part of the primary key and Cassandra doesn't permit
            // updates to PK values, a delete + insert is needed.  The transaction will retain its transactionId, but
            // it will lose all attributes that datascience / user has added to the transaction.  Reason is that this
            // service operates on a subset of the columns of the datascience keyspace.
            final boolean changedDate = !toLocalDate(match.getDateTime()).equals(storedTrx.getDate());

            // We need to do a similar check for status, if a transactions' status changes (this field is part of the
            // primary key in the datascience keyspace) we will also need to delete it before inserting it again.
            final boolean changedStatus = match.getStatus() != storedTrx.getStatus();

            if (changedDate || changedStatus) {
                transactionsToDelete.add(storedTrx);
                ++primaryKeyUpdates;
            }

            // We need to know if the transactions that we have (storedTrx) is identical to any of the transactions
            // we got from the upstream. For this we have to convert one to the other, this might not be the best place for this.
            // If the two transactions are identical (see equals implementation in Transaction) then we can ignore the transaction
            // and add them to the ignored list.
            var providerTransactionAsTransaction = TransactionService.map(
                    new ProviderTransactionWithId(match, storedTrx.getId()),
                    new TransactionService.AccountIdentifiable(storedTrx.getUserId(), storedTrx.getAccountId(), storedTrx.getCurrency()),
                    false,
                    clock,
                    storedTrx.getCreatedAt()
            );

            if (providerTransactionAsTransaction.equals(storedTrx)) {
                // ignore the upstream transaction if nothing changed
                transactionsToIgnore.add(new ProviderTransactionWithId(match, storedTrx.getId()));
            } else {
                // otherwise update the transaction.
                transactionsToUpdate.add(
                        // We copy over transactionId from the stored transaction so that we're sure we're updating an
                        // existing transaction.
                        new ProviderTransactionWithId(match, storedTrx.getId())
                );
            }
        }

        // At this point we have removed from upstreamTransactions all the storedTransactions in the same time period
        // that we could match.  What's left in upstreamTransactions are new transactions that we will insert.

        // Insert the remaining transactions in the index (these are new transactions).
        transactionsToInsert = upstreamTransactions.transactions().stream()
                // Assign a random id to the new transaction.
                .map(t -> new ProviderTransactionWithId(t, UUID.randomUUID().toString()))
                .collect(Collectors.toList());

        // calculate the oldest transaction change date
        var oldestChangeTransactionDate
                = calculateOldestTransactionChangeDate(transactionsToDelete, transactionsToInsert, transactionsToUpdate);

        // Keep track of statistics so we can track the performance of this code.
        var metrics = TransactionReconciliationResultMetrics.builder()
                .provider(provider)
                // Informational only
                .upstreamTotal(originalUpstreamTransactions.size())
                .upstreamNew(transactionsToInsert.size())
                .upstreamUnchanged(transactionsToIgnore.size())
                // How well does the reconciliation algorithm match transactions?
                .storedTotal(storedTransactions.size())
                .storedMatchedByExternalId(matchedByExternalId)
                .storedMatchedByAttributesUnique(matchedByAttributesUnique)
                .storedMatchedByAttributesOptimistic(matchedByAttributesOptimistic)
                .storedBookedNotMatched(unmatchedBooked)
                .storedPendingNotMatched(unmatchedPending)
                .storedPrimaryKeyUpdated(primaryKeyUpdates)
                .storedPendingToBooked(pendingToBooked)
                .storedBookedToPending(bookedToPending)
                // Bank data quality
                .upstreamQualityMissingExternalIds(upstreamTrxWithMissingExternalIds)
                .upstreamQualityDuplicateExternalIds(upstreamTrxWithDuplicateExternalIds)
                .build();

        return new Instruction(
                transactionsToDelete,
                transactionsToInsert,
                transactionsToUpdate,
                transactionsToIgnore,
                metrics,
                oldestChangeTransactionDate
        );
    }

    /**
     * Calculate the oldest transaction change date by taking the min(date) of all the transactions in the given sets.
     * The function will return {@link Optional#empty()} when all the lists are empty
     *
     * @param transactionsToDelete the transactions to delete
     * @param transactionsToInsert the transactions to insert
     * @param transactionsToUpdate the transactions to update
     * @return a possible oldest transaction change date
     */
    private static Optional<LocalDate> calculateOldestTransactionChangeDate(
            final @NonNull List<Transaction> transactionsToDelete,
            final @NonNull List<ProviderTransactionWithId> transactionsToInsert,
            final @NonNull List<ProviderTransactionWithId> transactionsToUpdate) {

        // take the oldest changed transactions from all the changes we are going to make/ write to the database.
        var deleteDates = transactionsToDelete.stream()
                .map(Transaction::getDate)
                .min(LocalDate::compareTo);
        var insertDates = transactionsToInsert.stream()
                .map(providerTransactionWithId -> toLocalDate(providerTransactionWithId.getProviderTransactionDTO().getDateTime()))
                .min(LocalDate::compareTo);
        var updateDates = transactionsToUpdate.stream()
                .map(providerTransactionWithId -> toLocalDate(providerTransactionWithId.getProviderTransactionDTO().getDateTime()))
                .min(LocalDate::compareTo);

        // flatten + min()
        return Stream.of(deleteDates, insertDates, updateDates)
                .filter(Optional::isPresent)
                .map(Optional::get) // safe to call get() because filter
                .min(LocalDate::compareTo);
    }

    private static long countDuplicateTransactions(Collection<? extends Collection<?>> cs) {
        return cs.stream()
                .map(Collection::size)
                .filter(s -> s > 1)
                .reduce(0, Integer::sum);
    }

    public static List<Transaction> retrieveStoredTransactionsInSameTimeWindow(
            String provider,
            List<ProviderTransactionDTO> upstreamTransactions,
            Function<LocalDate, List<Transaction>> retrieveTransactionsFn) {

        boolean retrieveStoredTransactionsInSameTimeWindowUsedDate = false;

        // Determine the earliest point in time present in the upstream transactions.
        var earliestUpstreamTransactionZDT = upstreamTransactions.stream()
                .map(ProviderTransactionDTO::getDateTime) // 2021-10-24T00:00+02:00[Europe/Amsterdam]
                .min(ZonedDateTime::compareTo)
                .orElseThrow(() -> new IllegalArgumentException("upstreamTransactions must not be empty"));

        // This .toLocalDate() call truncates the time + timezone information (unfortunate), we have to do this because we need to query using
        // just the date (part of PK)
        var earliestUpstreamTransactionDate = earliestUpstreamTransactionZDT.toLocalDate(); // 2021-10-24

        // Subtract 1 day to make sure we get transactions that cross the date boundary.
        var earliestUpstreamTransactionDateMinusOne = earliestUpstreamTransactionDate.minusDays(1); // 2021-10-23

        var storedTransactions = retrieveTransactionsFn.apply(earliestUpstreamTransactionDateMinusOne); // haal alles op van 23-10 >
        var earliestUpstreamTransactionInstant = earliestUpstreamTransactionZDT.toInstant();

        List<Pair<String, Transaction>> storedTransactionsInTimeWindow = new ArrayList<>(storedTransactions.size());
        List<Pair<String, Transaction>> evictedTransactionsFromTimeWindow = new ArrayList<>();

        boolean isRabobank = "RABOBANK".equalsIgnoreCase(provider);
        HashSet<String> externalIds = isRabobank ? new HashSet<>(getExternalIds(upstreamTransactions)) : new HashSet<>();

        if (isRabobank) {
            var storedExtIds = storedTransactions.stream()
                    .map(Transaction::getExternalId)
                    .map(TransactionSyncService::integerFromExternalId)
                    .collect(toList());
            var upstreamExtIds = upstreamTransactions.stream()
                    .map(ProviderTransactionDTO::getExternalId)
                    .map(TransactionSyncService::integerFromExternalId)
                    .collect(toList());

            var storedLowest = storedExtIds.stream().min(Integer::compareTo).orElse(-1);
            var storedHighest = storedExtIds.stream().max(Integer::compareTo).orElse(-1);
            final Set<Integer> storedExpectedRange = storedLowest != -1 && storedHighest != -1
                    ? IntStream.rangeClosed(storedLowest, storedHighest).boxed().collect(toSet())
                    : Collections.emptySet();

            var upstreamLowest = upstreamExtIds.stream().min(Integer::compareTo).orElse(-1);
            var upstreamHighest = upstreamExtIds.stream().max(Integer::compareTo).orElse(-1);
            final Set<Integer> upstreamExpectedRange = upstreamLowest != -1 && upstreamHighest != -1
                    ? IntStream.rangeClosed(upstreamLowest, upstreamHighest).boxed().collect(toSet())
                    : Collections.emptySet();
            var upstreamIsContiguous = new HashSet<>(upstreamExtIds).equals(upstreamExpectedRange);

            if (upstreamIsContiguous) {
                storedTransactions.removeIf(p -> integerFromExternalId(p.getExternalId()) < upstreamLowest);
            } else {
                log.warn("Rabobank upstream is not contiguous, this is unexpected.");
            }
        }

        for (Transaction tx : storedTransactions) {

            //The external_id of one of the upstream tx's is the same as one of the stored ones, they should be the same
            if (isRabobank && externalIds.contains(tx.getExternalId())) {
                storedTransactionsInTimeWindow.add(Pair.of("external-id", tx));
                continue;
            }

            // If we have a timestamp on file for the transaction, we rely on that.  Not all transactions have this, this was introduced in april of 2020.
            if (tx.getTimestamp() != null) {
                if (!tx.getTimestamp().truncatedTo(ChronoUnit.SECONDS).isBefore(earliestUpstreamTransactionInstant.truncatedTo(ChronoUnit.SECONDS))) {
                    storedTransactionsInTimeWindow.add(Pair.of("timestamp", tx)); // 2021-10-23 00:00 +02:00
                } else {
                    evictedTransactionsFromTimeWindow.add(Pair.of("evicted-timestamp", tx));
                }
                continue;
            }

            // If we don't have a transactionTimestamp on file, we need to fallback to using the date for comparisons, this is imperfect.
            if (!tx.getDate().isBefore(earliestUpstreamTransactionDate)) {
                retrieveStoredTransactionsInSameTimeWindowUsedDate = true;
                storedTransactionsInTimeWindow.add(Pair.of("date", tx));
                continue;
            }

            evictedTransactionsFromTimeWindow.add(Pair.of("evicted-no-match", tx));
        }

        if (retrieveStoredTransactionsInSameTimeWindowUsedDate) {
            //
            // If this message does not show up in production we can delete a bunch of code from this function which
            // is nice.
            //
            log.info("retrieveStoredTransactionsInSameTimeWindow used date.  This should no longer occur.");
        }

        try {
            outputTransactionsInWindowInTabular(evictedTransactionsFromTimeWindow, earliestUpstreamTransactionInstant,
                    (header, group) -> log.info("Evicted stored transactions in time window for {}:\n{}\n{}", provider, header, String.join("\n", group)));
        } catch (Exception e) {
            log.error("Failed to output evicted transactions from time window.");
        }

        return storedTransactionsInTimeWindow.stream()
                .map(Pair::getRight)
                .collect(toList());
    }

    private static Integer integerFromExternalId(@NonNull String externalId) {
        return Integer.parseInt(externalId.trim());
    }

    private static List<String> getExternalIds(List<ProviderTransactionDTO> upstreamTransactions) {
        return upstreamTransactions.stream().
                map(ProviderTransactionDTO::getExternalId).
                collect(Collectors.toList());
    }

    private static LocalDate toLocalDate(final ZonedDateTime zonedDateTime) {
        return zonedDateTime.toLocalDate();
    }

    static void outputTransactionsInWindowInTabular(final @NonNull List<Pair<String, Transaction>> transactionWithSource,
                                                    final @NonNull Instant earliestUpstreamTransactionInstant,
                                                    final @NonNull BiConsumer<String, List<String>> groupOut) {

        var format = "|%1$-36s|%2$-36s|%3$-12s|%4$-32s|%5$-24s|%6$-24s|%7$-16s";
        var counter = new AtomicInteger();

        transactionWithSource.stream()
                .collect(groupingBy(ignored -> counter.getAndIncrement() / 50)) // output in chunks of 50
                .values()
                .forEach(group -> {
                    var tuples = group.stream()
                            .map(tuple -> {
                                Transaction trx = tuple.getRight();
                                String source = tuple.getLeft();

                                return String.format(format,
                                        trx.getAccountId(),
                                        trx.getId(),
                                        trx.getStatus(),
                                        Optional.ofNullable(trx.getExternalId()).orElse("n/a"),
                                        Optional.ofNullable(trx.getTimestamp()),
                                        earliestUpstreamTransactionInstant,
                                        source
                                );
                            })
                            .collect(toList());

                    var header = String.format(format,
                            "account-id",
                            "transaction-id",
                            "status",
                            "external-id",
                            "transaction instant",
                            "earliest instant",
                            "source"
                    );

                    groupOut.accept(header, tuples);
                });
    }

}
