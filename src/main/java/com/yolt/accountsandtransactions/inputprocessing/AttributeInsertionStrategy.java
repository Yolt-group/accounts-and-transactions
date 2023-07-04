package com.yolt.accountsandtransactions.inputprocessing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.yolt.accountsandtransactions.datascience.TransactionSyncService;
import com.yolt.accountsandtransactions.inputprocessing.matching.*;
import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher.MatchResult;
import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher.MatchResults;
import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher.Unmatched;
import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction.ProviderGeneralizedTransaction;
import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction.StoredGeneralizedTransaction;
import com.yolt.accountsandtransactions.transactions.Transaction;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.springframework.util.Assert;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.yolt.accountsandtransactions.Predef.Streams.foldLeft;
import static com.yolt.accountsandtransactions.Predef.Streams.zip;
import static com.yolt.accountsandtransactions.Predef.concat;
import static com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher.Unmatched.Reason.*;
import static com.yolt.accountsandtransactions.transactions.Transaction.FillType.BACKFILLED;
import static com.yolt.accountsandtransactions.transactions.Transaction.FillType.REGULAR;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.*;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.BOOKED;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.PENDING;

/**
 * This implementation of the {@link TransactionInsertionStrategy} matches two different type of transactions,
 * namely the {@link ProviderTransactionDTO} and {@link Transaction}, based on a set of transaction attributes.
 * <p/>
 * <h3>
 * Attribute, AttributeExtractors and AttributeSelectors.
 * </h3>
 * A transaction {@link Attribute} represents a database field name plus its associated value, such as external-id, amount,
 * booking-date, creditor/debtor-name, etc.
 * <p/>
 * {@link Attribute}s are taken from respectively the (upstream) {@link ProviderTransactionDTO}s
 * and (stored) {@link Transaction}s; these are the two transaction types we are trying to match.
 * Attributes are *created* by an {@link AttributeTransactionMatcher.AttributeExtractor} which defines "how" the attribute from either transaction type
 * is taken. This is done by implementing {@link AttributeTransactionMatcher.AttributeExtractor#extract(GeneralizedTransaction)}}.
 * We can define as many extractors as we like.
 * <p/>
 * An {@link AttributeTransactionMatcher.AttributeExtractor} which takes the booking-date from both transactions should be defined as:
 * <pre>
 *  IdentityAttributeSelector<String> EXTERNAL_ID
 *      = new IdentityAttributeSelector<>(transaction -> new Attribute<>("external-id", transaction.getExternalId()));
 * </pre>
 * The end goal of defining/ extracting the attributes is to construct a "key" for each transaction,
 * in this case a set of {@link Attribute}s, which we can use in a number of set operations to determine its equality.
 * <pre>
 * Example:
 *
 *   We define our extractions to extract external-id and booking-date.
 *
 *   Upstream Transactions U1 {external-id => 123, booking-date -> 1970, ...}
 *   Upstream Transactions U2 {external-id => 456, booking-date -> 2000, ...}
 *   Stored Transactions S1 {external-id => 123, booking-date -> "1970", ...}
 *   Stored Transactions S2 {external-id => 456, booking-date -> "2000", ...}
 *
 *   Extract the set of attributes (ea. the key) from the transactions
 *
 *   Attributes U1 -> extract(external-id+booking-date) -> Set(Attribute(external-id => 123), Attribute("booking-date" => 1970)
 *   Attributes U2 -> extract(external-id+booking-date) -> Set(Attribute(external-id => 456), Attribute("booking-date" => 2000)
 *   Attributes S1 -> extract(external-id+booking-date) -> Set(Attribute(external-id => 123), Attribute("booking-date" => 1970)
 *   Attributes S2 -> extract(external-id+booking-date) -> Set(Attribute(external-id => 456), Attribute("booking-date" => 2000)
 *
 *   These sets can be used in {@link Sets#intersection(Set, Set)} and {@link Sets#difference(Set, Set)}.
 *   Therefore:
 *     intersection((U1+U2), (S1+S2) yields a match on U1 and S1 (both external-id and booking-date have the same value)
 *     difference((U1+U2)), (S1+S2)) yields the unmatched transaction U2 (it has no counterpart in the set of stored transactions)
 *     difference((S1+S2)), (U1+U2)) yields the unmatched transaction S2 (it has no counterpart in the set of upstream transactions)
 * </pre>
 * Before we start matching on the attributes, we want to set some restrictions on which transactions are included in the match.
 * For example, we might want to only try to match transactions that have an external-id that is not null or blank (narrow match).
 * Or, we might want to restrict the value an attribute can take (e.a. excluding transactions with placeholder
 * values such as 00000000000 or "Not Provided")
 * <p/>
 * If, for example, we define a restriction that the external-id cannot be null or blank, then every transaction which has
 * an external-id that is either null or blank will not be included/ processed and redirected to the
 * unmatched output of its corresponding type.
 * <p/>
 * Restrictions are implemented via a {@link AttributeSelector}. Attribute selectors take some
 * attribute and either transaction type and test these against whatever restriction was imposed
 * by the selector (via {@link AttributeSelector#selectAttribute}).
 * The accept method returns a {@link AttributeSelector.Result} containing the (extracted) attribute and
 * the test results (accepted=true|false).
 * <p/>
 * The default attribute selector is the {@link AttributeSelector.IdentityAttributeSelector}. This selector accepts any transaction,
 * has no restrictions, and returns the extracted attribute as-is.
 * <p/>
 * Restrictions on extractors can be chained via the decorator pattern.
 * <pre>
 * Example:
 *
 *    Wrap an AttributeExtractor into a IdentityAttributeSelector and apply the NotBlankAttributeSelectorDecorator
 *    to restrict the value from being null or blank.
 *
 *    new NotBlankAttributeSelectorDecorator(new IdentityAttributeSelector<>(new AttributeExtractor<>() {
 *         public Attribute<String> extract(ProviderTransactionDTO upstreamTransaction) {
 *             return new Attribute<>("external-id", upstreamTransaction.getExternalId());
 *         }
 *         public Attribute<String> extract(Transaction transaction) {
 *             return new Attribute<>("external-id", transaction.getExternalId());
 *         }
 *     }));
 * </pre>
 * Conclusion: We don't define attributes, but we define extractors and selectors.
 * Extractors define how we obtain an attribute on which the selectors operate.
 * <p/>
 * After we have collected all the transactions and selector results, we run every defined matcher
 * against the two sets of transactions until there are no matchers left.
 */
@Slf4j
public class AttributeInsertionStrategy implements TransactionInsertionStrategy {

    private final Mode mode;
    private final TransactionLoader transactionLoader;
    private final TransactionIdProvider<UUID> transactionIdProvider;
    private final ProviderConfiguration providerConfiguration;

    public AttributeInsertionStrategy(
            final @NonNull Mode mode,
            final @NonNull TransactionLoader transactionLoader,
            final @NonNull TransactionIdProvider<UUID> transactionIdProvider,
            final @NonNull ProviderConfiguration providerConfiguration) {
        this.mode = mode;
        this.transactionLoader = transactionLoader;
        this.transactionIdProvider = transactionIdProvider;
        this.providerConfiguration = providerConfiguration;
    }

    @Override
    public @NonNull Mode getMode() {
        return this.mode;
    }

    @Override
    public Instruction determineTransactionPersistenceInstruction(
            final List<ProviderTransactionDTO> upstreamTransactions,
            final ClientUserToken clientUserToken,
            final UUID accountId,
            final String provider,
            final CurrencyCode currencyCode) throws UnsupportedOperationException {

        // Ideally we want this method to receive booked upstream transactions so we don't have to filter here
        // The code below (var bookedUpstreamTransactions =...) should be removed in favour of passing only booked transactions.
        // Until that time we leave the above mentioned code in to no break anything.
        // TODO: remove the booked filter if the upstreamTransactions only contain booked transactions.
        var bookedUpstreamTransactions = upstreamTransactions.stream()
                .filter(transaction -> transaction.getStatus() == BOOKED)
                .collect(toList());

        Assert.isTrue(bookedUpstreamTransactions.stream().allMatch(transaction -> transaction.getStatus() == BOOKED),
                "This method can only operate on booked upstream transactions.");

        if (bookedUpstreamTransactions.isEmpty()) {
            return TransactionInsertionStrategy.EMPTY_INSTRUCTION;
        }

        // retrieve stored transactions from the database
        var storedTransactions = TransactionSyncService.retrieveStoredTransactionsInSameTimeWindow(
                provider,
                bookedUpstreamTransactions,
                (LocalDate earliestDate) -> transactionLoader.load(clientUserToken.getUserIdClaim(), accountId, earliestDate)
        );

        // If there are no stored transactions, we can insert all the upstream transactions without passing through the matcher.
        // Note that since the rules for the matchers are not applied, no assumptions are made about the upstream transactions.
        // This means that transactions are inserted as-is.
        // If the transactions are loaded back from the database and the ruleset (duplicate check, rejected check) is applied to them
        // these errors, which could have been present when initially storing the transactions, will manifest itself.
        // This exception mostly exists for One Off AIS where reconciliation is not required.
        if (storedTransactions.isEmpty()) {
            var bookedUpstreamGeneralizedTransactions = bookedUpstreamTransactions.stream()
                    .map(GeneralizedTransaction::toGeneralized)
                    .collect(toList());

            var oldestBookedUpstreamTransactionDate
                    = calculateOldestTransactionDate(bookedUpstreamGeneralizedTransactions);

            log.info("""
                        Mode: {}
                        Upstream total: {}, oldest: {}
                        Using reconciliation fast-path (e.a. reconciliation is skipped) as there are no stored transactions available.
                    """, getMode(), bookedUpstreamGeneralizedTransactions.size(), oldestBookedUpstreamTransactionDate);

            return new Instruction(
                    createProviderTransactionWithId(bookedUpstreamGeneralizedTransactions, transactionIdProvider, REGULAR),
                    oldestBookedUpstreamTransactionDate
            );
        }

        var storedTransactionsByStatus = storedTransactions.stream()
                .collect(groupingBy(Transaction::getStatus));
        var bookedStoredTransactions = storedTransactionsByStatus.getOrDefault(BOOKED, emptyList());
        var pendingStoredTransactions = storedTransactionsByStatus.getOrDefault(PENDING, emptyList());

        var bookedIntermediateInstruction = toInstruction(
                accountId,
                bookedUpstreamTransactions,
                bookedStoredTransactions
        );

        // Remove any pending stored transaction.
        // As we only reconsile on booked transactions, the pending transactions, currently in the database, will never be reconciled and never cleaned up.
        // Also, the transaction fetch window is based on the oldest pending transaction,
        // if we don't remove the pending transactions, the window will eventually become the MAX window size for every fetch.
        Assert.isTrue(pendingStoredTransactions.stream().allMatch(transaction -> transaction.getStatus() == PENDING),
                "pendingStoredTransactions should only contain pending transactions.");
        pendingStoredTransactions
                .forEach(transaction -> log.info("Deleting pending transaction {} with external-id = {} for provider = {}, mode = {}", transaction.getId(), Optional.ofNullable(transaction.getExternalId()), provider, getMode()));

        return bookedIntermediateInstruction
                .appendToDelete(pendingStoredTransactions);
    }

    @VisibleForTesting
    public Instruction toInstruction(
            final UUID accountId,
            final List<ProviderTransactionDTO> upstreamTransactions,
            final List<Transaction> storedTransactions) {

        Assert.isTrue(upstreamTransactions.size() > 0,
                "This method requires at least one (1) booked upstream transaction.");
        Assert.isTrue(upstreamTransactions.stream().allMatch(transaction -> transaction.getStatus() == BOOKED),
                "This method can only operate on booked upstream transactions.");
        Assert.isTrue(storedTransactions.stream().allMatch(transaction -> transaction.getStatus() == BOOKED),
                "This method can only operate on booked stored transactions.");

        var allUpstream = GeneralizedTransaction.toProviderGeneralized(upstreamTransactions);
        var allStored = GeneralizedTransaction.toStoredGeneralized(storedTransactions);

        var selectedWindow = providerConfiguration.syncWindowSelector.selectWindow(allUpstream, allStored);

        var upstream = selectedWindow.truncate(allUpstream);
        var stored = selectedWindow.truncate(allStored);

        // calculate the booked transaction date in the upstream
        var oldestUpstreamTransactionDateOrEpoch = calculateOldestTransactionDate(upstream)
                .orElseThrow(() -> new IllegalArgumentException("At least 1 (one) booked upstream transaction is required."));

        // calculate the most recently added booked transaction from the storage.
        var mostRecentStoredTransactionDateOrEpoch
                = calculateMostRecentTransactionDate(stored).orElse(LocalDate.EPOCH);

        var oldestStoredTransactionDateOrEpoch
                = calculateOldestTransactionDate(stored).orElse(LocalDate.EPOCH);

        // reconcile on booked transactions only
        var matchResults = reconcileTransactionsOnAttributes(providerConfiguration, upstream, stored);

        logMatchResults(
                mode,
                providerConfiguration.provider,
                accountId,
                matchResults,
                upstream,
                stored,
                oldestUpstreamTransactionDateOrEpoch,
                oldestStoredTransactionDateOrEpoch,
                mostRecentStoredTransactionDateOrEpoch,
                selectedWindow
        );

        // The last element in the matchResults contains the final state.
        // We only use this result to determine the instruction set
        var finalMatchResult = matchResults.last()
                .orElseThrow(() -> new IllegalStateException("Fatal: No match results available."));

        handleUnmatchedStored(
                finalMatchResult.unmatchedStored,
                finalMatchResult.unmatchedUpstream,
                oldestUpstreamTransactionDateOrEpoch,
                oldestStoredTransactionDateOrEpoch,
                mostRecentStoredTransactionDateOrEpoch);

        return toInsertInstruction(finalMatchResult.unmatchedUpstream, mostRecentStoredTransactionDateOrEpoch);
    }

    private Instruction toInsertInstruction(
            final List<Unmatched<ProviderGeneralizedTransaction>> unmatchedUpstream,
            final LocalDate mostRecentStoredTransactionDateOrEpoch) {

        // group the upstream transactions by reason
        var unmatchedUpstreamByReason = unmatchedUpstream.stream()
                .collect(groupingBy(unmatched -> unmatched.reason));

        // extract all groups
        var peerless = unmatchedUpstreamByReason.getOrDefault(PEERLESS, emptyList());
        var duplicates = unmatchedUpstreamByReason.getOrDefault(DUPLICATE, emptyList());
        var rejected = unmatchedUpstreamByReason.getOrDefault(REJECTED, emptyList());
        var unprocessed = unmatchedUpstreamByReason.getOrDefault(UNPROCESSED, emptyList());

        if (unprocessed.size() > 0) {
            throw new IllegalStateException("""
                    Mode: %s
                    Fatal: Found %d unprocessed transaction(s) in the upstream (%s).
                    """.formatted(mode, unprocessed.size(), providerConfiguration.provider));
        }

        // we have no insert strategy for rejected and duplicate transactions in the upstream,
        // the only thing we can do is fail the processing and create a better matcher for this provider
        // which eliminates the rejection and resolves the duplicates with a more exact match strategy (better attributes)
        if (rejected.size() > 0 || duplicates.size() > 0) {
            throw new IllegalStateException("""
                    Mode: %s
                    Fatal: Found %d rejected and/or %d duplicate transaction(s) in the upstream (%s).
                    Processing of this transaction set has been interrupted and all transactions will be dropped.
                    A modification to the attribute matching is required. Until the rejected/ duplicate
                    transactions are removed, this account can no longer be updated as the matching would
                    constantly fail.
                                        
                    Rejected transactions:
                    %s
                                        
                    Duplicate transactions:
                    %s
                                        
                    Consult the match report for detailed information about the matching output.
                    """.formatted(
                    mode,
                    rejected.size(),
                    duplicates.size(),
                    providerConfiguration.provider,
                    outputTransactionsInTabularAsString(rejected),
                    outputTransactionsInTabularAsString(duplicates)
            ));
        }

        // The previous matcher accidentally deleted some transactions in the past.
        // This was caused by mis-alignment of transaction windows.
        // When users re-consent their account, we will get 3 to 18 months of historical data.
        // This data includes the transactions which were accidentally deleted, which will cause them
        // to be re-inserted again with a different transaction identifier (technically creating a duplicate).
        // To prevent the (historical) duplicates from being served out again to the client/ user
        // we tag these transactions with "BACKFILLED" and will get filtered out in the API.
        // Provider transactions will be tagged with "BACKFILLED" if the transaction date is older than 7 days given
        // the most recently stored transaction.

        // Partition the - to be inserted - transactions based on the 7 days window, creating two sets,
        // one that contains transactions that are earlier than 7 days given the most recently stored transaction
        // and one that contains transactions which are older than 7 days given the most recently stored transaction.
        var transactionsPartitionedOnInsertWindow = peerless.stream()
                .map(unmatched -> unmatched.transaction)
                .collect(partitioningBy(upstreamTransaction -> {
                    return upstreamTransaction.getDate()
                            .isAfter(mostRecentStoredTransactionDateOrEpoch.minusDays(7));
                }));

        var toInsertWithinWindow
                = transactionsPartitionedOnInsertWindow.getOrDefault(true, emptyList());
        var toInsertOutsideWindow
                = transactionsPartitionedOnInsertWindow.getOrDefault(false, emptyList());

        var oldestUpstreamTransactionWithinWindow = calculateOldestTransactionDate(toInsertWithinWindow);

        var toInsertWithId = concat(
                createProviderTransactionWithId(toInsertWithinWindow, transactionIdProvider, REGULAR),
                createProviderTransactionWithId(toInsertOutsideWindow, transactionIdProvider, BACKFILLED)
        );

        return new Instruction(toInsertWithId, oldestUpstreamTransactionWithinWindow);
    }

    private void handleUnmatchedStored(
            final List<Unmatched<StoredGeneralizedTransaction>> unmatchedStored,
            final List<Unmatched<ProviderGeneralizedTransaction>> unmatchedUpstream,
            final LocalDate oldestUpstreamTransaction,
            final LocalDate oldestStoredTransactionDateOrEpoch,
            final LocalDate mostRecentStoredTransactionDateOrEpoch) {

        // group the stored transactions by reason
        var unmatchedStoredByReason = unmatchedStored.stream()
                .collect(groupingBy(unmatched -> unmatched.reason));

        var peerless = unmatchedStoredByReason.getOrDefault(PEERLESS, emptyList());
        var duplicates = unmatchedStoredByReason.getOrDefault(DUPLICATE, emptyList());
        var rejected = unmatchedStoredByReason.getOrDefault(REJECTED, emptyList());
        var unprocessed = unmatchedStoredByReason.getOrDefault(UNPROCESSED, emptyList());

        if (unprocessed.size() > 0) {
            throw new IllegalStateException("""
                    Mode: %s
                    Fatal: Found %d unprocessed transaction(s) in the upstream (%s).
                    """.formatted(mode, unprocessed.size(), providerConfiguration.provider));
        }

        if (rejected.size() > 0) {
            throw new IllegalStateException("""
                    Mode: %s
                    Fatal: Found %d rejected stored (%s) transaction(s).
                    Processing of this transaction set has been interrupted and all upstream transactions will be dropped.
                    A modification to the attribute matching is required. Until the rejected
                    transactions are removed, this account can no longer be updated as the matching would
                    constantly fail.
                                        
                    Rejected transactions:
                    %s
                                        
                    Consult the match report for detailed information about the matching output.
                    """.formatted(mode, rejected.size(), providerConfiguration.provider, outputTransactionsInTabularAsString(rejected)));
        }

        // Verify that we are not left with any un-matched stored transactions
        // as this *could* indicate a problem with the matcher or a data corruption.
        //
        // By default, left-over unmatched stored transactions should throw an error.
        //
        // There is 1 exception to the rule:
        // 1. if an un-matched stored transaction has the same date as the oldest upstream transaction,
        // then we assume that this is due to mis-alignment of the upstream and stored transaction window.
        if (peerless.size() > 0) {

            // check if the unmatched stored transactions coincides with the oldest upstream transaction
            var allPeerlessDatesEqualOldestUpstream = peerless.stream()
                    .map(unmatched -> unmatched.transaction)
                    .allMatch(isDateEqual(oldestUpstreamTransaction));

            if (allPeerlessDatesEqualOldestUpstream) {
                log.warn("""
                                Mode: {}
                                Provider: {}
                                Allowing {} un-matched (PEERLESS) stored transaction(s), for which the date coincides with the oldest upstream transaction ({}).
                                                        
                                Unmatched transactions:
                                {}
                                                        
                                Consult the match report for detailed information about the matching output.
                                """,
                        mode,
                        providerConfiguration.provider,
                        peerless.size(),
                        oldestUpstreamTransaction,
                        outputTransactionsInTabularAsString(peerless));
            } else {
                throw new IllegalStateException("""
                        Mode: %s
                        Provider: %s
                        Fatal: Found %d un-matched (PEERLESS) stored transactions
                        Processing of this transaction set has been interrupted and all upstream transactions will be dropped.
                                                
                        Unmatched transactions:
                        %s
                                                
                        Consult the match report for detailed information about the matching output.
                        """.formatted(mode, providerConfiguration.provider, peerless.size(), outputTransactionsInTabularAsString(peerless)));
            }
        }

        if (duplicates.size() > 0) {
            throw new IllegalStateException("""
                    Mode: %s
                    Provider: %s
                    Fatal: Found %d un-matched (DUPLICATE) stored transactions
                    Processing of this transaction set has been interrupted and all upstream transactions will be dropped.

                    Unmatched transactions:
                    %s

                    Consult the match report for detailed information about the matching output.
                    """.formatted(mode, providerConfiguration.provider, duplicates.size(), outputTransactionsInTabularAsString(duplicates)));
        }
    }

    /**
     * Take a transaction and check if the date is between <code>startExclusive</code> and <code>endExclusive</code>.
     * Both the start and end are exclusive.
     */
    static <T extends GeneralizedTransaction> Predicate<T> isDateBetweenExclusive(
            final LocalDate startExclusive, final LocalDate endExclusive) {
        return transaction -> transaction.getDate().isAfter(startExclusive) && transaction.getDate().isBefore(endExclusive);
    }

    static <T extends GeneralizedTransaction> Predicate<T> isDateEqual(final LocalDate date) {
        return transaction -> transaction.getDate().isEqual(date);
    }

    static List<ProviderTransactionWithId> createProviderTransactionWithId(
            final @NonNull List<ProviderGeneralizedTransaction> upstreamTransactions,
            final @NonNull TransactionIdProvider<UUID> transactionIdProvider,
            final @NonNull Transaction.FillType fillType) {
        return upstreamTransactions.stream()
                .map(transaction -> new ProviderTransactionWithId(
                        transaction.provider,
                        transactionIdProvider.generate(transaction.provider).toString(),
                        fillType))
                .collect(Collectors.toList());
    }

    static <T extends GeneralizedTransaction> Optional<LocalDate> calculateOldestTransactionDate(
            final List<T> transactions) {
        return transactions.stream()
                .map(GeneralizedTransaction::getDate)
                .min(LocalDate::compareTo);
    }

    static <T extends GeneralizedTransaction> Optional<LocalDate> calculateMostRecentTransactionDate(
            final List<T> transactions) {
        return transactions.stream()
                .map(GeneralizedTransaction::getDate)
                .max(LocalDate::compareTo);
    }

    /**
     * Reconcile two sets of transactions based on the provided {@link ProviderConfiguration}.
     * <p/>
     * The {@link EqualityAttributeTransactionMatcher#match(String, MatchResult)} is called for every matcher in the chain.
     * This is equivalent to:
     * <pre>
     *     [initial match-results]
     *        -> matcher1.match(initial match-result)
     *            -> [matcher1 match-results]
     *                -> matcher2.match(matcher1 match-result)
     *                    -> [matcher2 match-results]
     *                        -> ... etc
     *
     *   </pre>
     *
     * @param providerConfiguration      the provider configuration
     * @param bookedUpstreamTransactions the booked upstream transactions
     * @param bookedStoredTransactions   the booked stored transactions
     * @return a list of intermediary {@link MatchResult}s collected in {@link MatchResults}
     */
    public static MatchResults reconcileTransactionsOnAttributes(
            final ProviderConfiguration providerConfiguration,
            final List<ProviderGeneralizedTransaction> bookedUpstreamTransactions,
            final List<StoredGeneralizedTransaction> bookedStoredTransactions) {

        Assert.isTrue(providerConfiguration.matchers.size() > 0,
                "At least one (1) matcher is required.");
        Assert.isTrue(bookedUpstreamTransactions.stream().allMatch(transaction -> transaction.getStatus() == BOOKED),
                "This method can only operate on booked upstream transactions.");
        Assert.isTrue(bookedStoredTransactions.stream().allMatch(transaction -> transaction.getStatus() == BOOKED),
                "This method can only operate on booked stored transactions.");

        // initially all the transactions are unprocessed
        var initial = new MatchResults(
                List.of(new MatchResult(
                        emptyList(),
                        emptyList(),
                        Unmatched.unprocessed(bookedUpstreamTransactions),
                        Unmatched.unprocessed(bookedStoredTransactions))));

        // apply all the matchers left to right and collect their results
        return providerConfiguration.matchers.stream()
                .collect(foldLeft(initial, (results, matcher) -> {
                    var previousMatchResult = results.last()
                            .orElseThrow(() -> new IllegalStateException("Fatal: Previous match results not available."));
                    // if there are unmatched outputs from the last/previous matcher/stage
                    // continue processing the next matcher, otherwise skip the matcher. No need to run
                    // any other matcher if nothing is to be done.
                    if (previousMatchResult.hasUnmatched()) {
                        return results.append(matcher.match(providerConfiguration.provider, previousMatchResult));
                    } else {
                        return results;
                    }
                }));
    }

    private static void logMatchResults(
            final Mode mode,
            final String provider,
            final UUID accountId,
            final MatchResults matchResults,
            final List<ProviderGeneralizedTransaction> upstreamTransactions,
            final List<StoredGeneralizedTransaction> storedTransactions,
            final LocalDate oldestUpstreamTransactionDate,
            final LocalDate oldestStoredTransactionDateOrEpoch,
            final LocalDate mostRecentStoredTransactionDateOrEpoch,
            final SyncWindowSelector.SyncWindow syncWindow) {

        var matchResultsLogOutput = zip(matchResults.results.stream(), Stream.iterate(0, i -> i + 1))
                .map(indexedResult -> asIntermediateLog(indexedResult.getRight(), indexedResult.getLeft()))
                .collect(joining("\n\n"));

        log.info("""
                        MatchResults
                        Mode: {}
                        Sync window: {}
                        Account id: {}, provider: {},
                        Upstream total: {}, oldest: {}
                        Stored total: {}, oldest: {}, most-recent: {}
                                          
                        {}
                        """,
                mode,
                syncWindow.getLowerBound()
                        .map(LocalDate::toString)
                        .orElse("unbounded"),
                accountId,
                provider,
                upstreamTransactions.size(),
                oldestUpstreamTransactionDate,
                storedTransactions.size(),
                oldestStoredTransactionDateOrEpoch,
                mostRecentStoredTransactionDateOrEpoch,
                matchResultsLogOutput
        );
    }

    private static String asIntermediateLog(final int stage, final MatchResult matchResult) {
        if (stage == 0) { // initial unprocessed; do not output all transactions
            return """
                    MatchResult (%d)
                    Matched: %d, MultiMatched: %d, Unmatched upstream: %d, Unmatched stored: %d
                    """.formatted(
                    stage,
                    matchResult.matched.size(),
                    matchResult.multiMatched.size(),
                    matchResult.unmatchedUpstream.size(),
                    matchResult.unmatchedStored.size());
        } else {
            return """
                    MatchResult (%d)
                    Matched: %d, MultiMatched: %d, Unmatched upstream: %d, Unmatched stored: %d
                                            
                    Unmatched upstream:
                    %s
                                            
                    Unmatched stored:
                    %s
                    """.formatted(
                    stage,
                    matchResult.matched.size(),
                    matchResult.multiMatched.size(),
                    matchResult.unmatchedUpstream.size(),
                    matchResult.unmatchedStored.size(),
                    outputTransactionsInTabularAsString(matchResult.unmatchedUpstream),
                    outputTransactionsInTabularAsString(matchResult.unmatchedStored));
        }
    }

    private static <T extends GeneralizedTransaction> String outputTransactionsInTabularAsString(
            final @NonNull List<Unmatched<T>> unmatchedTransactions) {
        var unmatchedOutput = new StringBuilder();
        outputTransactionsInTabular(unmatchedTransactions, (header, groups)
                -> unmatchedOutput.append("%s\n%s".formatted(header, groups)));
        return unmatchedOutput.isEmpty() ? "None" : unmatchedOutput.toString();
    }

    private static <T extends GeneralizedTransaction> void outputTransactionsInTabular(
            final @NonNull List<Unmatched<T>> unmatchedTransactions,
            final @NonNull BiConsumer<String, String> groupOut) {

        var format = "|%1$-36s|%2$-32s|%3$-10s|%4$-10s|%5$-24s|%6$-12s|%7$-32s|%8$-6s|";
        var counter = new AtomicInteger();

        unmatchedTransactions.stream()
                .collect(groupingBy(ignored -> counter.getAndIncrement() / 50)) // output in chunks of 50
                .values()
                .forEach(group -> {
                    var tuples = group.stream()
                            .map(unmatched -> {
                                var transaction = unmatched.transaction;
                                var reason = unmatched.reason;

                                return String.format(format,
                                        Optional.ofNullable(transaction.getInternalId()).orElse("n/a"),
                                        Optional.ofNullable(transaction.getExternalId()).orElse("n/a"),
                                        transaction.getDate(),
                                        transaction.getBookingDate(),
                                        Optional.ofNullable(transaction.getTimestamp())
                                                .map(Instant::toString)
                                                .orElse("n/a"),
                                        reason,
                                        unmatched.matcher,
                                        unmatched.attributes.size()
                                );
                            })
                            .collect(Collectors.joining("\n"));

                    var header = String.format(format,
                            "transaction-id",
                            "external-id",
                            "date",
                            "booking-date",
                            "timestamp",
                            "reason",
                            "matcher",
                            "n-attr"
                    );

                    groupOut.accept(header, tuples);
                });
    }
}
