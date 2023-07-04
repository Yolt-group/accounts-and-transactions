package com.yolt.accountsandtransactions.inputprocessing.matching;

import com.yolt.accountsandtransactions.Predef;
import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction.ProviderGeneralizedTransaction;
import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction.StoredGeneralizedTransaction;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.util.Assert;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;


public interface AttributeTransactionMatcher {

    /**
     * Take a provider(-name) and {@see MatchResults} and transform this into a new {@see MatchResults}.
     *
     * @param provider            the provider name (e.a. RABOBANK)
     * @param previousMatchResult the match results defined as input; this can be used to chain transaction matchers.
     * @return the new {@see MatchResults}
     */
    MatchResult match(final String provider, final MatchResult previousMatchResult);

    String name();

    /**
     * The {@link AttributeExtractor} is responsible for the attribute/field extraction(s) from a {@link GeneralizedTransaction}
     *
     * @param <A> the type of the attribute value
     */
    @FunctionalInterface
    interface AttributeExtractor<A> {

        Attribute<A> extract(final @NonNull GeneralizedTransaction upstreamTransaction);
    }

    interface TransactionsExtractable {
        List<? extends GeneralizedTransaction> extractTransactions();

        default boolean containsExactlyNumberOfElements(final @NonNull TransactionsExtractable other) {
            return extractTransactions().size() == other.extractTransactions().size();
        }
    }

    @EqualsAndHashCode
    @RequiredArgsConstructor
    class Matched implements TransactionsExtractable {

        /**
         * The identifier/ name of the matcher which generated this entry.
         */
        @NonNull
        public final String matcher;
        /**
         * The upstream transaction the stored transaction was matched to.
         */
        @NonNull
        public final ProviderGeneralizedTransaction upstreamTransaction;
        /**
         * The stored transaction the upstream transaction was matched to.
         */
        @NonNull
        public final StoredGeneralizedTransaction storedTransaction;
        /**
         * The set of attributes (key) the transactions where matched on.
         */
        @NonNull
        public final Set<? extends Attribute<?>> attributes;

        @Override
        public List<? extends GeneralizedTransaction> extractTransactions() {
            return List.of(upstreamTransaction, storedTransaction);
        }

        @Override
        public String toString() {
            return "Matched(n-upstream=1, n-stored=1, n-attributes=%d)".formatted(attributes.size());
        }
    }

    @EqualsAndHashCode
    class MultiMatched implements TransactionsExtractable {

        /**
         * The identifier/ name of the matcher which generated this entry.
         */
        @NonNull
        public final String matcher;
        /**
         * The upstream transaction the stored transaction was matched to.
         */
        @NonNull
        public final ProviderGeneralizedTransaction upstreamTransaction;
        /**
         * The stored transactions the upstream transaction was matched to.
         */
        @NonNull
        public final List<StoredGeneralizedTransaction> storedTransactions;
        /**
         * The set of attributes (key) the transactions were matched on.
         */
        @NonNull
        public final Set<? extends Attribute<?>> attributes;

        public MultiMatched(@NonNull String matcher,
                            @NonNull ProviderGeneralizedTransaction upstreamTransaction,
                            @NonNull List<StoredGeneralizedTransaction> storedTransactions,
                            @NonNull Set<? extends Attribute<?>> attributes) {

            Assert.isTrue(attributes.size() >= 1,
                    "At least one (1) attribute has to be present.");

            Assert.isTrue(storedTransactions.size() >= 2,
                    "At least two (2) stored transactions has to be present.");

            this.matcher = matcher;
            this.upstreamTransaction = upstreamTransaction;
            this.storedTransactions = storedTransactions;
            this.attributes = attributes;
        }

        @Override
        public List<? extends GeneralizedTransaction> extractTransactions() {
            return Predef.concat(List.of(upstreamTransaction), storedTransactions);
        }

        @Override
        public String toString() {
            return "MultiMatched(n-upstream=1, n-stored=%d, n-attributes=%d)"
                    .formatted(storedTransactions.size(), attributes.size());
        }
    }

    @EqualsAndHashCode
    class Unmatched<T extends GeneralizedTransaction> implements TransactionsExtractable {

        public enum Reason {
            /**
             * The transaction associated with this reason could not get *matched* to a peer/counterpart
             * but was unique identified based on the attributes
             */
            PEERLESS(3),
            /**
             * The transaction associated with this reason is *deemed a duplicate*
             * because 1 or more other transactions got matched to the same set of attributes (key).
             */
            DUPLICATE(2),
            /**
             * The transaction associated with this reason could not be *used* in the matching algorithm
             * because at least one attribute selectors was not accepted.
             */
            REJECTED(1),
            /**
             * The transaction associated with this reason has not been processed and contains no attributes;
             * This is <b>not</b> a valid end-state and only used to bootstrap the matching process.
             */
            UNPROCESSED(0);

            final int weight;

            Reason(int weight) {
                this.weight = weight;
            }

            public boolean hasEqualOrLowerWeightThan(final Reason reason) {
                return this.weight <= reason.weight;
            }

            public boolean hasHigherWeightThan(final Reason reason) {
                return this.weight > reason.weight;
            }
        }

        /**
         * The identifier/ name of the matcher which generated this entry.
         */
        @NonNull
        public final String matcher;

        /**
         * The unmatched transaction
         */
        @NonNull
        public final T transaction;
        /**
         * The set of attributes (key) that was used when the transaction failed to get matched.
         */
        @NonNull
        public final Set<? extends Attribute<?>> attributes;

        /**
         * The reason the transaction got unmatched.
         */
        @NonNull
        public final Reason reason;

        public Unmatched(@NonNull String matcher, @NonNull T transaction, @NonNull Set<? extends Attribute<?>> attributes, @NonNull Reason reason) {
            Assert.isTrue(attributes.size() >= 1 || reason == Reason.UNPROCESSED,
                    "Attributes can only be empty when the transaction is marked unprocessed.");

            this.matcher = matcher;
            this.transaction = transaction;
            this.attributes = attributes;
            this.reason = reason;
        }

        @Override
        public List<? extends GeneralizedTransaction> extractTransactions() {
            return List.of(transaction);
        }

        @Override
        public String toString() {
            return "Unmatched(n-transactions=1, n-attributes=%d, reason=%s)"
                    .formatted(attributes.size(), reason);
        }

        /**
         * Create unmatched output with reason {@see REJECTED}.
         */
        public static <T extends GeneralizedTransaction> List<Unmatched<T>> unprocessed(final Collection<T> collection) {
            return collection.stream()
                    .map(t -> new Unmatched<>("UNPROCESSED", t, emptySet(), Reason.UNPROCESSED))
                    .collect(toList());
        }
    }

    @EqualsAndHashCode
    @RequiredArgsConstructor
    class MatchResult implements TransactionsExtractable {

        public static final MatchResult EMPTY = new MatchResult(emptyList(), emptyList(), emptyList(), emptyList());

        /**
         * The list of uniquely matched transactions
         */
        @NonNull
        public final List<Matched> matched;

        /**
         * A list of single upstream transaction matched against two or more stored transactions e.a. duplicates.
         */
        @NonNull
        public final List<MultiMatched> multiMatched;

        /**
         * The list of transactions from the upstream which did not get matched to a stored peer/ counterpart.
         */
        @NonNull
        public final List<Unmatched<ProviderGeneralizedTransaction>> unmatchedUpstream;

        /**
         * The list of transactions from the storage which did not get matched to an upstream peer/ counterpart.
         */
        @NonNull
        public final List<Unmatched<StoredGeneralizedTransaction>> unmatchedStored;

        @Override
        public List<? extends GeneralizedTransaction> extractTransactions() {
            return extractTransactionsFlattened(matched, multiMatched, unmatchedUpstream, unmatchedStored);
        }

        public boolean hasUnmatched() {
            return unmatchedUpstream.size() > 0 || unmatchedStored.size() > 0;
        }

        @Override
        public String toString() {
            return "MatchResults(n-matched=%d, n-multi-matched=%d, unmatched-upstream=%d, unmatched-stored=%d)"
                    .formatted(matched.size(), multiMatched.size(), unmatchedUpstream.size(), unmatchedStored.size());
        }
    }

    /**
     * Aggregates a number of {@link MatchResult} which are emitted for every matcher.
     */
    @EqualsAndHashCode
    class MatchResults {

        @NonNull
        public final List<MatchResult> results;

        public MatchResults(@NonNull final List<MatchResult> results) {
            Assert.isTrue(results.size() > 0,
                    "At least one (1) match result is required.");
            this.results = new ArrayList<>(results);
        }

        public MatchResults append(final MatchResult matchResult) {
            boolean containsExactlyNumberOfTransactionsInPrevious = last()
                    .map(result -> result.containsExactlyNumberOfElements(matchResult))
                    .orElse(false);

            Assert.isTrue(containsExactlyNumberOfTransactionsInPrevious,
                    """
                            Every matchResult is required to have the same number of transactions as the MatchResult
                            preceding it in the chain.
                            Removing or adding transactions during the matching process is not allowed.
                            This indicates a programmer error.
                            Make sure you return all the transactions and no more or less in every matching stage.
                            """);

            return new MatchResults(Predef.append(results, matchResult));
        }

        public Optional<MatchResult> head() {
            return Predef.head(results);
        }

        public Optional<MatchResult> last() {
            return Predef.last(results);
        }
    }

    @SafeVarargs
    static List<? extends GeneralizedTransaction> extractTransactionsFlattened(final List<? extends TransactionsExtractable>... extractables) {
        return Arrays.stream(extractables)
                .flatMap(Collection::stream)
                .flatMap(extractable -> extractable.extractTransactions().stream())
                .collect(Collectors.toList());
    }
}
