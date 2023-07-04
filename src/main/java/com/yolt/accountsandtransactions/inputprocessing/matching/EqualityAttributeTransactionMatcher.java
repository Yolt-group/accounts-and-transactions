package com.yolt.accountsandtransactions.inputprocessing.matching;

import com.google.common.annotations.VisibleForTesting;
import com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher.Unmatched.Reason;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;
import static com.yolt.accountsandtransactions.Predef.*;
import static com.yolt.accountsandtransactions.Predef.Streams.partitioned;
import static com.yolt.accountsandtransactions.inputprocessing.matching.AttributeSelector.Result.isUsable;
import static com.yolt.accountsandtransactions.inputprocessing.matching.AttributeTransactionMatcher.Unmatched.Reason.*;
import static java.util.stream.Collectors.*;

/**
 * Match a set of upstream transactions against a set of stored transactions based on a set of attributes representing a functional key.
 * <p/>
 * This function only operates on <b>unique</b> transaction sets.
 * <p/>
 * Prior to matching, the following transactions are excluded from <b>both</b> sets:
 * <ul>
 *     <li>transactions for which at least 1 (one) attribute selector is not accepted (rejected).</li>
 *     <li>transactions for which at least 1 (one) duplicate exists.</li>
 * </ul>
 * <p/>
 * This implementation implements a type of voting mechanism.
 * The {@link EqualityAttributeTransactionMatcher#match(String, MatchResult)} receives the {@link MatchResult}
 * from the previous matcher in the chain.
 * <p/>
 * If a transaction/ candidate was not matched in a previous matcher/ stage and the
 * match {@link Reason} is of lower importance then the match {@link Reason} we determined in this matcher, then we output the
 * reason from current matcher.
 * If the reason in the previous matcher was determined to be of higher importance
 * then we determined the match reason to be in the current matcher, we propagate the previous reason.
 * <p/>
 * Example 1: If matcher 1 determine Transaction A to be a <b>duplicate</b>, but matcher 2 determined it to be PEERLESS,
 * then we propagate PEERLESS.
 * Example 2: If matcher 1 determine Transaction A to be a <b>peerless</b>, but matcher 2 determined it to be DUPLICATE,
 * then we propagate PEERLESS
 */
@Slf4j
public class EqualityAttributeTransactionMatcher implements AttributeTransactionMatcher {

    @NonNull
    private final String name;
    @NonNull
    private final List<AttributeSelector<?>> selectors;

    public EqualityAttributeTransactionMatcher(@NonNull final String name, @NonNull final List<AttributeSelector<?>> selectors) {
        Assert.notEmpty(selectors,
                "Attribute selectors must contain at least one (1) selector.");

        this.name = name;
        this.selectors = selectors;
    }

    @Override
    public MatchResult match(final @NonNull String provider,
                             final @NonNull MatchResult previousMatchResult) {

        var previousUnmatchedUpstream = previousMatchResult.unmatchedUpstream;
        var previousUnmatchedStored = previousMatchResult.unmatchedStored;

        // Handle rejections
        var upstreamRejectedAndCandidates = determineRejectedAndCandidates(previousUnmatchedUpstream);
        var upstreamNonRejectedCandidates = upstreamRejectedAndCandidates.getRight();
        var upstreamRejectedCandidates = upstreamRejectedAndCandidates.getLeft();

        var storedRejectedAndCandidates = determineRejectedAndCandidates(previousUnmatchedStored);
        var storedNonRejectedCandidates = storedRejectedAndCandidates.getRight();
        var storedRejectedCandidates = storedRejectedAndCandidates.getLeft();

        // Handle duplicates
        var upstreamDuplicateAndCandidates = determineDuplicateAndCandidates(upstreamNonRejectedCandidates);
        var upstreamNonDuplicateCandidates = upstreamDuplicateAndCandidates.getRight();
        var upstreamDuplicateCandidates = upstreamDuplicateAndCandidates.getLeft();

        var storedDuplicateAndCandidates = determineDuplicateAndCandidates(storedNonRejectedCandidates);
        var storedNonDuplicateCandidates = storedDuplicateAndCandidates.getRight();
        var storedDuplicateCandidates = storedDuplicateAndCandidates.getLeft();

        var uniqueUpstreamTransactions = upstreamNonDuplicateCandidates.stream()
                .collect(toMap(candidate -> candidate.attributes, candidate -> candidate.transaction));
        var uniqueStoredTransactions = storedNonDuplicateCandidates.stream()
                .collect(toMap(candidate -> candidate.attributes, candidate -> candidate.transaction));

        // Match unique transactions based on the Set<Attributes<T>> key

        /*
         * The returned set contains all elements that are contained by both backing sets. The iteration order of the returned set matches that of set1.
         */
        var matchedAttributes = intersection(uniqueUpstreamTransactions.keySet() /* set1 */, uniqueStoredTransactions.keySet() /* set2 */); // matched
        var matches = matchedAttributes.stream()
                .map(attributes -> new Matched(name(), uniqueUpstreamTransactions.get(attributes), uniqueStoredTransactions.get(attributes), attributes))
                .collect(toList());

        /*
         * The returned set contains all elements that are contained by {@code set1} and
         * not contained by {@code set2}. {@code set2} may also contain elements not
         * present in {@code set1}; these are simply ignored. The iteration order of
         * the returned set matches that of {@code set1}.
         */
        var unmatchedUpstreamAttributes = difference(uniqueUpstreamTransactions.keySet() /* set1 */, uniqueStoredTransactions.keySet() /* set 2 */); // unmatched upstream
        var unmatchedStoredAttributes = difference(uniqueStoredTransactions.keySet() /* set1 */, uniqueUpstreamTransactions.keySet() /* set 2 */); // unmatched stored

        var unmatchedUpstream = unmatchedUpstreamAttributes.stream()
                .map(attributes -> new Unmatched<>(name(), uniqueUpstreamTransactions.get(attributes), attributes, PEERLESS))
                .collect(toList());

        var unmatchedStored = unmatchedStoredAttributes.stream()
                .map(attributes -> new Unmatched<>(name(), uniqueStoredTransactions.get(attributes), attributes, PEERLESS))
                .collect(toList());

        return new MatchResult(
                concatAll(previousMatchResult.matched, matches), // return previous matches + current matches
                previousMatchResult.multiMatched, // return previous multi-matched
                concatAll(unmatchedUpstream, upstreamRejectedCandidates, upstreamDuplicateCandidates), // return unmatched upstream + excluded
                concatAll(unmatchedStored, storedRejectedCandidates, storedDuplicateCandidates) // returned unmatched stored + excluded
        );
    }

    private <T extends GeneralizedTransaction> Pair<List<Unmatched<T>>, List<Candidate<T>>> determineRejectedAndCandidates(
            final List<Unmatched<T>> unmatched) {
        return determineUnmatchedAndCandidates(partitionOnMatcherUsability(unmatched, selectors), REJECTED);
    }

    private <T extends GeneralizedTransaction> Pair<List<Unmatched<T>>, List<Candidate<T>>> determineDuplicateAndCandidates(
            final List<Candidate<T>> candidates) {
        return determineUnmatchedAndCandidates(partitionOnUnique(candidates), DUPLICATE);
    }

    private <T extends GeneralizedTransaction> Pair<List<Unmatched<T>>, List<Candidate<T>>> determineUnmatchedAndCandidates(
            final Partitioned<List<Candidate<T>>> candidates,
            final Reason reason) {

        var eligible = candidates.included;
        var ineligible = candidates.excluded;

        // The previous matcher in the chain voted a reason for this candidate which was equal or
        // of lower weight the current matcher -> emit a new Unmatched output with reason
        var ineligibleAndPreviousReasonLessImportant = ineligible.stream()
                .filter(candidate -> candidate.hasPreviousReasonEqualOrLowerWeight(reason))
                .map(candidate -> new Unmatched<>(name(), candidate.transaction, candidate.attributes, reason)) // resleeve transaction
                .collect(toList());

        // The previous matcher in the chain voted a reason for this candidate which was
        // of higher weight the current matcher -> propagate the reason from the previous matcher.
        var ineligibleAndPreviousReasonMoreImportant = ineligible.stream()
                .filter(candidate -> candidate.hasPreviousReasonHigherWeight(reason))
                .map(EqualityAttributeTransactionMatcher::unwrapPreviouslyUnmatched)
                .collect(toList());

        // left = NOK, right = OK
        return Pair.of(
                concat(
                        ineligibleAndPreviousReasonLessImportant,
                        ineligibleAndPreviousReasonMoreImportant
                ),
                eligible
        );
    }

    @Override
    public String name() {
        return name;
    }

    @VisibleForTesting
    @NonNull List<AttributeSelector<?>> getSelectors() {
        return selectors;
    }

    public <A> EqualityAttributeTransactionMatcher withoutSelector(final String postfix, final AttributeSelector<A> selector) {
        var newSelectors = selectors.stream()
                .filter(attributeSelector -> attributeSelector != selector)
                .collect(toList());
        return new EqualityAttributeTransactionMatcher("%s-%s".formatted(name, postfix), newSelectors);
    }

    private static <T extends GeneralizedTransaction> Unmatched<T> unwrapPreviouslyUnmatched(final Candidate<T> candidate) {
        return candidate.previousResult;
    }

    @VisibleForTesting
    @RequiredArgsConstructor
    static class Candidate<T extends GeneralizedTransaction> {
        /**
         * The attributes as determined by the current matcher;
         */
        @NonNull
        public final Set<? extends Attribute<?>> attributes;

        /**
         * The transaction to match.
         */
        @NonNull
        public final T transaction;

        /**
         * Indicates if the set of extracted attributes can be used in the matching process.
         */
        @NonNull
        public final Boolean usable;

        /**
         * The {@link Unmatched} match results as determined by the previous matcher.
         */
        @NonNull
        public final Unmatched<T> previousResult;

        public boolean hasPreviousReasonEqualOrLowerWeight(final Reason reference) {
            return this.previousResult.reason.hasEqualOrLowerWeightThan(reference);
        }

        public boolean hasPreviousReasonHigherWeight(final Reason reference) {
            return this.previousResult.reason.hasHigherWeightThan(reference);
        }
    }

    /**
     * Partition the list of up stream transactions in to sets:
     * <ul>
     *     <li>One that only contains the upstream transactions (as {@link Candidate}s) which can be used by the {@link AttributeSelector}s</li>
     *     <li>One that only contains the upstream transactions (as {@link Candidate}s) which cannot be used by the {@link AttributeSelector}s</li>
     * </ul>
     *
     * @param upstreamTransactions the list of upstream transactions as {@link Unmatched} to partition
     * @param selectors            the {@link AttributeSelector}s to test
     * @return a {@link Partitioned} result with maps {@link Partitioned#included} to usable upstream transactions and
     * the {@link Partitioned#excluded} to unusable upstream transactions.
     */
    private static <T extends GeneralizedTransaction> Partitioned<List<Candidate<T>>> partitionOnMatcherUsability(
            final List<Unmatched<T>> upstreamTransactions,
            final List<AttributeSelector<?>> selectors) {

        return upstreamTransactions.stream()
                .map(unmatched -> selectors.stream()
                        .flatMap(attributeSelector -> attributeSelector.selectAttribute(unmatched.transaction).stream())
                        .collect(collectingAndThen(toSet(), results -> new Candidate<T>(extractAttributes(results), unmatched.transaction, isUsable(results), unmatched))))
                .collect(partitioned(candidate -> candidate.usable));
    }

    /**
     * Partition a list of {@link Candidate}s in two sets:
     * <ul>
     *     <li>One that only contains duplicate transactions</li>
     *     <li>One that only contains unique transactions</li>
     * </ul>
     *
     * @param candidates the {@link Candidate}s to partition
     * @param <T>        a {@link GeneralizedTransaction}
     * @return a {@link Partitioned} result with maps {@link Partitioned#included} to unique candidates and
     * the {@link Partitioned#excluded} to duplicate candidates.
     */
    private <T extends GeneralizedTransaction> Partitioned<List<Candidate<T>>> partitionOnUnique(
            final List<Candidate<T>> candidates) {

        var candidatesGroupedByAttributes = candidates.stream()
                .collect(groupingBy(candidate -> candidate.attributes, toList()));

        var uniqueCandidates = candidatesGroupedByAttributes.values().stream()
                .filter(intermediate -> intermediate.size() == 1)
                .flatMap(Collection::stream)
                .collect(toList());

        var nonUniqueCandidates = candidatesGroupedByAttributes.values().stream()
                .filter(intermediate -> intermediate.size() > 1)
                .flatMap(Collection::stream)
                .collect(toList());

        return Partitioned.<List<Candidate<T>>>builder()
                .included(uniqueCandidates)
                .excluded(nonUniqueCandidates)
                .build();
    }

    private static Set<? extends Attribute<?>> extractAttributes(final Set<? extends AttributeSelector.Result<?>> results) {
        return results.stream()
                .map(result -> result.attribute)
                .collect(Collectors.toSet());
    }

}
