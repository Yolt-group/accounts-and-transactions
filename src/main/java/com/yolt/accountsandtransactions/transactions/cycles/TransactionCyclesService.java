package com.yolt.accountsandtransactions.transactions.cycles;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.springframework.stereotype.Service;

import java.util.*;

import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;
import static java.util.stream.Stream.concat;

@Service
@AllArgsConstructor
public class TransactionCyclesService {
    private final TransactionCycleRepository transactionCycleRepository;

    public List<TransactionCycle> getAll(final @NonNull UUID userId) {
        return transactionCycleRepository.getTransactionCycles(userId);
    }

    /**
     * Create a {@link TransactionCycleChangeSet} reflecting the changes (inserted, updated or deleted) in two sets of {@link TransactionCycle}s
     * <p/>
     * The {@link TransactionCycle}s are compared by the {@link TransactionCycle#getCycleId()}.
     * <p/>
     * The local set is used as base. The inserts, updates and deleted are relative tho this set.
     *
     * @param local    the local set used as base.
     * @param upstream the upstream new set
     * @return a {@link TransactionCycleChangeSet}
     */
    public static TransactionCycleChangeSet calculateChangeSet(
            final @NonNull Collection<TransactionCycle> local,
            final @NonNull Collection<TransactionCycle> upstream) {

        var localById = local.stream()
                .collect(toMap(TransactionCycle::getCycleId, identity(), (t1, t2) -> t1));
        var upstreamById = upstream.stream()
                .collect(toMap(TransactionCycle::getCycleId, identity(), (t1, t2) -> t1));

        /*
            difference:
            The returned set contains all elements that are contained by set1 and not contained by set2. set2 may also contain elements not present in set1

            intersection:
            The returned set contains all elements that are contained by both backing sets
         */
        return TransactionCycleChangeSet.builder()
                .added(extractValues(upstreamById, difference(upstreamById.keySet(), localById.keySet())))
                .updated(extractValues(upstreamById, intersection(localById.keySet(), upstreamById.keySet()))) // take updated cycle from upstreamById
                .deleted(extractValues(localById, difference(localById.keySet(), upstreamById.keySet())))
                .build();
    }

    /**
     * Extracts the values from a Map<K, T>, given a subset of Ks, as List<T>
     * <p/>
     * Note: the set of <code>ids</code> <b>must</b> be a subset of the keys in the <code>map</code>.
     *
     * @param superset the superset of elements identified by K
     * @param subset   the subset
     * @param <T>      T as value type
     * @return a list of values T
     */
    private static <K, T> Set<T> extractValues(final @NonNull Map<K, T> superset, final @NonNull Set<K> subset) {

        if (difference(subset, superset.keySet()).size() > 0) {
            throw new IllegalArgumentException("The subset includes element which are not contained in the superset.");
        }

        return subset.stream()
                .map(superset::get) // subset check prevent null
                .collect(toSet());
    }

    /**
     * Synchronize the database according to the given {@see TransactionCycleChangeSet}. In this implementation the
     * {@link TransactionCycle}s are not deleted, but merely marked as expired by a flag.
     *
     * @param changeSet the change-set to synchronize
     */
    public void reconsile(final @NonNull TransactionCycleChangeSet changeSet) {

        // assertion
        boolean addedOrUpdatedIsExpired = changeSet.addedAndUpdated()
                .stream()
                .anyMatch(TransactionCycle::isExpired);
        if (addedOrUpdatedIsExpired) {
            throw new IllegalArgumentException("Transaction cycles which are to be inserted or updated cannot be marked as expired.");
        }

        var toAddOrUpdate = changeSet.addedAndUpdated().stream();

        var toExpire = changeSet.deleted().stream()
                .map(transactionCycle -> transactionCycle.toBuilder()
                        .expired(true)
                        .build());

        saveBatch(concat(toAddOrUpdate, toExpire).collect(toList()));
    }

    /**
     * See {@link TransactionCycleRepository#upsert(TransactionCycle)}
     *
     * @param transactionCycle the transaction-cycle to upsert
     * @return a future
     */
    public TransactionCycle upsert(final @NonNull TransactionCycle transactionCycle) {
        return transactionCycleRepository.upsert(transactionCycle);
    }

    /**
     * See {@link TransactionCycleRepository#expire(UUID, UUID)} (TransactionCycle)}
     *
     * @param userId  the user-id owning the {@link TransactionCycle}
     * @param cycleId the cycle-id identifying the {@link TransactionCycle}
     */
    public void expire(UUID userId, UUID cycleId) {
        transactionCycleRepository.expire(userId, cycleId);
    }

    public void saveBatch(Collection<TransactionCycle> transactionCycles) {
        transactionCycleRepository.saveBatch(List.copyOf(transactionCycles), 100);
    }

    public Optional<TransactionCycle> find(UUID userId, UUID cycleId) {
        return transactionCycleRepository.findTransactionCycle(userId, cycleId);
    }

    public void deleteCycle(final @NonNull UUID userId, final @NonNull UUID cycleId) {
        transactionCycleRepository.deleteCycle(userId, cycleId);
    }
}

