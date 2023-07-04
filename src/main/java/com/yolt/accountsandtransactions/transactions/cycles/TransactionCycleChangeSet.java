package com.yolt.accountsandtransactions.transactions.cycles;

import lombok.*;

import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

@Builder
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class TransactionCycleChangeSet {

    @NonNull
    private final Set<TransactionCycle> added;

    @NonNull
    private final Set<TransactionCycle> updated;

    @NonNull
    private final Set<TransactionCycle> deleted;

    public Set<TransactionCycle> addedAndUpdated() {
        return concat(added.stream(), updated.stream()).collect(toSet());
    }

    public Set<TransactionCycle> deleted() {
        return deleted;
    }
}
