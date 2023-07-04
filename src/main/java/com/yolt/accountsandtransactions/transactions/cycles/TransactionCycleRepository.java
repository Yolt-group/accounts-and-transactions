package com.yolt.accountsandtransactions.transactions.cycles;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.cassandra.CassandraRepository;
import org.springframework.stereotype.Repository;
import org.springframework.validation.annotation.Validated;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

@Validated
@Repository
@Slf4j
public class TransactionCycleRepository extends CassandraRepository<TransactionCycle> {
    protected TransactionCycleRepository(Session session) {
        super(session, TransactionCycle.class);

        setAuditLoggingEnabled(false);
    }

    public List<TransactionCycle> getTransactionCycles(final @NonNull UUID userId) {
        return select(createSelect().where(eq("user_id", userId)));
    }

    public Optional<TransactionCycle> findTransactionCycle(final @NonNull UUID userId, final @NonNull UUID cycleId) {
        final Select selectQuery = createSelect();
        selectQuery.where(eq("user_id", userId))
                .and(eq("cycle_id", cycleId));
        return selectOne(selectQuery);
    }

    /**
     * Mark as {@link TransactionCycle} as expired.
     * <p/>
     * Note: Do *not* make this method public. All calls should be made through the {@link TransactionCyclesService#expire(UUID, UUID)}
     *
     * @param userId  the user-id owning the {@link TransactionCycle}
     * @param cycleId the cycle-id identifing the {@link TransactionCycle}
     */
    void expire(final @NonNull UUID userId, final @NonNull UUID cycleId) {
        var update = createUpdate();
        update.with(set("expired", true))
                .where(eq("user_id", userId))
                .and(eq("cycle_id", cycleId));
        executeUpdate(update);
    }

    /**
     * Upsert a {@link TransactionCycle} to the database
     * <p/>
     * Note: Do *not* make this method public. All calls should be made through the {@link TransactionCyclesService#upsert(TransactionCycle)}
     *
     * @param transactionCycle the transaction-cycle to persis
     * @return a future
     */
    TransactionCycle upsert(final @NonNull TransactionCycle transactionCycle) {
        save(transactionCycle);
        return transactionCycle;

    }

    /**
     * Delete a {@link TransactionCycle} from the local keyspace.
     * <p/>
     * Note: This method exists solely for the purpose of purging user related data.
     * Use {@link TransactionCycleRepository#expire(UUID, UUID)} if you want to mark a {@link TransactionCycle}
     * as expired (e.a. not longer in use of erroneous detection)
     *
     * @param userId  the user-id owning the {@link TransactionCycle}
     * @param cycleId the cycle-id identifing the {@link TransactionCycle}
     */
    public void deleteCycle(final @NonNull UUID userId, final @NonNull UUID cycleId) {
        var delete = super.createDelete();
        delete.where(eq("user_id", userId))
                .and(eq("cycle_id", cycleId));
        executeDelete(delete);
    }


}
