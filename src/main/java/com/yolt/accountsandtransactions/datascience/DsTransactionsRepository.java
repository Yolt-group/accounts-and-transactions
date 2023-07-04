package com.yolt.accountsandtransactions.datascience;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.Mapper;
import com.yolt.accountsandtransactions.transactions.TransactionService.TransactionPrimaryKey;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.cassandra.CassandraRepository;
import nl.ing.lovebird.cassandra.codec.LocalDateTypeCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.yolt.accountsandtransactions.datascience.PendingType.PENDING;
import static com.yolt.accountsandtransactions.datascience.PendingType.REGULAR;
import static org.apache.commons.collections4.ListUtils.partition;

/**
 * Do **NOT** use the {@link Session} that is available on the {@link ApplicationContext}.
 */
@Repository
@Slf4j
public class DsTransactionsRepository extends CassandraRepository<DsTransaction> {
    private final LocalDateTypeCodec localDateTypeCodec = new LocalDateTypeCodec();

    private final int batchSize;

    @Autowired
    public DsTransactionsRepository(
            final DataScienceCassandraSession session,
            @Value("${lovebird.accounts-and-transactions.batch-size:500}") final int batchSize) {
        super(session.getSession(), DsTransaction.class);
        this.batchSize = batchSize;
    }

    public void saveTransactionBatch(final List<DsTransaction> transactions) {
        // Using unlogged batch here because all transactions have the same userId, thus they will be written
        // to the same partition
        // This improves performance 2-3 times in comparison with saving each transaction using a separate save() call
        //
        // For large number of transactions it is necessary to do writes in several batches

        partition(transactions, batchSize).forEach(subBatch -> {
            BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);

            subBatch.forEach(t -> {
                // Avoid writing NULL's to C* to prevent creation of too many tombstones
                // Accepting a small risk that a previously non-NULL value would not be overwritten
                Statement saveStatement = mapper.saveQuery(t, Mapper.Option.saveNullFields(false));
                saveStatement.setConsistencyLevel(writeConsistency);
                batch.add(saveStatement);
            });

            session.execute(batch);

            log.debug("Saved a batch of {} transactions", subBatch.size());
        });

        // Temporarily log the number of saved transactions to troubleshoot lingering transactions in the DS
        // transactions table after user deletion (YCO-1917). Once fixed, this logging will be removed again.
        log.info("Saved {} transactions in the DS keyspace", transactions.size());
    }

    public void deleteTransactions(final List<DsTransaction> transactions) {
        transactions.forEach(this.mapper::delete);

        log.debug("Deleted {} transactions", transactions.size());
    }

    public List<DsTransaction> getTransactionsForUser(final UUID userId) {
        Select select = getTransactionForUserQuery(userId);
        return select(select);
    }

    public Select getTransactionForUserQuery(UUID userId) {
        Select select = super.createSelect();
        select.where(eq(DsTransaction.USER_ID_COLUMN, userId));
        return select;
    }

    public List<DsTransaction> getTransactionsInAccountFromDate(final UUID userId, final UUID accountId, final String date) {
        Select selectRegular = super.createSelect();
        selectRegular.where(eq(DsTransaction.USER_ID_COLUMN, userId))
                .and(QueryBuilder.in(DsTransaction.PENDING_COLUMN, PENDING, REGULAR))
                .and(eq(DsTransaction.ACCOUNT_ID_COLUMN, accountId))
                .and(gte(DsTransaction.DATE_COLUMN, date));
        return select(selectRegular);
    }

    public Mapper<DsTransaction> getMapper() {
        return mapper;
    }

    /**
     * Due to legacy it returns you the dates as string in format 'yyyy-MM'
     */
    public Stream<String> getDatesPendingTransactions(final UUID userId, final List<UUID> accountIds) {
        Select select = QueryBuilder
                .select(DsTransaction.DATE_COLUMN)
                .from(DsTransaction.TABLE_NAME);
        select.where(eq(DsTransaction.USER_ID_COLUMN, userId))
                .and(eq(DsTransaction.PENDING_COLUMN, PENDING))
                .and(QueryBuilder.in(DsTransaction.ACCOUNT_ID_COLUMN, accountIds));

        ResultSet resultSet = session.execute(select);
        return resultSet.all().stream().map(row -> row.getString(0).substring(0, 7));
    }

    public void deleteTransactionsForAccount(final UUID userId, final UUID accountId) {
        var delete = QueryBuilder.delete()
                .from(DsTransaction.TABLE_NAME)
                .where(eq(DsTransaction.USER_ID_COLUMN, userId))
                .and(QueryBuilder.in(DsTransaction.PENDING_COLUMN, PENDING, REGULAR))
                .and(eq(DsTransaction.ACCOUNT_ID_COLUMN, accountId));

        session.execute(delete);
    }

    /**
     * Delete a batch (unlogged) of {@link com.yolt.accountsandtransactions.transactions.Transaction} identified by their primary key.
     * <p/>
     * Deletes are batch according to {@see batchSize}
     *
     * @param transactionPrimaryKeys the transactions to delete identified by their primary key
     */
    public void deleteSpecificTransactions(final @NonNull List<TransactionPrimaryKey> transactionPrimaryKeys) {

        partition(transactionPrimaryKeys, batchSize)
                .forEach(subBatch -> {
                    BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);

                    subBatch.forEach(t -> {
                        Delete delete = super.createDelete();
                        delete.where(eq("user_id", t.getUserId()));
                        delete.where(eq("pending", PendingType.of(t.getStatus())));
                        delete.where(eq("account_id", t.getAccountId()));
                        delete.where(eq("date", localDateTypeCodec.format(t.getDate())));
                        delete.where(eq("transaction_id", t.getId()));
                        batch.add(delete);
                    });

                    session.execute(batch);

                    log.debug("Deleted a batch of {} datascience transactions", subBatch.size());
                });
    }
}
