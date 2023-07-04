package com.yolt.accountsandtransactions.transactions;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.mapping.Mapper;
import com.google.common.annotations.VisibleForTesting;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.cassandra.CassandraRepository;
import nl.ing.lovebird.cassandra.codec.LocalDateTypeCodec;
import com.yolt.accountsandtransactions.datetime.DateInterval;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Repository;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.BOOKED;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.PENDING;

@Validated
@Repository
@Slf4j
public class TransactionRepository extends CassandraRepository<Transaction> {

    private final int batchSize;
    private final int fetchSizeForInternalSummary;
    private final TransactionPager<Transaction> pager;
    private final LocalDateTypeCodec localDateTypeCodec = new LocalDateTypeCodec();

    protected TransactionRepository(Session session,
                                    @Value("${lovebird.accounts-and-transactions.batch-size:500}") final int batchSize,
                                    @Value("${yolt.transactions.internal-summary.fetch-size:2000}") int fetchSizeForInternalSummary) {
        super(session, Transaction.class);
        setAuditLoggingEnabled(false);

        this.batchSize = batchSize;
        this.fetchSizeForInternalSummary = fetchSizeForInternalSummary;
        this.pager = new TransactionPager<>(session, Transaction.class);
    }

    public List<Transaction> getTransactionsForUser(UUID userId) {
        Select select = getTransactionsForUserQuery(userId);
        return select(select);
    }

    public Select getTransactionsForUserQuery(UUID userId) {
        Select select = QueryBuilder.select()
                .from("transactions");
        select.where(eq("user_id", userId));
        return select;
    }

    public void deleteSpecificTransactions(final @NonNull List<TransactionService.TransactionPrimaryKey> transactionsToDelete) {
        BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);

        transactionsToDelete.forEach(t -> {
            Delete delete = super.createDelete();
            delete.where(eq("user_id", t.getUserId()));
            delete.where(eq("account_id", t.getAccountId()));
            delete.where(eq("date", localDateTypeCodec.format(t.getDate())));
            delete.where(eq("id", t.getId()));
            batch.add(delete);
        });

        session.execute(batch);

        log.debug("Deleted a batch of {} transactions", transactionsToDelete.size());
    }

    TransactionsPage getPageOfTransactionsForAccounts(@NonNull UUID userId, @NonNull List<UUID> accountIds, @Nullable String pagingState) {
        var select = QueryBuilder.select().from("transactions");

        select.setFetchSize(100);

        select.where(eq("user_id", userId))
                .and(in("account_id", accountIds));

        return pager.getPage(select, pagingState);
    }

    public void deleteAllTransactionsForAccounts(UUID userId, List<UUID> accountIds) {
        Delete delete = super.createDelete();
        delete.where(eq("user_id", userId)).and(in("account_id", accountIds));
        executeDelete(delete);
    }

    public void upsert(@Valid List<Transaction> transactions) {
        super.saveBatch(transactions, batchSize);
    }

    public TransactionsPage get(@NonNull UUID userId, @NonNull List<UUID> accountIds, @NonNull DateInterval interval, @Nullable String pagingState, int pageSize) {

        Select select = QueryBuilder.select()
                .from("transactions");

        select.setFetchSize(pageSize);

        select.where(eq("user_id", userId))
                .and(in("account_id", accountIds))
                .and(gte("date", interval.getStartFormatted()))
                .and(lte("date", interval.getEndFormatted()));

        return pager.getPage(select, pagingState);
    }

    /**
     * Retrieve, for a given account, the status and timestamp for all transactions in the window [onOrAfterDate, now)
     * <p>
     * The {@code onOrAfterDate} is required to prevent needlessly retrieving years of transaction history for a user.
     *
     * @param userId        the user
     * @param accountId     the account in question
     * @param onOrAfterDate only consider transactions on or after this date
     */
    public List<Pair<TransactionStatus, Instant>> getStatusAndTimestampForTrxsOnOrAfter(@NonNull UUID userId, @NonNull UUID accountId, @NonNull LocalDate onOrAfterDate) {
        Select select = QueryBuilder.select("status", "transaction_timestamp")
                .from("transactions");
        select.where(eq("user_id", userId))
                .and(eq("account_id", accountId))
                .and(gte("date", localDateTypeCodec.format(onOrAfterDate)));

        // When setting the fetch size, the driver will fetch pages this size.
        // if session.execute(select).all() is invoked, the driver will fetch all the results in chunks of fetchSizeForInternalSummary.
        select.setFetchSize(fetchSizeForInternalSummary);

        return session.execute(select).all().stream()
                // Even though "transaction_timestamp" is functionally required, we include this as a sanity check since C* cannot guarantee that this field is present.
                .filter(t -> t.getObject("transaction_timestamp") != null)
                // Create an ad-hoc object.
                .map(t -> Pair.of(
                        PENDING.name().equals(t.getString("status")) ? PENDING : BOOKED,
                        t.get("transaction_timestamp", InstantCodec.instance)
                ))
                .collect(Collectors.toList());
    }

    public Optional<Transaction> get(@NonNull UUID userId, @NonNull UUID accountId, @NonNull LocalDate date, @NonNull String transactionId) {
        return select(QueryBuilder.select()
                .from("transactions")
                .where(eq("user_id", userId))
                .and(eq("account_id", accountId))
                .and(eq("date", DateTimeFormatter.ISO_LOCAL_DATE.format(date)))
                .and(eq("id", transactionId))).stream()
                .findFirst();
    }

    public List<Transaction> getTransactionsInAccountFromDate(final @NonNull UUID userId, final @NonNull UUID accountId, @NonNull LocalDate onOrAfterDate) {
        return select(QueryBuilder.select()
                .from("transactions")
                .where(eq("user_id", userId))
                .and(eq("account_id", accountId))
                .and(gte("date", localDateTypeCodec.format(onOrAfterDate))));
    }

    public Mapper<Transaction> getMapper() {
        return mapper;
    }

    @VisibleForTesting
    int getFetchSizeForInternalSummary() {
        return fetchSizeForInternalSummary;
    }
}
