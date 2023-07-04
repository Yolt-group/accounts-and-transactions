package com.yolt.accountsandtransactions.transactions.enrichments;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.yolt.accountsandtransactions.transactions.TransactionService.TransactionPrimaryKey;
import com.yolt.accountsandtransactions.transactions.enrichments.api.CategoryTransactionEnrichment;
import com.yolt.accountsandtransactions.transactions.enrichments.api.CounterpartyTransactionEnrichment;
import com.yolt.accountsandtransactions.transactions.enrichments.api.CycleTransactionEnrichment;
import com.yolt.accountsandtransactions.transactions.enrichments.api.LabelsTransactionEnrichment;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.cassandra.CassandraRepository;
import com.yolt.accountsandtransactions.datetime.DateInterval;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Repository;
import org.springframework.validation.annotation.Validated;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.yolt.accountsandtransactions.transactions.enrichments.TransactionEnrichments.*;
import static java.util.stream.Collectors.toList;

@Validated
@Repository
@Slf4j
public class TransactionEnrichmentsRepository extends CassandraRepository<TransactionEnrichments> {

    protected TransactionEnrichmentsRepository(Session session) {
        super(session, TransactionEnrichments.class);
        setAuditLoggingEnabled(false);
    }

    public void updateCategories(List<CategoryTransactionEnrichment> categoryTransactionEnrichments) {
        var transactionEnrichments = categoryTransactionEnrichments.stream()
                .map(it ->
                        TransactionEnrichments.builder()
                                .accountId(it.getAccountId())
                                .userId(it.getUserId())
                                .id(it.getTransactionId())
                                .date(it.getDate())
                                .enrichmentCategoryPersonal(it.getCategoryPersonal().orElse(null))
                                .enrichmentCategorySME(it.getCategorySME().orElse(null))
                                .build())
                .collect(toList());
        batchUpsertOmitNullValues(transactionEnrichments);
    }

    public void updateLabels(List<LabelsTransactionEnrichment> labelsTransactionEnrichments) {
        var transactionEnrichments = labelsTransactionEnrichments.stream()
                .map(it ->
                        TransactionEnrichments.builder()
                                .accountId(it.getAccountId())
                                .userId(it.getUserId())
                                .id(it.getTransactionId())
                                .date(it.getDate())
                                .enrichmentLabels(it.getLabels())
                                .build()
                ).collect(toList());
        batchUpsertOmitNullValues(transactionEnrichments);
    }

    public void updateCounterparties(List<CounterpartyTransactionEnrichment> counterpartyTransactionEnrichments) {
        var transactionEnrichments = counterpartyTransactionEnrichments.stream()
                .map(enrichment ->
                        TransactionEnrichments.builder()
                                .accountId(enrichment.getAccountId())
                                .userId(enrichment.getUserId())
                                .id(enrichment.getTransactionId())
                                .date(enrichment.getDate())
                                .enrichmentCounterpartyName(enrichment.getCounterparty())
                                .enrichmentCounterpartyIsKnownMerchant(enrichment.isKnownMerchant())
                                .build()
                )
                .collect(toList());
        batchUpsertOmitNullValues(transactionEnrichments);
    }

    public void updateEnrichmentCycleIds(List<CycleTransactionEnrichment> cycleTransactionEnrichments) {
        var transactionEnrichments = cycleTransactionEnrichments.stream()
                .map(it ->
                        TransactionEnrichments.builder()
                                .accountId(it.getAccountId())
                                .userId(it.getUserId())
                                .id(it.getTransactionId())
                                .date(it.getDate())
                                .enrichmentCycleId(it.getCycleId())
                                .build()
                )
                .collect(toList());
        batchUpsertOmitNullValues(transactionEnrichments);
    }

    public void deleteTransactionEnrichmentsForAccount(UUID userId, List<UUID> accountIds) {
        var delete = super.createDelete();
        delete.where(eq(USER_ID_COLUMN, userId))
                .and(in(ACCOUNT_ID_COLUMN, accountIds));

        executeDelete(delete);
    }

    public void deleteSpecificEnrichments(final @NonNull List<TransactionPrimaryKey> enrichmentsToDelete) {
        BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        enrichmentsToDelete.forEach(it -> {
            var delete = super.createDelete();
            delete.where(eq(USER_ID_COLUMN, it.getUserId()))
                    .and(eq(ACCOUNT_ID_COLUMN, it.getAccountId()))
                    .and(eq(DATE_COLUMN, DateTimeFormatter.ISO_LOCAL_DATE.format(it.getDate())))
                    .and(eq(ID_COLUMN, it.getId()));
            batch.add(delete);
        });

        session.execute(batch);

        log.debug("Deleted a batch of {} transaction enrichments", enrichmentsToDelete.size());
    }

    /**
     * This method does not overwrite the nulled fields of the {@link TransactionEnrichments} because the table will be
     * updated with new fields on each message we receive from datascience.
     * That's why we call the {@link super#saveBatchWithOption(List, int)} with saveNullFields
     */
    @VisibleForTesting
    void batchUpsertOmitNullValues(List<TransactionEnrichments> transactionEnrichments) {
        var batchUpsert = transactionEnrichments.stream()
                .map(it -> new Tuple2<>(it, ImmutableList.of(Mapper.Option.saveNullFields(false))))
                .collect(toList());
        super.saveBatchWithOption(batchUpsert, 100);
    }

    public Optional<TransactionEnrichments> get(@NonNull UUID userId, @NonNull UUID accountId, @NonNull LocalDate date, @NonNull String transactionId) {
        var select = createSelect();
        select.where(eq(USER_ID_COLUMN, userId))
                .and(eq(ACCOUNT_ID_COLUMN, accountId))
                .and(eq(DATE_COLUMN, DateTimeFormatter.ISO_LOCAL_DATE.format(date)))
                .and(eq(ID_COLUMN, transactionId));
        return select(select).stream().findFirst();
    }

    public List<TransactionEnrichments> get(@NonNull UUID userId, @NonNull List<UUID> accountIds, @Nullable DateInterval interval) {
        var select = createSelect().where(eq(USER_ID_COLUMN, userId));
        select.and(in(ACCOUNT_ID_COLUMN, accountIds));
        if (interval != null) {
            select.and(gte(DATE_COLUMN, interval.getStartFormatted()));
            select.and(lte(DATE_COLUMN, interval.getEndFormatted()));
        }
        return select(select);
    }

    public List<TransactionEnrichments> getAllEnrichments(@NonNull UUID userId) {
        var select = createSelect();
        select.where(eq(USER_ID_COLUMN, userId));
        return select(select);
    }

    @Value
    public static class TransactionEnrichmentsPrimaryKey {
        @NonNull
        UUID userId;

        @NonNull
        UUID accountId;

        @NonNull
        LocalDate date;

        @NonNull
        String id;
    }
}
