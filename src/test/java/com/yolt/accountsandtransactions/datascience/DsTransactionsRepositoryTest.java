package com.yolt.accountsandtransactions.datascience;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.Mapper;
import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.TestUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.yolt.accountsandtransactions.TestUtils.createTransaction;
import static com.yolt.accountsandtransactions.TestUtils.saveTransaction;
import static com.yolt.accountsandtransactions.datascience.PendingType.PENDING;
import static com.yolt.accountsandtransactions.datascience.PendingType.REGULAR;
import static org.assertj.core.api.Assertions.assertThat;


public class DsTransactionsRepositoryTest extends BaseIntegrationTest {


    public static final String CATEGORY_B = "category_b";
    public static final String MER_CHANT = "MerChant";
    @Autowired
    private DsTransactionsRepository repository;
    @Autowired
    private DataScienceCassandraSession session;

    private Mapper<DsTransaction> mapper;


    @Override
    protected void setup() {
        mapper = mappingManager.mapper(DsTransaction.class, dsKeyspace);
    }

    @Test
    public void testInsertDelete() {
        UUID userId1 = UUID.randomUUID();
        UUID userId2 = UUID.randomUUID();
        UUID account1 = UUID.randomUUID();
        UUID account2 = UUID.randomUUID();
        UUID account3 = UUID.randomUUID();

        assertRecordCount(0);
        assertRecordCountForUser(userId1, 0);
        assertRecordCountForUser(userId2, 0);

        List<DsTransaction> trx1 = createTransactions(userId1, account1);
        repository.saveTransactionBatch(trx1);
        assertRecordCount(8);
        assertRecordCountForUser(userId1, 8);
        assertRecordCountForUser(userId2, 0);

        List<DsTransaction> trx2 = createTransactions(userId1, account2);
        repository.saveTransactionBatch(trx2);
        assertRecordCount(16);
        assertRecordCountForUser(userId1, 16);
        assertRecordCountForUser(userId2, 0);

        List<DsTransaction> trx3 = createTransactions(userId2, account3);
        repository.saveTransactionBatch(trx3);
        assertRecordCount(24);
        assertRecordCountForUser(userId1, 16);
        assertRecordCountForUser(userId2, 8);

        repository.deleteTransactions(trx1);
        assertRecordCount(16);
        assertRecordCountForUser(userId1, 8);
        assertRecordCountForUser(userId2, 8);

        repository.deleteTransactions(trx3);
        assertRecordCount(8);
        assertRecordCountForUser(userId1, 8);
        assertRecordCountForUser(userId2, 0);

        repository.deleteTransactions(trx2);
        assertRecordCount(0);
        assertRecordCountForUser(userId1, 0);
        assertRecordCountForUser(userId2, 0);
    }

    @Test
    public void testInsertWithSmallBatchSize() {
        UUID userId1 = UUID.randomUUID();
        UUID userId2 = UUID.randomUUID();
        UUID accountId1 = UUID.randomUUID();
        UUID accountId2 = UUID.randomUUID();
        UUID accountId3 = UUID.randomUUID();
        repository = new DsTransactionsRepository(session, 2);

        assertRecordCount(0);
        assertRecordCountForUser(userId1, 0);
        assertRecordCountForUser(userId2, 0);

        List<DsTransaction> trx1 = createTransactions(userId1, accountId1);
        repository.saveTransactionBatch(trx1);
        assertRecordCount(8);
        assertRecordCountForUser(userId1, 8);
        assertRecordCountForUser(userId2, 0);

        List<DsTransaction> trx2 = createTransactions(userId1, accountId2);
        repository.saveTransactionBatch(trx2);
        assertRecordCount(16);
        assertRecordCountForUser(userId1, 16);
        assertRecordCountForUser(userId2, 0);

        List<DsTransaction> trx3 = createTransactions(userId2, accountId3);
        repository.saveTransactionBatch(trx3);
        assertRecordCount(24);
        assertRecordCountForUser(userId1, 16);
        assertRecordCountForUser(userId2, 8);

    }


    @Test
    public void testExtendedTransactionFields() {
        UUID userId1 = UUID.randomUUID();
        UUID accountId1 = UUID.randomUUID();

        // prep the repo
        repository = new DsTransactionsRepository(session, 1);
        assertRecordCount(0);

        // insert list of transactions for a single user
        List<DsTransaction> trx1 = createTransactions(userId1, accountId1);
        repository.saveTransactionBatch(trx1);
        assertRecordCount(8);
        assertRecordCountForUser(userId1, 8);

        // Retrieve the transactions
        Select select = QueryBuilder.select().all().from(dsKeyspace, DsTransaction.TABLE_NAME);
        List<DsTransaction> dsTransactions = mapper.map(session.getSession().execute(select)).all();
        assertThat(dsTransactions.size()).isEqualTo(8);
        for (DsTransaction dsTransaction : dsTransactions) {
            // Check the extended transaction is cool
            assertThat(dsTransaction.getExtendedTransactionAsString()).isEqualTo(TestUtils.EXTENDED_TRANSACTION_JSON);
        }

    }

    @Test
    public void testGetMonthRangeForPending() {
        UUID userId1 = UUID.randomUUID();
        UUID accountId1 = UUID.randomUUID();
        UUID accountId2 = UUID.randomUUID();
        UUID accountId3 = UUID.randomUUID();
        saveTransactions(userId1, accountId1, accountId2);
        assertRecordCountForUser(userId1, 8);

        Stream<String> result = repository.getDatesPendingTransactions(userId1, Collections.singletonList(accountId1));
        Stream<String> result2 = repository.getDatesPendingTransactions(userId1, Arrays.asList(accountId1, accountId2));
        Stream<String> result3 = repository.getDatesPendingTransactions(userId1, Collections.singletonList(accountId3));

        assertThat(result.collect(Collectors.toList())).containsExactlyInAnyOrder("2015-01",
                "2015-03");


        assertThat(result2.collect(Collectors.toList())).containsExactlyInAnyOrder(
                "2016-01",
                "2016-03",
                "2015-01",
                "2015-03");
        assertThat(result3.collect(Collectors.toList())).isEmpty();
    }

    @Test
    public void getTransactionsInAccountFromDate() {
        UUID userId1 = UUID.randomUUID();
        UUID accountId1 = UUID.randomUUID();

        List<DsTransaction> savedTransactions = saveTransactions(userId1, accountId1, UUID.randomUUID());

        List<DsTransaction> result = repository.getTransactionsInAccountFromDate(userId1, accountId1, "2015-01-01");
        assertThat(result).containsExactlyInAnyOrderElementsOf(savedTransactions.subList(0, 4));

        result = repository.getTransactionsInAccountFromDate(userId1, accountId1, "2015-03-03");
        assertThat(result).containsExactlyInAnyOrderElementsOf(savedTransactions.subList(2, 4));

        result = repository.getTransactionsInAccountFromDate(userId1, accountId1, "2015-05-05");
        assertThat(result).isEmpty();
    }

    @Test
    public void nullsNotSaved() {
        UUID userId1 = UUID.randomUUID();
        UUID accountId1 = UUID.randomUUID();

        DsTransaction transaction = createTransaction(userId1, REGULAR, "2015-02-02", accountId1, "12", new BigDecimal("223.45"), "EUR", CATEGORY_B, MER_CHANT);
        repository.saveTransactionBatch(Collections.singletonList(transaction));

        DsTransaction dbTransaction = mapper.get(userId1, REGULAR, accountId1, "2015-02-02", "12");
        assertThat(dbTransaction.getCurrency()).isEqualTo("EUR");
        assertThat(dbTransaction.getMappedCategory()).isEqualTo(CATEGORY_B);

        transaction.setCurrency("GBP");
        transaction.setMappedCategory(null);
        repository.saveTransactionBatch(Collections.singletonList(transaction));

        dbTransaction = mapper.get(userId1, REGULAR, accountId1, "2015-02-02", "12");
        assertThat(dbTransaction.getCurrency()).isEqualTo("GBP");
        // Fields not overwritten
        assertThat(dbTransaction.getMappedCategory()).isEqualTo(CATEGORY_B);
    }

    private List<DsTransaction> createTransactions(final UUID userId, final UUID accountId) {
        return Arrays.asList(
                createTransaction(userId, REGULAR, "2015-01-01", accountId, "1", new BigDecimal("123.45"), "EUR", CATEGORY_B, MER_CHANT),
                createTransaction(userId, REGULAR, "2015-01-02", accountId, "2", new BigDecimal("223.45"), "EUR", CATEGORY_B, MER_CHANT),
                createTransaction(userId, REGULAR, "2015-01-03", accountId, "3", new BigDecimal("323.45"), "EUR", CATEGORY_B, MER_CHANT),
                createTransaction(userId, REGULAR, "2015-01-04", accountId, "4", new BigDecimal("423.45"), "EUR", CATEGORY_B, MER_CHANT),
                createTransaction(userId, REGULAR, "2015-01-05", accountId, "5", new BigDecimal("523.45"), "EUR", CATEGORY_B, MER_CHANT),
                createTransaction(userId, PENDING, "2015-01-05", accountId, "6", new BigDecimal("623.45"), "EUR", CATEGORY_B, MER_CHANT),
                createTransaction(userId, REGULAR, "2015-01-06", accountId, "7", new BigDecimal("723.45"), "EUR", CATEGORY_B, MER_CHANT),
                createTransaction(userId, PENDING, "2015-01-06", accountId, "8", new BigDecimal("823.45"), "EUR", CATEGORY_B, MER_CHANT)
        );
    }

    private List<DsTransaction> saveTransactions(UUID userId1, UUID accountId1, UUID accountId2) {
        return Arrays.asList(
                saveTransaction(userId1, PENDING, "2015-01-01", accountId1, "11", new BigDecimal("123.45"), "EUR", CATEGORY_B, MER_CHANT, mapper),
                saveTransaction(userId1, REGULAR, "2015-02-02", accountId1, "12", new BigDecimal("223.45"), "EUR", CATEGORY_B, MER_CHANT, mapper),
                saveTransaction(userId1, PENDING, "2015-03-03", accountId1, "13", new BigDecimal("323.45"), "EUR", CATEGORY_B, MER_CHANT, mapper),
                saveTransaction(userId1, REGULAR, "2015-04-04", accountId1, "14", new BigDecimal("423.45"), "EUR", CATEGORY_B, MER_CHANT, mapper),
                saveTransaction(userId1, PENDING, "2016-01-01", accountId2, "21", new BigDecimal("123.45"), "EUR", CATEGORY_B, MER_CHANT, mapper),
                saveTransaction(userId1, REGULAR, "2016-02-02", accountId2, "22", new BigDecimal("223.45"), "EUR", CATEGORY_B, MER_CHANT, mapper),
                saveTransaction(userId1, PENDING, "2016-03-03", accountId2, "23", new BigDecimal("323.45"), "EUR", CATEGORY_B, MER_CHANT, mapper),
                saveTransaction(userId1, REGULAR, "2016-04-04", accountId2, "24", new BigDecimal("423.45"), "EUR", CATEGORY_B, MER_CHANT, mapper)
        );
    }

    private void assertRecordCountForUser(final UUID userId, final long count) {
        assertThat(countForUser(dsKeyspace, DsTransaction.TABLE_NAME, userId, session.getSession())).isEqualTo(count);
    }

    private void assertRecordCount(final long count) {
        assertThat(countAll(dsKeyspace, DsTransaction.TABLE_NAME, session.getSession())).isEqualTo(count);
    }

    static long countAll(final String keyspace, final String table, final Session session) {
        Select select = QueryBuilder.select().countAll().from(keyspace, table);

        ResultSet resultSet = session.execute(select);
        Row row = resultSet.one();

        return row.getLong(0);
    }

    static long countForUser(final String keyspace, final String table, final UUID userId, final Session session) {
        Select select = QueryBuilder.select().countAll().from(keyspace, table);
        select.where(eq("user_id", userId));

        ResultSet resultSet = session.execute(select);
        Row row = resultSet.one();

        return row.getLong(0);
    }
}
