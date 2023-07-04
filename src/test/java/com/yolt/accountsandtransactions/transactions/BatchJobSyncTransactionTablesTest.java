package com.yolt.accountsandtransactions.transactions;

import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountRepository;
import com.yolt.accountsandtransactions.batch.BatchJobSyncTransactionTables;
import com.yolt.accountsandtransactions.batch.BatchSyncProgressStateRepository;
import com.yolt.accountsandtransactions.datascience.DsTransaction;
import com.yolt.accountsandtransactions.datascience.DsTransactionsRepository;
import com.yolt.accountsandtransactions.datascience.PendingType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.UUID;

import static com.yolt.accountsandtransactions.accounts.Account.Status.ENABLED;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.TEN;
import static java.time.LocalDate.EPOCH;
import static java.time.LocalDate.parse;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static nl.ing.lovebird.extendeddata.common.CurrencyCode.EUR;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.BOOKED;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.PENDING;
import static nl.ing.lovebird.providerdomain.AccountType.CURRENT_ACCOUNT;
import static org.assertj.core.api.Assertions.assertThat;

class BatchJobSyncTransactionTablesTest extends BaseIntegrationTest {

    @Autowired
    public AccountRepository atAcctRepository;
    @Autowired
    public DsTransactionsRepository dsTransactionsRepository;
    @Autowired
    public TransactionRepository atTransactionRepository;
    @Autowired
    public BatchSyncProgressStateRepository batchSyncProgressStateRepository;

    @Autowired
    public BatchJobSyncTransactionTables batchJobSyncTransactionTables;

    @Test
    public void testBatchJobSyncTransactionKeyspaces() {
        UUID userId = randomUUID();
        UUID acctId = randomUUID();

        atAcctRepository.upsert(Account.builder()
                .userId(userId)
                .id(acctId)
                .name("test")
                .type(CURRENT_ACCOUNT)
                .balance(TEN)
                .userSiteId(new UUID(0, 0))
                .siteId(new UUID(0, 0))
                .externalId("123")
                .currency(EUR)
                .status(ENABLED)
                .build());

        // Simple example, 1 booked trx, 1 pending trx.
        final List<DsTransaction> trxs = List.of(
                DsTransaction.builder()
                        .userId(userId)
                        .accountId(acctId)
                        .date("1970-01-01")
                        .transactionId("1")
                        .pending(PendingType.REGULAR)
                        .build(),
                DsTransaction.builder()
                        .userId(userId)
                        .accountId(acctId)
                        .date("1970-01-01")
                        .transactionId("2")
                        .pending(PendingType.PENDING)
                        .build()
        );

        // Store these trxs in both keyspace (datascience, at)
        dsTransactionsRepository.saveTransactionBatch(trxs);
        atTransactionRepository.saveBatch(trxs.stream()
                .map(this::fromDsTrx)
                .collect(toList()
                ), 10);

        // Run the batch, this should do nothing.
        batchSyncProgressStateRepository.reset();
        batchJobSyncTransactionTables.doRun(false, -1);

        // Make sure the batch has indeed not deleted anything from our table.
        assertThat(atTransactionRepository.getTransactionsForUser(userId)).hasSize(2);

        // Add a new PENDING and a new BOOKED transaction to *ONLY* our table, not the datascience keyspace.
        atTransactionRepository.saveBatch(List.of(
                Transaction.builder()
                        .userId(userId)
                        .accountId(acctId)
                        .date(EPOCH)
                        .id("3")
                        .status(PENDING)
                        .currency(EUR)
                        .amount(TEN)
                        .build(),
                Transaction.builder()
                        .userId(userId)
                        .accountId(acctId)
                        .date(EPOCH)
                        .id("4")
                        .status(BOOKED)
                        .currency(EUR)
                        .amount(ONE)
                        .build()
        ), 10);

        // Do a dryrun, this should have no effect, it shouldn't delete anything (there are 4 trxs before/after)
        assertThat(atTransactionRepository.getTransactionsForUser(userId)).hasSize(4);
        batchSyncProgressStateRepository.reset();
        batchJobSyncTransactionTables.doRun(true, -1);
        assertThat(atTransactionRepository.getTransactionsForUser(userId)).hasSize(4);

        // This run should delete transaction 3, and not transaction 4, despite it not being present in the
        // datascience keyspace, since it has status BOOKED.
        batchSyncProgressStateRepository.reset();
        batchJobSyncTransactionTables.doRun(false, -1);

        // Check that it has indeed deleted transaction 3, which is expected.
        assertThat(atTransactionRepository.getTransactionsForUser(userId)).hasSize(3);
        assertThat(atTransactionRepository.getTransactionsForUser(userId).stream().map(Transaction::getId).collect(toList()))
                .containsExactlyInAnyOrder("1", "2", "4");
    }

    Transaction fromDsTrx(DsTransaction trx) {
        return Transaction.builder()
                .userId(trx.getUserId())
                .accountId(trx.getAccountId())
                .date(trx.getLocalDate())
                .id(trx.getTransactionId())
                .status(trx.getPending().equals(PendingType.PENDING) ? PENDING : BOOKED)
                .build();
    }
}