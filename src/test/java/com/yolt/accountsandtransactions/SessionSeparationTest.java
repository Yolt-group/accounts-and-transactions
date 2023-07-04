package com.yolt.accountsandtransactions;

import com.datastax.driver.core.Session;
import com.yolt.accountsandtransactions.datascience.DataScienceCassandraSession;
import com.yolt.accountsandtransactions.datascience.DataScienceService;
import com.yolt.accountsandtransactions.datascience.DsTransaction;
import com.yolt.accountsandtransactions.datascience.PendingType;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import com.yolt.accountsandtransactions.inputprocessing.ProviderTransactionWithId;
import com.yolt.accountsandtransactions.inputprocessing.TransactionInsertionStrategy.Instruction.InstructionType;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.test.TestJwtClaims;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import static java.util.UUID.randomUUID;
import static nl.ing.lovebird.extendeddata.common.CurrencyCode.EUR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Makes sure that the {@link DataScienceService} and {@link TransactionService} are writing and reading
 * to separate keyspaces.  This test doesn't exercise all methods, but as long as the repositories that
 * query the datascience keyspace use {@link DataScienceCassandraSession} instead of {@link Session}.
 */
public class SessionSeparationTest extends BaseIntegrationTest {

    @Autowired
    DataScienceService dsTrxService;

    @Autowired
    TransactionService atTrxService;

    @Test
    public void testKeyspaceSeparation() {
        // Store transaction with id 1 in the datascience keyspace
        UUID userId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        var clientUserToken = new ClientUserToken(null, TestJwtClaims.createClientUserClaims("junit", UUID.randomUUID(), UUID.randomUUID(), userId));

        dsTrxService.saveTransactionBatch(List.of(
                DsTransaction.builder()
                        .userId(userId)
                        .pending(PendingType.REGULAR)
                        .date("1970-01-01")
                        .accountId(accountId)
                        .transactionId("1")
                        .amount(BigDecimal.ZERO)
                        .description("")
                        .build()
        ));

        var accountFromProviders = AccountFromProviders.accountsFromProvidersBuilder()
                .yoltUserSiteId(randomUUID())
                .currency(EUR)
                .provider("aProvider")
                .build();

        // Store another transaction with id 2 in the accounts_and_transactions keyspace (for the same user, account)
        atTrxService.saveTransactionsBatch(accountId, clientUserToken, accountFromProviders, List.of(new ProviderTransactionWithId(
                ProviderTransactionDTO.builder()
                        .status(TransactionStatus.BOOKED)
                        .dateTime(ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC))
                        .description("")
                        .amount(BigDecimal.ONE)
                        .build(),
                "2"
        )), InstructionType.INSERT);

        // Make sure the transaction with id 1 is in the datascience keyspace, and the transaction with id 2
        // in the accounts_and_transactions keyspace.
        final List<DsTransaction> dsTrxs = dsTrxService.getTransactionsForUser(userId);
        assertThat(dsTrxs).hasSize(1);
        assertThat(dsTrxs.get(0).getTransactionId()).isEqualTo("1");

        final List<Transaction> atTrxs = atTrxService.getTransactionsForUser(userId);
        assertThat(atTrxs).hasSize(1);
        assertThat(atTrxs.get(0).getId()).isEqualTo("2");
    }

}
