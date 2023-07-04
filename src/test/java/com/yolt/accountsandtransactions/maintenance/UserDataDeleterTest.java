package com.yolt.accountsandtransactions.maintenance;

import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountRepository;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import com.yolt.accountsandtransactions.transactions.cycles.CycleType;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCycle;
import com.yolt.accountsandtransactions.transactions.cycles.TransactionCycleRepository;
import com.yolt.accountsandtransactions.transactions.enrichments.TransactionEnrichments;
import com.yolt.accountsandtransactions.transactions.enrichments.TransactionEnrichmentsRepository;
import com.yolt.accountsandtransactions.transactions.enrichments.api.CycleTransactionEnrichment;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.AccountType;
import org.jose4j.jwt.JwtClaims;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class UserDataDeleterTest extends BaseIntegrationTest {
    private static final UUID dummy = new UUID(0, 0);

    @Autowired
    UserDataDeleter subject;

    @Autowired
    AccountRepository accountRepository;
    @Autowired
    TransactionRepository transactionRepository;
    @Autowired
    TransactionEnrichmentsRepository transactionEnrichmentsRepository;
    @Autowired
    TransactionCycleRepository transactionCycleRepository;

    /**
     * Test the {@link UserDataDeleter} by adding data across all relevant tables for two separate users.
     * We will then issue a delete for 1 of these users and check that the data has indeed been removed for
     * the user in question and that all the data of the other user is retained.
     */
    @Test
    void testUserDataDeleter() {
        UUID userToBeDeleted = UUID.randomUUID();
        UUID userToBeRetained = UUID.randomUUID();
        final UUID[] allUsers = {userToBeDeleted, userToBeRetained};

        // Add data for both users.
        for (UUID userId : allUsers) {
            final UUID accountId = UUID.randomUUID();
            final String transactionId = UUID.randomUUID().toString();
            accountRepository.upsert(Account.builder()
                    .name("account")
                    .userId(userId)
                    .siteId(dummy)
                    .userSiteId(dummy)
                    .id(accountId)
                    .externalId(dummy.toString())
                    .type(AccountType.CURRENT_ACCOUNT)
                    .currency(CurrencyCode.EUR)
                    .balance(BigDecimal.ONE)
                    .status(Account.Status.ENABLED)
                    .build());
            transactionRepository.upsert(List.of(Transaction.builder()
                    .userId(userId)
                    .accountId(accountId)
                    .id(transactionId)
                    .date(LocalDate.EPOCH)
                    .status(TransactionStatus.BOOKED)
                    .amount(BigDecimal.ZERO)
                    .currency(CurrencyCode.EUR)
                    .description("")
                    .build()));
            final UUID cycleId = UUID.randomUUID();
            transactionEnrichmentsRepository.updateEnrichmentCycleIds(List.of(new CycleTransactionEnrichment(
                    userId, accountId, LocalDate.EPOCH, transactionId, cycleId
            )));
            transactionCycleRepository.saveBatch(List.of(TransactionCycle.builder()
                    .userId(userId)
                    .cycleId(cycleId)
                    .amount(BigDecimal.ZERO)
                    .cycleType(CycleType.CREDITS)
                    .currency(CurrencyCode.EUR.name())
                    .period("bogus value")
                    .counterparty("Ajax")
                    .build()), 1);
        }

        // Check that data is indeed present in all tables.
        assertDataPresent(userToBeDeleted);
        assertDataPresent(userToBeRetained);

        // Get the cycleId that will be deleted to later check that it is indeed removed.
        UUID cycleIdToBeDeleted = transactionEnrichmentsRepository.getAllEnrichments(userToBeDeleted).get(0).getEnrichmentCycleId();

        // Subject under test.
        JwtClaims jwtClaims = new JwtClaims();
        jwtClaims.setClaim("user-id", userToBeDeleted.toString());
        ClientUserToken clientUserToken = new ClientUserToken("serialized", jwtClaims);
        subject.deleteUserData(clientUserToken);

        // Check that all data that needs to be deleted has been deleted.
        assertThat(accountRepository.getAccounts(userToBeDeleted).size()).isEqualTo(0);
        assertThat(transactionRepository.getTransactionsForUser(userToBeDeleted).size()).isEqualTo(0);
        final List<TransactionEnrichments> enrichments = transactionEnrichmentsRepository.getAllEnrichments(userToBeDeleted);
        assertThat(enrichments.size()).isEqualTo(0);
        assertThat(transactionCycleRepository.findTransactionCycle(userToBeDeleted, cycleIdToBeDeleted)).isEmpty();

        // Check that we haven't deleted data that we shouldn't have deleted.
        assertDataPresent(userToBeRetained);
    }

    private void assertDataPresent(UUID userId) {
        assertThat(accountRepository.getAccounts(userId).size()).isEqualTo(1);
        assertThat(transactionRepository.getTransactionsForUser(userId).size()).isEqualTo(1);
        final List<TransactionEnrichments> enrichments = transactionEnrichmentsRepository.getAllEnrichments(userId);
        assertThat(enrichments.size()).isEqualTo(1);
        assertThat(transactionCycleRepository.findTransactionCycle(enrichments.get(0).getUserId(), enrichments.get(0).getEnrichmentCycleId())).isPresent();
    }

}