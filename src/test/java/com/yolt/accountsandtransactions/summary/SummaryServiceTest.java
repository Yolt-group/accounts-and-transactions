package com.yolt.accountsandtransactions.summary;

import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountRepository;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.AccountType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.within;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class SummaryServiceTest extends BaseIntegrationTest {

    private static final UUID SITE_ID_MONZO = UUID.fromString("82c16668-4d59-4be8-be91-1d52792f48e3");

    @Autowired
    AccountRepository accountRepository;
    @Autowired
    TransactionRepository transactionRepository;
    @Autowired
    SummaryService summaryService;

    @Test
    public void given_userWithoutTransaction_when_getSummary_then_emptySummary() {
        var userId = UUID.randomUUID();
        var summary = summaryService.getUserSiteTransactionStatusSummary(userId);
        assertThat(summary).isEmpty();
    }

    @Test
    public void given_userWithOneBookedTransaction_when_getSummary_then_summaryIsCorrect() {
        var userId = UUID.randomUUID();
        var userSiteId = UUID.randomUUID();
        var date = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        addAccountWithTransactions(userId, userSiteId, Map.of(
                date, TransactionStatus.BOOKED
        ));
        var summary = summaryService.getUserSiteTransactionStatusSummary(userId);
        assertThat(summary).containsExactlyInAnyOrder(UserSiteTransactionStatusSummary.builder()
                .userSiteId(userSiteId)
                .transactionRetrievalLowerBoundTimestamp(Optional.of(date.minus(1, ChronoUnit.DAYS)))
                .build());
    }

    @Test
    public void given_userWithOnePendingTransaction_when_getSummary_then_transactionRetrievalLowerBoundTimestampIsEmpty() {
        var userId = UUID.randomUUID();
        var userSiteId = UUID.randomUUID();
        var date = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        addAccountWithTransactions(userId, userSiteId, Map.of(
                date, TransactionStatus.PENDING
        ));
        var summary = summaryService.getUserSiteTransactionStatusSummary(userId);
        assertThat(summary).containsExactlyInAnyOrder(UserSiteTransactionStatusSummary.builder()
                .userSiteId(userSiteId)
                .transactionRetrievalLowerBoundTimestamp(Optional.empty())
                .build());
    }

    @Test
    public void given_userWithOverlappingPendingAndBookedTransactions_when_getSummary_then_summaryIsCorrect() {
        var userId = UUID.randomUUID();
        var userSiteId = UUID.randomUUID();
        var date = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        addAccountWithTransactions(userId, userSiteId, Map.of(
                date, TransactionStatus.PENDING,
                date.plusNanos(1), TransactionStatus.BOOKED
        ));
        var summary = summaryService.getUserSiteTransactionStatusSummary(userId);
        assertThat(summary).containsExactlyInAnyOrder(UserSiteTransactionStatusSummary.builder()
                .userSiteId(userSiteId)
                .transactionRetrievalLowerBoundTimestamp(Optional.of(date.minus(1, ChronoUnit.DAYS)))
                .build());
    }

    @Test
    public void given_userWithMultipleTransactions_when_getSummary_then_summaryIsCorrect() {
        var userId = UUID.randomUUID();
        var userSiteId = UUID.randomUUID();
        var t0 = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        var t1 = t0.plusSeconds(1);
        var t2 = t1.plusSeconds(1);
        var t3 = t2.plusSeconds(1);
        var t4 = t3.plusSeconds(1);
        var t5 = t4.plusSeconds(1);
        var t6 = t5.plusSeconds(1);
        var t7 = t6.plusSeconds(1);
        var t8 = t7.plusSeconds(1);
        addAccountWithTransactions(userId, userSiteId, Map.of(
                t0, TransactionStatus.BOOKED,
                t1, TransactionStatus.BOOKED,
                t2, TransactionStatus.PENDING,
                t3, TransactionStatus.PENDING,
                t4, TransactionStatus.BOOKED,
                t5, TransactionStatus.PENDING,
                t6, TransactionStatus.BOOKED,
                t7, TransactionStatus.BOOKED,
                t8, TransactionStatus.PENDING
        ));
        var summary = summaryService.getUserSiteTransactionStatusSummary(userId);
        assertThat(summary).containsExactlyInAnyOrder(UserSiteTransactionStatusSummary.builder()
                .userSiteId(userSiteId)
                .transactionRetrievalLowerBoundTimestamp(Optional.of(t1.minus(1, ChronoUnit.DAYS)))
                .build());
    }

    @Test
    public void given_userWithMultipleAccountsAndTransactions_when_getSummary_then_summaryIsCorrect() {
        var userId = UUID.randomUUID();
        var userSiteId = UUID.randomUUID();
        var t0 = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        var t1 = t0.plusSeconds(1);
        var t2 = t1.plusSeconds(1);
        var t3 = t2.plusSeconds(1);
        addAccountWithTransactions(userId, userSiteId, Map.of(
                t1, TransactionStatus.BOOKED,
                t2, TransactionStatus.PENDING
        ));
        addAccountWithTransactions(userId, userSiteId, Map.of(
                t0, TransactionStatus.BOOKED,
                t3, TransactionStatus.PENDING
        ));
        var summary = summaryService.getUserSiteTransactionStatusSummary(userId);
        assertThat(summary).containsExactlyInAnyOrder(UserSiteTransactionStatusSummary.builder()
                .userSiteId(userSiteId)
                .transactionRetrievalLowerBoundTimestamp(Optional.of(t0.minus(1, ChronoUnit.DAYS)))
                .build());
    }

    @Test
    public void given_userWithEmptyAccount_when_getSummary_then_emptyAccountDoesNotChangeOutcome() {
        var userId = UUID.randomUUID();
        var userSiteId = UUID.randomUUID();
        var t0 = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        var t1 = t0.plusSeconds(1);
        var t2 = t1.plusSeconds(1);
        var t3 = t2.plusSeconds(1);
        addAccountWithTransactions(userId, userSiteId, Map.of(
                t0, TransactionStatus.BOOKED,
                t1, TransactionStatus.PENDING,
                t2, TransactionStatus.PENDING,
                t3, TransactionStatus.BOOKED
        ));
        addAccountWithTransactions(userId, userSiteId, Map.of());
        var summary = summaryService.getUserSiteTransactionStatusSummary(userId);
        assertThat(summary).containsExactlyInAnyOrder(UserSiteTransactionStatusSummary.builder()
                .userSiteId(userSiteId)
                .transactionRetrievalLowerBoundTimestamp(Optional.of(t0.minus(1, ChronoUnit.DAYS)))
                .build());
    }

    @Test
    public void given_userWithAccountWithOnlyPendingTransaction_when_getSummary_then_transactionRetrievalLowerBoundTimestampIsEmpty() {
        var userId = UUID.randomUUID();
        var userSiteId = UUID.randomUUID();
        var t0 = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        var t1 = t0.plusSeconds(1);
        var t2 = t1.plusSeconds(1);
        var t3 = t2.plusSeconds(1);
        addAccountWithTransactions(userId, userSiteId, Map.of(
                t1, TransactionStatus.BOOKED,
                t2, TransactionStatus.PENDING,
                t3, TransactionStatus.BOOKED
        ));
        addAccountWithTransactions(userId, userSiteId, Map.of(
                t0, TransactionStatus.PENDING
        ));
        var summary = summaryService.getUserSiteTransactionStatusSummary(userId);
        assertThat(summary).containsExactlyInAnyOrder(UserSiteTransactionStatusSummary.builder()
                .userSiteId(userSiteId)
                .transactionRetrievalLowerBoundTimestamp(Optional.empty())
                .build());
    }

    @Test
    public void given_monzoUserWithPendingTransactionsOlderThanAMonth_when_getSummary_then_returnLowerBoundTimestampOfOneMonthAgo() {
        var userId = UUID.randomUUID();
        var userSiteId = UUID.randomUUID();
        var now = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
        addAccountWithTransactions(userId, SITE_ID_MONZO, userSiteId, Map.of(
                now.toInstant(), TransactionStatus.BOOKED,
                now.minus(32, ChronoUnit.DAYS).toInstant(), TransactionStatus.PENDING,
                now.minus(33, ChronoUnit.DAYS).toInstant(), TransactionStatus.BOOKED
        ));

        var summary = summaryService.getUserSiteTransactionStatusSummary(userId);
        assertThat(summary).hasSize(1);
        assertThat(summary.get(0).getUserSiteId()).isEqualTo(userSiteId);
        assertThat(summary.get(0).getTransactionRetrievalLowerBoundTimestamp()).isPresent();

        //noinspection OptionalGetWithoutIsPresent
        assertThat(summary.get(0).getTransactionRetrievalLowerBoundTimestamp().get()).isCloseTo(
                now.minus(1, ChronoUnit.MONTHS).toInstant(), within(10, ChronoUnit.SECONDS));
    }

    /**
     * Helper fn to add 1 account for a given userId and userSiteId.  Adds a transaction to the account for every
     * entry in values.  Callers can only specify two attrbutes: the status of the transaction and the time at which
     * it happened.
     * <p>
     * All other attributes are irrelevant to these tests and so cannot be provided.
     */
    private void addAccountWithTransactions(UUID userId, UUID siteId, UUID userSiteId, Map<Instant, TransactionStatus> values) {
        var accountId = UUID.randomUUID();

        // Add the account
        accountRepository.upsert(Account.builder()
                .name("account")
                .userId(userId)
                .id(accountId)
                .userSiteId(userSiteId)
                .siteId(siteId)
                .externalId("")
                .type(AccountType.CURRENT_ACCOUNT)
                .currency(CurrencyCode.EUR)
                .balance(BigDecimal.ZERO)
                .status(Account.Status.ENABLED)
                .build());

        var trxs = values.entrySet().stream()
                .map(e -> Transaction.builder()
                        .userId(userId)
                        .accountId(accountId)
                        .currency(CurrencyCode.EUR)
                        .amount(BigDecimal.ZERO)
                        .id(UUID.randomUUID().toString())
                        .description("")
                        .status(e.getValue())
                        .timestamp(e.getKey())
                        .date(e.getKey().atOffset(ZoneOffset.UTC).toLocalDate())
                        .build())
                .collect(Collectors.toList());

        // Add the transactions
        transactionRepository.upsert(trxs);
    }

    private void addAccountWithTransactions(UUID userId, UUID userSiteId, Map<Instant, TransactionStatus> values) {
       addAccountWithTransactions(userId, UUID.randomUUID(), userSiteId, values);
    }

}
