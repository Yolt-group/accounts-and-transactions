package com.yolt.accountsandtransactions;

import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountRepository;
import com.yolt.accountsandtransactions.inputprocessing.AccountFromProviders;
import com.yolt.accountsandtransactions.inputprocessing.AccountsAndTransactionDiagnosticsService;
import com.yolt.accountsandtransactions.inputprocessing.AccountsAndTransactionsRequestDTO;
import com.yolt.accountsandtransactions.inputprocessing.AccountsAndTransactionsService;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.activities.ActivityEnrichmentService;
import com.yolt.accountsandtransactions.summary.SummaryService;
import com.yolt.accountsandtransactions.summary.UserSiteTransactionStatusSummary;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import lombok.SneakyThrows;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import nl.ing.lovebird.providerdomain.AccountType;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import nl.ing.lovebird.providerdomain.ProviderTransactionType;
import nl.ing.lovebird.providerdomain.YoltCategory;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.groups.Tuple;
import org.jose4j.jwt.JwtClaims;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

public class TransactionRefreshWindowIntegrationTest extends BaseIntegrationTest {

    @Autowired
    AccountRepository accountRepository;
    @Autowired
    TransactionRepository transactionRepository;
    @Autowired
    SummaryService summaryService;
    @Autowired
    ActivityEnrichmentService activityEnrichmentService;
    @Autowired
    AccountsAndTransactionDiagnosticsService accountsAndTransactionDiagnosticsService;
    @Autowired
    AccountsAndTransactionsService accountsAndTransactionsService;

    @Test
    @SneakyThrows
    public void given_accountWithBookedBeforePendingTrx_when_pendingChanges_then_correctlyUpdated() {
        // Points in time
        var now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        var t0 = now.minus(3, ChronoUnit.DAYS);
        var t1 = now.minus(36, ChronoUnit.HOURS);
        var t2 = now.minus(24, ChronoUnit.HOURS);
        var t3 = now.minus(12, ChronoUnit.HOURS);

        var userId = UUID.randomUUID();
        var userSiteId = UUID.randomUUID();
        var siteId = UUID.randomUUID();
        var clientUserToken = buildClientUserToken(userId);

        // Account and transactions to be used
        var account = buildAccount(userId, userSiteId, siteId);
        var transactions = buildTransactions(userId, account.getId(), List.of(
                Pair.of(t0, TransactionStatus.BOOKED),
                Pair.of(t1, TransactionStatus.BOOKED),
                Pair.of(t2, TransactionStatus.PENDING)
        ));

        // Data that is ingested to set up data stores
        var initialIngestionTransactions = withoutChange(transactions);
        var initialIngestionData = buildIngestionData(userSiteId, siteId, account, initialIngestionTransactions);

        // Data that is ingested to perform the actual test
        var ingestionTransactions = List.of(
                withoutChange(transactions.get(1)),
                pendingToBooked(transactions.get(2), t3));
        var ingestionData = buildIngestionData(userSiteId, siteId, account, ingestionTransactions);

        // Perform ingestion to set up data stores
        stubNewAccounts(initialIngestionData, account.getId());
        accountsAndTransactionsService.processAccountsAndTransactionsForUserSite(clientUserToken, initialIngestionData);

        assertThat(transactionRepository.getTransactionsForUser(userId))
                .extracting("timestamp", "status")
                .containsExactlyInAnyOrder(transactions.stream()
                        .map(trx -> tuple(trx.getTimestamp(), trx.getStatus()))
                        .toArray(Tuple[]::new));

        // Verify requested timestamp is in line with test data
        var requestedTimestamp = getRequestedTimestamp(userId, userSiteId);
        assertThat(requestedTimestamp).isBeforeOrEqualTo(t1).isAfter(t0);

        // Perform test
        stubExistingAccounts(userId, ingestionData, account.getId());
        accountsAndTransactionsService.processAccountsAndTransactionsForUserSite(clientUserToken, ingestionData);

        var result = transactionRepository.getTransactionsForUser(userId);

        // Assert results
        assertThat(result)
                .extracting("timestamp", "status")
                .containsExactlyInAnyOrder(
                        tuple(t0, TransactionStatus.BOOKED),
                        tuple(t1, TransactionStatus.BOOKED),
                        tuple(t3, TransactionStatus.BOOKED));
    }

    @Test
    @SneakyThrows
    public void given_accountWithEarliestPendingTrx_when_pendingDoesNotChange_then_nothingChanges() {
        // Points in time
        var now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        var t0 = now.minus(3, ChronoUnit.DAYS);
        var t1 = now.minus(2, ChronoUnit.DAYS);

        var userId = UUID.randomUUID();
        var userSiteId = UUID.randomUUID();
        var siteId = UUID.randomUUID();
        var clientToken = buildClientUserToken(userId);

        // Account and transactions to be used
        var account = buildAccount(userId, userSiteId, siteId);
        var transactions = buildTransactions(userId, account.getId(), List.of(
                Pair.of(t0, TransactionStatus.PENDING),
                Pair.of(t1, TransactionStatus.BOOKED)
        ));

        // Data that is ingested to set up data stores
        var initialIngestionTransactions = withoutChange(transactions);
        var initialIngestionData = buildIngestionData(userSiteId, siteId, account, initialIngestionTransactions);

        // Data that is ingested to perform the actual test
        var ingestionTransactions = withoutChange(transactions);
        var ingestionData = buildIngestionData(userSiteId, siteId, account, ingestionTransactions);

        // Perform ingestion to set up data stores
        stubNewAccounts(initialIngestionData, account.getId());
        accountsAndTransactionsService.processAccountsAndTransactionsForUserSite(clientToken, initialIngestionData);

        assertThat(transactionRepository.getTransactionsForUser(userId))
                .extracting("timestamp", "status")
                .containsExactlyInAnyOrder(transactions.stream()
                        .map(trx -> tuple(trx.getTimestamp(), trx.getStatus()))
                        .toArray(Tuple[]::new));

        // Verify requested timestamp is in line with test data
        var requestedTimestamp = getRequestedTimestamp(userId, userSiteId);
        assertThat(requestedTimestamp).isNull();

        // Perform test
        stubExistingAccounts(userId, ingestionData, account.getId());
        accountsAndTransactionsService.processAccountsAndTransactionsForUserSite(clientToken, ingestionData);

        var result = transactionRepository.getTransactionsForUser(userId);

        // Assert results
        assertThat(result)
                .extracting("timestamp", "status")
                .containsExactlyInAnyOrder(
                        tuple(t0, TransactionStatus.PENDING),
                        tuple(t1, TransactionStatus.BOOKED));
    }

    @Disabled("Expected to fail - we cannot correctly handle this use case at the moment")
    @Test
    @SneakyThrows
    public void given_accountWithEarliestPendingTrx_when_pendingChanges_then_correctlyUpdated() {
        // Points in time
        var now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        var t0 = now.minus(3, ChronoUnit.DAYS);
        var t1 = now.minus(2, ChronoUnit.DAYS);
        var t2 = now.minus(1, ChronoUnit.DAYS);

        var userId = UUID.randomUUID();
        var userSiteId = UUID.randomUUID();
        var siteId = UUID.randomUUID();
        var clientToken = buildClientUserToken(userId);

        // Account and transactions to be used
        var account = buildAccount(userId, userSiteId, siteId);
        var transactions = buildTransactions(userId, account.getId(), List.of(
                Pair.of(t0, TransactionStatus.PENDING),
                Pair.of(t1, TransactionStatus.BOOKED)
        ));

        // Data that is ingested to set up data stores
        var initialIngestionTransactions = withoutChange(transactions);
        var initialIngestionData = buildIngestionData(userSiteId, siteId, account, initialIngestionTransactions);

        // Data that is ingested to perform the actual test
        var ingestionTransactions = List.of(
                pendingToBooked(transactions.get(0), t1),
                withoutChange(transactions.get(1)));
        var ingestionData = buildIngestionData(userSiteId, siteId, account, ingestionTransactions);

        // Perform ingestion to set up data stores
        stubNewAccounts(initialIngestionData, account.getId());
        accountsAndTransactionsService.processAccountsAndTransactionsForUserSite(clientToken, initialIngestionData);

        assertThat(transactionRepository.getTransactionsForUser(userId))
                .extracting("timestamp", "status")
                .containsExactlyInAnyOrder(transactions.stream()
                        .map(trx -> tuple(trx.getTimestamp(), trx.getStatus()))
                        .toArray(Tuple[]::new));

        // Verify requested timestamp is in line with test data
        var requestedTimestamp = getRequestedTimestamp(userId, userSiteId);
        assertThat(requestedTimestamp).isNull();

        // Perform test
        stubExistingAccounts(userId, ingestionData, account.getId());
        accountsAndTransactionsService.processAccountsAndTransactionsForUserSite(clientToken, ingestionData);

        var result = transactionRepository.getTransactionsForUser(userId);

        // Assert results
        assertThat(result)
                .extracting("timestamp", "status")
                .containsExactlyInAnyOrder(
                        tuple(t1, TransactionStatus.BOOKED),
                        tuple(t2, TransactionStatus.BOOKED));
    }

    @Test
    @SneakyThrows
    public void given_accountWithOnlyBookedTrx_when_newTrxAdded_then_correctlyUpdated() {
        // Points in time
        var now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        var t0 = now.minus(3, ChronoUnit.DAYS);
        var t1 = now.minus(36, ChronoUnit.HOURS);
        var t2 = now.minus(12, ChronoUnit.HOURS);

        var userId = UUID.randomUUID();
        var userSiteId = UUID.randomUUID();
        var siteId = UUID.randomUUID();
        var clientToken = buildClientUserToken(userId);

        // Account and transactions to be used
        var account = buildAccount(userId, userSiteId, siteId);
        var transactions = buildTransactions(userId, account.getId(), List.of(
                Pair.of(t0, TransactionStatus.BOOKED),
                Pair.of(t1, TransactionStatus.BOOKED)
        ));

        // Data that is ingested to set up data stores
        var initialIngestionTransactions = withoutChange(transactions);
        var initialIngestionData = buildIngestionData(userSiteId, siteId, account, initialIngestionTransactions);

        // Data that is ingested to perform the actual test
        var newTransactions = buildTransactions(userId, account.getId(), List.of(Pair.of(t2, TransactionStatus.PENDING)));
        var ingestionTransactions = List.of(
                withoutChange(transactions.get(1)),
                withoutChange(newTransactions.get(0)));
        var ingestionData = buildIngestionData(userSiteId, siteId, account, ingestionTransactions);

        // Perform ingestion to set up data stores
        stubNewAccounts(initialIngestionData, account.getId());
        accountsAndTransactionsService.processAccountsAndTransactionsForUserSite(clientToken, initialIngestionData);

        assertThat(transactionRepository.getTransactionsForUser(userId))
                .extracting("timestamp", "status")
                .containsExactlyInAnyOrder(transactions.stream()
                        .map(trx -> tuple(trx.getTimestamp(), trx.getStatus()))
                        .toArray(Tuple[]::new));

        // Verify requested timestamp is in line with test data
        var requestedTimestamp = getRequestedTimestamp(userId, userSiteId);
        assertThat(requestedTimestamp).isBeforeOrEqualTo(t1).isAfter(t0);

        // Perform test
        stubExistingAccounts(userId, ingestionData, account.getId());
        accountsAndTransactionsService.processAccountsAndTransactionsForUserSite(clientToken, ingestionData);

        var result = transactionRepository.getTransactionsForUser(userId);

        // Assert results
        assertThat(result)
                .extracting("timestamp", "status")
                .containsExactlyInAnyOrder(
                        tuple(t0, TransactionStatus.BOOKED),
                        tuple(t1, TransactionStatus.BOOKED),
                        tuple(t2, TransactionStatus.PENDING));
    }

    private Instant getRequestedTimestamp(UUID userId, UUID userSiteId) {
        return summaryService.getUserSiteTransactionStatusSummary(userId).stream()
                .filter(summary -> summary.getUserSiteId().equals(userSiteId))
                .findAny()
                .flatMap(UserSiteTransactionStatusSummary::getTransactionRetrievalLowerBoundTimestamp)
                .orElse(null);
    }

    private static ClientUserToken buildClientUserToken(UUID userId) {
        JwtClaims claims = new JwtClaims();
        claims.setClaim("client-id", UUID.randomUUID().toString());
        claims.setClaim("user-id", userId.toString());
        return new ClientUserToken(null, claims);
    }

    private static Account buildAccount(UUID userId, UUID userSiteId, UUID siteId) {
        return Account.builder()
                .userId(userId)
                .id(UUID.randomUUID())
                .name("")
                .userSiteId(userSiteId)
                .siteId(siteId)
                .externalId("")
                .type(AccountType.CURRENT_ACCOUNT)
                .currency(CurrencyCode.EUR)
                .balance(BigDecimal.ZERO)
                .status(Account.Status.ENABLED)
                .build();
    }

    private static List<Transaction> buildTransactions(UUID userId, UUID accountId, List<Pair<Instant, TransactionStatus>> values) {
        return values.stream()
                .map(p -> Transaction.builder()
                        .userId(userId)
                        .accountId(accountId)
                        .currency(CurrencyCode.EUR)
                        .amount(BigDecimal.ONE.negate())
                        .id(UUID.randomUUID().toString())
                        .externalId(UUID.randomUUID().toString())
                        .description("")
                        .status(p.getValue())
                        .timestamp(p.getKey())
                        .date(p.getKey().atOffset(ZoneOffset.UTC).toLocalDate())
                        .originalCategory(YoltCategory.GENERAL)
                        .build())
                .collect(Collectors.toList());
    }

    private static AccountsAndTransactionsRequestDTO buildIngestionData(UUID userSiteId, UUID siteId, Account account, List<ProviderTransactionDTO> transactions) {
        return AccountsAndTransactionsRequestDTO.builder()
                .activityId(UUID.randomUUID())
                .ingestionAccounts(List.of(AccountFromProviders.accountsFromProvidersBuilder()
                        .accountId(account.getId().toString())
                        .name(account.getName())
                        .yoltUserId(account.getUserId())
                        .yoltUserSiteId(account.getUserSiteId())
                        .yoltSiteId(account.getSiteId())
                        .yoltAccountType(account.getType())
                        .currency(account.getCurrency())
                        .currentBalance(account.getBalance())
                        .lastRefreshed(ZonedDateTime.now())
                        .transactions(transactions)
                        .provider("ABN Amro")
                        .build()))
                .userSiteId(userSiteId)
                .siteId(siteId)
                .build();
    }

    private static ProviderTransactionDTO.ProviderTransactionDTOBuilder transactionToDTOBuilder(Transaction transaction) {
        return ProviderTransactionDTO.builder()
                .dateTime(transaction.getTimestamp().atZone(ZoneOffset.UTC))
                .amount(transaction.getAmount().abs())
                .type(ProviderTransactionType.DEBIT)
                .status(transaction.getStatus())
                .externalId(transaction.getExternalId())
                .description(transaction.getDescription())
                .category(transaction.getOriginalCategory());
    }

    private static ProviderTransactionDTO pendingToBooked(Transaction transaction, Instant newTimestamp) {
        return transactionToDTOBuilder(transaction)
                .status(TransactionStatus.BOOKED)
                .dateTime(newTimestamp.atZone(ZoneOffset.UTC))
                .build();
    }

    private static ProviderTransactionDTO withoutChange(Transaction transaction) {
        return transactionToDTOBuilder(transaction).build();
    }

    private static List<ProviderTransactionDTO> withoutChange(List<Transaction> transactions) {
        return transactions.stream()
                .map(TransactionRefreshWindowIntegrationTest::withoutChange)
                .collect(Collectors.toList());
    }
}
