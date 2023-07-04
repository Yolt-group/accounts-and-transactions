package com.yolt.accountsandtransactions.offloading;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.yolt.accountsandtransactions.BaseIntegrationTest;
import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.AccountRepository;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.UserContext;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionRepository;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.providerdomain.AccountType;
import org.awaitility.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder.okForJson;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static java.math.BigDecimal.TEN;
import static java.time.LocalDate.EPOCH;
import static nl.ing.lovebird.extendeddata.common.CurrencyCode.EUR;
import static nl.ing.lovebird.extendeddata.transaction.TransactionStatus.PENDING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class BatchPushOffloadDataIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private OffloadedAccountsConsumer offloadedAccountsConsumer;

    @Autowired
    private OffloadedTransactionsConsumer offloadedTransactionsConsumer;

    @Autowired
    private BatchPushOffloadData batchPushOffloadData;

    @Autowired
    protected Session session;

    @BeforeEach
    public void before() {
        session.execute(QueryBuilder.truncate("transactions"));
        session.execute(QueryBuilder.truncate("accounts"));
    }

    @Test
    public void when_thereAreTransactions_then_theyShouldBePushedToDS() throws ExecutionException, InterruptedException, TimeoutException {
        UUID userId = UUID.randomUUID();
        UUID acctId = UUID.randomUUID();

        // Given 1 transactions
        transactionRepository.upsert(List.of(
                Transaction.builder()
                        .userId(userId)
                        .accountId(acctId)
                        .description("description")
                        .date(EPOCH)
                        .id("3")
                        .status(PENDING)
                        .currency(EUR)
                        .amount(TEN)
                        .build()));
        UserContext userContext = new UserContext(UUID.randomUUID(), userId);
        stubFor(get(urlPathMatching("/users/users/" + userId))
                .willReturn(okForJson(userContext)));

        // when triggering the batch
        CompletableFuture<Void> voidCompletableFuture = batchPushOffloadData.offloadTransactions(false, 5_000);
        voidCompletableFuture.toCompletableFuture().get(10, TimeUnit.SECONDS);

        // 1 'offloadable' transactions should be published
        await().atMost(Duration.TEN_SECONDS).untilAsserted(() -> {
            Set<OffloadedTransactionsConsumer.Message> messagesForUser = offloadedTransactionsConsumer.getConsumed().stream()
                    .filter(message -> message.getEnvelope().getPayload()
                            .filter(it -> it instanceof OffloadableTransaction)
                            .map(it -> ((OffloadableTransaction) it).getUserId())
                            .map(userId::equals)
                            .orElse(false))
                    .collect(Collectors.toSet());
            assertThat(messagesForUser.size()).isEqualTo(1);
        });
    }

    @Test
    public void when_thereAreAccounts_then_theyShouldBePushedToDS() throws ExecutionException, InterruptedException, TimeoutException {
        // given 1 accounts
        UUID userId = UUID.randomUUID();
        accountRepository.upsert(Account.builder()
                .name("account")
                .userId(userId)
                .siteId(UUID.randomUUID())
                .userSiteId(UUID.randomUUID())
                .id(UUID.randomUUID())
                .externalId(UUID.randomUUID().toString())
                .type(AccountType.CURRENT_ACCOUNT)
                .currency(CurrencyCode.EUR)
                .balance(BigDecimal.ONE)
                .status(Account.Status.ENABLED)
                .build());

        UserContext userContext = new UserContext(UUID.randomUUID(), userId);
        stubFor(get(urlPathMatching("/users/users/" + userId))
                .willReturn(okForJson(userContext)));

        // when triggering the batch
        CompletableFuture<Void> voidCompletableFuture = batchPushOffloadData.offloadAccounts(false, 5_000);
        voidCompletableFuture.toCompletableFuture().get(10, TimeUnit.SECONDS);

        // 1 'offloadable' accounts should be published
        await().atMost(Duration.TEN_SECONDS).untilAsserted(() -> {
            Set<OffloadedAccountsConsumer.Message> messagesForUser = offloadedAccountsConsumer.getConsumed().stream()
                    .filter(message -> message.getEnvelope().getPayload()
                            .filter(it -> it instanceof OffloadableAccount)
                            .map(it -> ((OffloadableAccount) it).getUserId())
                            .map(userId::equals)
                            .orElse(false))
                    .collect(Collectors.toSet());
            assertThat(messagesForUser.size()).isEqualTo(1);
        });
    }
}
